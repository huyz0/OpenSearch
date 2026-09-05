/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * M13: a node as a process.
 *
 * <p>The scheduler made a node <em>capable</em> of running unattended. Until the bootstrap existed
 * nothing ever built one outside a test method, so the claim was true of an arrangement that only
 * occurred inside JUnit. These tests drive the same entry point a deployment would.
 *
 * <p>Nothing here calls {@code tick}, {@code heartbeat}, {@code activateWriter}, {@code index} or
 * {@code publishShard}. Every effect below is the node acting on its own, reached over HTTP.
 *
 * <p><b>D5:</b> filesystem-backed store only.
 */
public class ServerlessBootstrapTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings bootSettings(String name, java.nio.file.Path store) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-m13-boot")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(ServerlessBootstrap.STORE_PATH, store.toString())
            // Short, so the renewal timer demonstrably fires inside the test rather than being trusted.
            .put(ServerlessBootstrap.LEASE_TTL, 600L)
            .build();
    }

    /** The store path is what the node cannot be built without, and saying so beats a later NPE. */
    public void testABootstrapWithNowhereToStoreAnythingRefusesToStart() {
        final Settings settings = Settings.builder().put("node.name", "m13-boot-nostore").put("path.home", createTempDir()).build();
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ServerlessBootstrap.start(settings));
        assertTrue(
            "the message should name the missing setting: " + e.getMessage(),
            e.getMessage().contains(ServerlessBootstrap.STORE_PATH)
        );
    }

    /**
     * The whole of M13 in one test: start a process, create an index over HTTP, write to it, search it
     * back, and observe that the node took the shard, renewed its lease and published a commit without
     * anybody driving it.
     */
    public void testAProcessThatIsOnlyEverStartedServesWritesAndSearches() throws Exception {
        final java.nio.file.Path store = createTempDir();

        try (ServerlessBootstrap boot = ServerlessBootstrap.start(bootSettings("m13-boot", store))) {
            final var http = boot.node().boundHttpAddress().publishAddress();

            assertEquals("index creation over HTTP", 200, send(http, "PUT", "/alpha?shards=1", MAPPING).status());

            // The node was told to want nothing. On-demand activation is what makes this write land: the
            // first attempt is refused because no node owns the shard, and that refusal is the signal.
            assertEquals(421, send(http, "PUT", "/alpha/_doc/1", "{\"msg\":\"unattended\",\"n\":1}").status());
            assertBusy(
                () -> assertFalse("the node should have taken the shard on its own", boot.node().reconciler().openShards().isEmpty()),
                20,
                java.util.concurrent.TimeUnit.SECONDS
            );

            final Response written = send(http, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"unattended\",\"n\":1}");
            assertEquals("the write must succeed once the shard is held: " + written.body(), 201, written.status());

            final Response found = send(http, "GET", "/alpha/_search?q=msg:unattended", null);
            assertEquals(200, found.status());
            assertTrue("the document must be searchable: " + found.body(), found.body().contains("unattended"));

            // Renewal and publication, neither of which anything in this test asked for.
            assertBusy(
                () -> assertTrue(
                    "the node should renew its own lease",
                    boot.plane().membership().read(boot.node().localNode().getId()).isPresent()
                ),
                20,
                java.util.concurrent.TimeUnit.SECONDS
            );
            assertBusy(
                () -> assertTrue(
                    "the node should publish its own commit",
                    boot.plane().segmentPublisher("alpha", 0).readManifest().isPresent()
                ),
                20,
                java.util.concurrent.TimeUnit.SECONDS
            );

            final var counts = boot.scheduler().counts();
            logger.info("m13 unattended process: {}", counts);
            assertTrue("the renewal timer must have run", counts.renewals() >= 1);
            assertTrue("an edge publish must have run", counts.publishes() >= 1);
        }
    }

    /**
     * On-demand activation is the daemon's default and can be turned off. With it off, the same write is
     * refused and stays refused — proving the daemon default is a choice the setting controls, not
     * something the bootstrap does unconditionally.
     */
    public void testOnDemandActivationCanBeTurnedOffAtTheProcessLevel() throws Exception {
        final java.nio.file.Path store = createTempDir();
        final Settings settings = Settings.builder()
            .put(bootSettings("m13-boot-off", store))
            .put(ServerlessBootstrap.ON_DEMAND, false)
            .build();

        try (ServerlessBootstrap boot = ServerlessBootstrap.start(settings)) {
            final var http = boot.node().boundHttpAddress().publishAddress();
            assertEquals(200, send(http, "PUT", "/alpha?shards=1", MAPPING).status());

            for (int i = 0; i < 3; i++) {
                assertEquals(421, send(http, "PUT", "/alpha/_doc/" + i, "{\"msg\":\"x\",\"n\":1}").status());
            }
            // Give the activation passes time to have run and done nothing.
            assertBusy(
                () -> assertTrue("activation passes must still run", boot.scheduler().counts().activations() >= 1),
                20,
                java.util.concurrent.TimeUnit.SECONDS
            );
            assertTrue("with on-demand off, no shard may be taken", boot.node().reconciler().openShards().isEmpty());
        }
    }

    /**
     * A clean shutdown must drop the node's lease rather than leave peers believing it is alive for a
     * full TTL — which is the difference between a restart being a handover and being a failover.
     */
    public void testACleanShutdownDoesNotLeaveAGhostBehind() throws Exception {
        final java.nio.file.Path store = createTempDir();
        final String nodeId;
        final org.opensearch.serverless.metadata.MetadataPlane plane;

        final ServerlessBootstrap boot = ServerlessBootstrap.start(bootSettings("m13-boot-ghost", store));
        try {
            plane = boot.plane();
            nodeId = boot.node().localNode().getId();
            assertBusy(
                () -> assertTrue("the node should have a lease while running", plane.membership().read(nodeId).isPresent()),
                20,
                java.util.concurrent.TimeUnit.SECONDS
            );
        } finally {
            boot.close();
        }

        // Absence, not expiry. Asserting "no longer live" would pass on the short TTL alone whether or
        // not shutdown did anything -- the first version of this test did exactly that, and was
        // measuring the clock rather than the code.
        assertTrue("a clean shutdown must drop the lease, not leave peers waiting out its TTL", plane.membership().read(nodeId).isEmpty());
    }

    private record Response(int status, String body) {
    }

    private static Response send(org.opensearch.core.common.transport.TransportAddress address, String method, String path, String body)
        throws Exception {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest.BodyPublisher payload = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .method(method, payload)
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
