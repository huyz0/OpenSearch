/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@code /_serverless/stats} — the numbers behind a refusal.
 *
 * <p>Real circuit breakers and a bound on in-flight writes turn a node that would have died into one that
 * refuses a request. That is the right trade and it leaves an operator with a 429 and no way to tell
 * whether the node has been near its limit for an hour or was hit by one enormous request.
 *
 * <p>The tests assert that the numbers <em>move</em> rather than that they exist. A stats endpoint that
 * returns plausible constants is worse than none: it answers the question wrongly instead of not at all.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessStatsTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name, String indexingLimit) {
        final Settings.Builder builder = Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-stats")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest");
        if (indexingLimit != null) {
            builder.put("indexing_pressure.memory.limit", indexingLimit);
        }
        return builder.build();
    }

    /** What the node is holding, reported as it changes. */
    public void testStatsReportWhatTheNodeIsActuallyHolding() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("counted", "uuid-counted-000000", 3, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("stats-shards", null))) {
            node.start();
            node.setMetadataPlane(plane);

            final Response before = send(node, "GET", "/_serverless/stats", null);
            assertEquals(before.body(), 200, before.status());
            assertTrue("it must name the node it answers for: " + before.body(), before.body().contains(node.localNode().getId()));
            assertTrue("holding nothing yet: " + before.body(), before.body().contains("\"open\":0"));
            assertTrue("and say what it is willing to do: " + before.body(), before.body().contains("\"ingest\""));

            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            for (int shard = 0; shard < 3; shard++) {
                loop.want("counted", shard);
            }
            loop.tick(clock.get());

            final Response after = send(node, "GET", "/_serverless/stats", null);
            // The number moved, which is the whole point: a constant that happened to look right would
            // pass an assertion that only checked the field was there.
            assertTrue("three shards held: " + after.body(), after.body().contains("\"open\":3"));
            assertTrue("all of them writers: " + after.body(), after.body().contains("\"writers\":3"));
            assertTrue("and none readers: " + after.body(), after.body().contains("\"readers\":0"));
        }
    }

    /** The breakers report a limit and a use, and the limit is the one that was configured. */
    public void testStatsReportTheBreakersThatDecideARefusal() throws Exception {
        final Settings settings = Settings.builder()
            .put(nodeSettings("stats-breakers", null))
            .put("indices.breaker.total.use_real_memory", false)
            .put("indices.breaker.request.limit", "4096b")
            .build();

        try (ServerlessNode node = new ServerlessNode(settings)) {
            node.start();
            final Response got = send(node, "GET", "/_serverless/stats", null);
            assertEquals(got.body(), 200, got.status());
            assertTrue("the request breaker must be reported: " + got.body(), got.body().contains("\"request\""));
            assertTrue(
                "with the limit an operator configured, so the number they see is the number they set: " + got.body(),
                got.body().contains("\"limit_bytes\":4096")
            );
            assertTrue("and the parent, which is what actually trips first: " + got.body(), got.body().contains("\"parent\""));
        }
    }

    /**
     * A rejected write shows up as a rejection, and the total climbs with what was accepted.
     *
     * <p>This is the trend the endpoint exists for: without it a 429 is a single event with no history, and
     * an operator cannot tell a node that has been at its limit all morning from one unlucky request.
     */
    public void testStatsCountRejectionsAndAcceptedBytes() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("pressed", "uuid-pressed-000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("stats-pressure", "40b"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("pressed", 0);
            loop.tick(clock.get());

            assertTrue("nothing rejected yet", send(node, "GET", "/_serverless/stats", null).body().contains("\"rejections\":0"));

            assertEquals(201, send(node, "PUT", "/pressed/_doc/1?refresh=true", "{\"msg\":\"ok\"}").status());
            final Response accepted = send(node, "GET", "/_serverless/stats", null);
            assertFalse("an accepted write must show in the total: " + accepted.body(), accepted.body().contains("\"total_bytes\":0"));
            assertTrue("and nothing is being held now it is done: " + accepted.body(), accepted.body().contains("\"current_bytes\":0"));

            assertEquals(
                429,
                send(node, "PUT", "/pressed/_doc/2", "{\"msg\":\"far more than forty bytes of document body here\"}").status()
            );
            final Response rejected = send(node, "GET", "/_serverless/stats", null);
            assertFalse("the rejection must be counted: " + rejected.body(), rejected.body().contains("\"rejections\":0"));
        }
    }

    /**
     * {@code ?nodes=} asks the fleet, and says which members did not answer.
     *
     * <p><b>The accounting is the assertion.</b> A fan-out that returned the nodes it could reach and said
     * nothing about the rest would let an operator read "everything is fine" off a report that is missing
     * the node that is not fine — and the node worth asking about is exactly the one most likely not to
     * answer. So the second half of this stops a node and checks that it is <em>named</em>, with a reason,
     * and counted in {@code failed}: reaching one node out of two must not look like reaching one node.
     *
     * <p>Peers are asked over the transport rather than over HTTP, so this also exercises the authenticated
     * hop; a node answering for itself takes no hop at all, which is why the local id must be present in
     * both halves.
     */
    public void testTheFleetIsAskedAndTheOnesThatDoNotAnswerAreNamed() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);

        try (ServerlessNode first = new ServerlessNode(nodeSettings("fleet-stats-a", null))) {
            final String secondId;
            try (ServerlessNode second = new ServerlessNode(nodeSettings("fleet-stats-b", null))) {
                first.start();
                first.setMetadataPlane(plane);
                second.start();
                second.setMetadataPlane(plane);
                secondId = second.localNode().getId();
                // Each publishes a lease, which is what a reconcile pass does in a running deployment and
                // is the only way one node learns where another is.
                new BackgroundReconciler(first, plane).tick(clock.get());
                new BackgroundReconciler(second, plane).tick(clock.get());

                final Response all = send(first, "GET", "/_serverless/stats?nodes=_all", null);
                assertEquals(all.body(), 200, all.status());
                assertTrue("both nodes asked: " + all.body(), all.body().contains("\"total\":2"));
                assertTrue("and both answered: " + all.body(), all.body().contains("\"successful\":2"));
                assertTrue("nothing failed: " + all.body(), all.body().contains("\"failed\":0"));
                assertTrue("the local node, answered without a hop: " + all.body(), all.body().contains(first.localNode().getId()));
                assertTrue("and the peer, answered over the transport: " + all.body(), all.body().contains(secondId));
                // The peer's own document, not a summary of it: this field is produced by the code that
                // answers /_serverless/stats and by nothing else.
                assertTrue("each node's own body is carried through: " + all.body(), all.body().contains("\"breakers\""));

                // Named rather than _all, and by name rather than by id, which is how a person asks.
                final Response one = send(first, "GET", "/_serverless/stats?nodes=fleet-stats-b", null);
                assertTrue("one node: " + one.body(), one.body().contains("\"total\":1"));
                assertTrue("the one asked for: " + one.body(), one.body().contains(secondId));
                assertFalse("and not the other: " + one.body(), one.body().contains(first.localNode().getId()));

                // A name that matches nothing is said, not quietly dropped: an operator who mistyped a node
                // name and got an answer about the others would read it as the fleet.
                final Response missing = send(first, "GET", "/_serverless/stats?nodes=no-such-node", null);
                assertEquals(missing.body(), 404, missing.status());
                assertTrue("it must name what it could not find: " + missing.body(), missing.body().contains("no-such-node"));
            }

            // The peer is gone but its lease has not expired, which is precisely the window in which a
            // fan-out is asked about a node that cannot answer.
            final Response afterLoss = send(first, "GET", "/_serverless/stats?nodes=_all", null);
            assertEquals(afterLoss.body(), 200, afterLoss.status());
            assertTrue("still two members: " + afterLoss.body(), afterLoss.body().contains("\"total\":2"));
            assertTrue("one answered: " + afterLoss.body(), afterLoss.body().contains("\"successful\":1"));
            assertTrue("and one is counted as failed: " + afterLoss.body(), afterLoss.body().contains("\"failed\":1"));
            assertTrue("the one that did not answer is named: " + afterLoss.body(), afterLoss.body().contains(secondId));
            assertTrue("with a reason: " + afterLoss.body(), afterLoss.body().contains("\"reason\""));
            assertTrue("the node that did answer still did: " + afterLoss.body(), afterLoss.body().contains(first.localNode().getId()));
        }
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .method(
                    method,
                    body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)
                )
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
