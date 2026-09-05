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
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@code /_cluster/settings} — §9.3's {@code /cluster/config} register, at the surface a caller reaches.
 *
 * <p>The one endpoint under {@code /_cluster/*} not refused, because it is the one piece of that
 * namespace that genuinely is one register rather than genuinely cluster-wide state. See
 * {@link org.opensearch.serverless.metadata.ClusterConfig} for what is and is not validated.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessClusterSettingsTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-cluster-config")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .build();
    }

    private MetadataPlane planeOver(java.nio.file.Path dir, AtomicLong clock) throws Exception {
        return new MetadataPlane(new FsBlobStore(1024, dir, false), BlobPath.cleanPath(), clock::get, TTL);
    }

    private record Response(int status, String body) {
    }

    private static Response send(TransportAddress address, String method, String path, String body) throws Exception {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(20))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }

    /** Nothing set, a plain read, and both stay 200 with an empty object rather than an error. */
    public void testAnEmptyRegisterReadsAsEmptySettings() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("cluster-config-empty"))) {
            node.start();
            node.setMetadataPlane(plane);
            final TransportAddress http = node.boundHttpAddress().publishAddress();

            final Response got = send(http, "GET", "/_cluster/settings", null);
            assertEquals(got.body(), 200, got.status());
            assertTrue(got.body(), got.body().contains("\"persistent\":{}"));
            assertTrue(got.body(), got.body().contains("\"transient\":{}"));
        }
    }

    /** A setting written is read back, including across a fresh read of the register from scratch. */
    public void testASettingWrittenIsReadBack() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("cluster-config-write"))) {
            node.start();
            node.setMetadataPlane(plane);
            final TransportAddress http = node.boundHttpAddress().publishAddress();

            final Response put = send(http, "PUT", "/_cluster/settings", "{\"persistent\":{\"my.setting\":\"hello\"}}");
            assertEquals(put.body(), 200, put.status());
            // Nested in the response, not flat -- Settings#toXContent renders a dotted key as a nested
            // object by default, the same as classic OpenSearch's own GET _cluster/settings.
            assertTrue(put.body(), put.body().contains("\"my\":{\"setting\":\"hello\"}"));

            final Response got = send(http, "GET", "/_cluster/settings", null);
            assertTrue(got.body(), got.body().contains("\"my\":{\"setting\":\"hello\"}"));

            // And the plane itself sees it, not only the REST response -- the register is what persists.
            assertEquals("hello", plane.clusterConfig().read().settings().get("my.setting"));
        }
    }

    /** A second write merges with the first rather than replacing the whole register. */
    public void testASecondWriteMergesRatherThanReplaces() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("cluster-config-merge"))) {
            node.start();
            node.setMetadataPlane(plane);
            final TransportAddress http = node.boundHttpAddress().publishAddress();

            send(http, "PUT", "/_cluster/settings", "{\"persistent\":{\"a\":\"1\"}}");
            send(http, "PUT", "/_cluster/settings", "{\"persistent\":{\"b\":\"2\"}}");

            final Response got = send(http, "GET", "/_cluster/settings", null);
            assertTrue("the first setting must survive the second write: " + got.body(), got.body().contains("\"a\":\"1\""));
            assertTrue("and the second must be there too: " + got.body(), got.body().contains("\"b\":\"2\""));
        }
    }

    /** A nested object is stored and read back with its structure, not flattened into one dotted string. */
    public void testANestedSettingRoundTrips() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("cluster-config-nested"))) {
            node.start();
            node.setMetadataPlane(plane);
            final TransportAddress http = node.boundHttpAddress().publishAddress();

            send(http, "PUT", "/_cluster/settings", "{\"persistent\":{\"group\":{\"one\":\"a\",\"two\":\"b\"}}}");
            assertEquals("a", plane.clusterConfig().read().settings().get("group.one"));
            assertEquals("b", plane.clusterConfig().read().settings().get("group.two"));

            final Response got = send(http, "GET", "/_cluster/settings", null);
            assertTrue(got.body(), got.body().contains("\"group\""));
            assertTrue(got.body(), got.body().contains("\"one\":\"a\""));
        }
    }

    /** Setting a key to null removes it, and it does not come back on the next write of an unrelated key. */
    public void testANullValueRemovesTheSetting() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("cluster-config-remove"))) {
            node.start();
            node.setMetadataPlane(plane);
            final TransportAddress http = node.boundHttpAddress().publishAddress();

            send(http, "PUT", "/_cluster/settings", "{\"persistent\":{\"gone\":\"here-for-now\",\"stays\":\"1\"}}");
            assertEquals("here-for-now", plane.clusterConfig().read().settings().get("gone"));

            send(http, "PUT", "/_cluster/settings", "{\"persistent\":{\"gone\":null}}");
            assertNull("a null value must remove the setting", plane.clusterConfig().read().settings().get("gone"));
            assertEquals("and an unrelated setting must survive the removal", "1", plane.clusterConfig().read().settings().get("stays"));

            final Response got = send(http, "GET", "/_cluster/settings", null);
            assertFalse(
                "the removed key must not be echoed back as a null, ever -- not even once: " + got.body(),
                got.body().contains("\"gone\"")
            );

            // And a further, unrelated write must not resurrect it -- proving the register itself was
            // cleaned rather than the removal only hiding the key from this one read.
            send(http, "PUT", "/_cluster/settings", "{\"persistent\":{\"another\":\"x\"}}");
            final Response after = send(http, "GET", "/_cluster/settings", null);
            assertFalse("a removed key must not resurface on a later write: " + after.body(), after.body().contains("\"gone\""));
        }
    }

    /** A non-empty transient block is refused, not silently dropped or silently treated as persistent. */
    public void testANonEmptyTransientBlockIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("cluster-config-transient"))) {
            node.start();
            node.setMetadataPlane(plane);
            final TransportAddress http = node.boundHttpAddress().publishAddress();

            final Response refused = send(http, "PUT", "/_cluster/settings", "{\"persistent\":{},\"transient\":{\"x\":\"1\"}}");
            assertEquals(refused.body(), 501, refused.status());
            assertTrue(refused.body(), refused.body().contains("transient"));

            // An empty transient block is harmless and must not be refused -- many client libraries send
            // it by default even when the caller never asked for one.
            final Response allowed = send(http, "PUT", "/_cluster/settings", "{\"persistent\":{\"ok\":\"1\"},\"transient\":{}}");
            assertEquals(allowed.body(), 200, allowed.status());
        }
    }

    /** A list-valued setting is refused rather than silently mishandled. */
    public void testAListValuedSettingIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("cluster-config-list"))) {
            node.start();
            node.setMetadataPlane(plane);
            final TransportAddress http = node.boundHttpAddress().publishAddress();

            final Response refused = send(http, "PUT", "/_cluster/settings", "{\"persistent\":{\"hosts\":[\"a\",\"b\"]}}");
            assertEquals(refused.body(), 501, refused.status());
            assertTrue(refused.body(), refused.body().contains("hosts"));
        }
    }

    /** A body with no {@code persistent} object is a bad request. */
    public void testAMissingPersistentBlockIsRejected() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("cluster-config-missing"))) {
            node.start();
            node.setMetadataPlane(plane);
            final TransportAddress http = node.boundHttpAddress().publishAddress();

            assertEquals(400, send(http, "PUT", "/_cluster/settings", "{}").status());
        }
    }

    /**
     * Concurrent writers on the register do not lose an update to a lost race.
     *
     * <p>The property the retry loop in {@code ClusterConfig#update} exists for: two writers racing the
     * same compare-and-swap must both end up reflected, not one silently overwriting the other's change.
     */
    public void testConcurrentUpdatesToDifferentKeysBothSurvive() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);
        plane.clusterConfig().update(java.util.Map.of("seed", "0"));

        final int threads = 8;
        final java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
        final Thread[] workers = new Thread[threads];
        final java.util.List<Exception> failures = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        for (int i = 0; i < threads; i++) {
            final int id = i;
            workers[i] = new Thread(() -> {
                try {
                    start.await();
                    plane.clusterConfig().update(java.util.Map.of("key-" + id, String.valueOf(id)));
                } catch (Exception e) {
                    failures.add(e);
                }
            });
            workers[i].start();
        }
        start.countDown();
        for (Thread t : workers) {
            t.join(30_000);
        }

        assertEquals("no writer should have failed after retrying: " + failures, java.util.List.of(), failures);
        final Settings finalSettings = plane.clusterConfig().read().settings();
        for (int i = 0; i < threads; i++) {
            assertEquals("writer " + i + "'s key must have survived the race", String.valueOf(i), finalSettings.get("key-" + i));
        }
        assertEquals("the seed set before the race must also survive", "0", finalSettings.get("seed"));
    }
}
