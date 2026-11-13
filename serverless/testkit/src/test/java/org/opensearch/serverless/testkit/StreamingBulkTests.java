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
import org.opensearch.serverless.rest.BulkHandler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Batches streamed through the Reactor Netty transport: applied a slice at a time as the body arrives, answered with an
 * ordinary bulk response, and waiting for memory rather than refused.
 */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*")
public class StreamingBulkTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, Settings extra) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-streaming-bulk")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(BulkHandler.STREAMING_SETTING, true)
            .put(BulkHandler.SLICE_BYTES_SETTING, "64kb")
            .put(extra)
            .build();
    }

    private record Response(int status, String body) {
    }

    private static Response post(ServerlessNode node, String path, byte[] body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (
            HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(10)).build()
        ) {
            final HttpResponse<String> response = client.send(
                HttpRequest.newBuilder(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                    .timeout(Duration.ofSeconds(120))
                    .header("Content-Type", "application/x-ndjson")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            );
            return new Response(response.statusCode(), response.body());
        }
    }

    private static byte[] batch(String prefix, int docs, int docBytes) {
        final StringBuilder out = new StringBuilder();
        final String filler = "word ".repeat(Math.max(1, docBytes / 5));
        for (int i = 0; i < docs; i++) {
            out.append("{\"index\":{\"_index\":\"").append(i % 2 == 0 ? "even" : "odd").append("\",\"_id\":\"").append(prefix).append(i);
            out.append("\"}}\n{\"msg\":\"").append(filler).append("\",\"n\":").append(i).append("}\n");
        }
        return out.toString().getBytes(StandardCharsets.UTF_8);
    }

    private ServerlessNode started(Settings settings, MetadataPlane plane, AtomicLong clock) throws Exception {
        final ServerlessNode node = new ServerlessNode(settings);
        node.startup().start();
        node.setMetadataPlane(plane);
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        loop.want("even", 0);
        loop.want("odd", 0);
        loop.tick(clock.get());
        loop.tick(clock.get());
        return node;
    }

    private MetadataPlane plane(AtomicLong clock) throws Exception {
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("even", "uuid-even-000000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("odd", "uuid-odd-0000000000", 1, MAPPING, null));
        return plane;
    }

    public void testABatchLargerThanManySlicesIsAppliedInOrderAndAnsweredWhole() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        try (ServerlessNode node = started(nodeSettings("streaming-order", Settings.EMPTY), plane, clock)) {
            // About 1.5 MB across 64 KB slices, with documents straddling every slice boundary.
            final int docs = 1_500;
            final Response response = post(node, "/_bulk?refresh=true", batch("d", docs, 1_000));
            assertEquals(response.body(), 200, response.status());
            assertFalse("no item failed: " + response.body(), response.body().contains("\"errors\":true"));
            final Matcher ids = Pattern.compile("\"_id\":\"(d\\d+)\"").matcher(response.body());
            final List<String> seen = new ArrayList<>();
            while (ids.find()) {
                seen.add(ids.group(1));
            }
            assertEquals("every operation answered, in request order", docs, seen.size());
            for (int i = 0; i < docs; i++) {
                assertEquals("d" + i, seen.get(i));
            }
            final Response count = post(node, "/even,odd/_count", "{}".getBytes(StandardCharsets.UTF_8));
            assertTrue(count.body(), count.body().contains("\"count\":" + docs));
        }
    }

    public void testBatchesWaitForMemoryRatherThanBeingRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        // The budget fits one slice at a time: four batches at once take turns, and none is refused.
        final Settings tight = Settings.builder().put(BulkHandler.BUDGET_SETTING, "256kb").build();
        try (ServerlessNode node = started(nodeSettings("streaming-wait", tight), plane, clock)) {
            final List<Response> responses = new CopyOnWriteArrayList<>();
            final List<Thread> clients = new ArrayList<>();
            for (int c = 0; c < 4; c++) {
                final String prefix = "c" + c + "-";
                final Thread client = new Thread(() -> {
                    try {
                        responses.add(post(node, "/_bulk", batch(prefix, 400, 1_000)));
                    } catch (Exception e) {
                        responses.add(new Response(-1, e.toString()));
                    }
                });
                client.start();
                clients.add(client);
            }
            for (Thread client : clients) {
                client.join();
            }
            for (Response response : responses) {
                assertEquals(response.body(), 200, response.status());
                assertFalse(response.body(), response.body().contains("\"errors\":true"));
            }
            assertEquals(4, responses.size());
        }
    }

    public void testAShardTheStoreIsTooSlowForIsWaitedForRatherThanRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        // A limiter that refuses all but one write at a time once any append has taken a millisecond.
        final Settings strict = Settings.builder()
            .put("serverless.write.backpressure.target_millis", 1)
            .put("serverless.write.backpressure.budget_millis", 1)
            .put("serverless.write.backpressure.min_in_flight", 1)
            .build();
        try (ServerlessNode node = started(nodeSettings("streaming-throttled", strict), plane, clock)) {
            final List<Response> responses = new CopyOnWriteArrayList<>();
            final List<Thread> clients = new ArrayList<>();
            for (int c = 0; c < 6; c++) {
                final String prefix = "t" + c + "-";
                final Thread client = new Thread(() -> {
                    try {
                        responses.add(post(node, "/_bulk?refresh=true", batch(prefix, 600, 1_000)));
                    } catch (Exception e) {
                        responses.add(new Response(-1, e.toString()));
                    }
                });
                client.start();
                clients.add(client);
            }
            for (Thread client : clients) {
                client.join();
            }
            assertEquals(6, responses.size());
            for (Response response : responses) {
                assertEquals(response.body(), 200, response.status());
                assertFalse("no operation was refused: " + response.body(), response.body().contains("\"errors\":true"));
            }
            assertTrue("the limiter did refuse some groups along the way", node.writeBackpressure().refused() > 0);
            final Response count = post(node, "/even,odd/_count", "{}".getBytes(StandardCharsets.UTF_8));
            assertTrue("every operation landed once: " + count.body(), count.body().contains("\"count\":3600"));
        }
    }

    public void testAMalformedLinePartWayIsRefusedAfterWhatCameBefore() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        try (ServerlessNode node = started(nodeSettings("streaming-bad", Settings.EMPTY), plane, clock)) {
            final byte[] good = batch("g", 300, 1_000);
            final byte[] bad = "{not json\n".getBytes(StandardCharsets.UTF_8);
            final byte[] body = new byte[good.length + bad.length];
            System.arraycopy(good, 0, body, 0, good.length);
            System.arraycopy(bad, 0, body, good.length, bad.length);
            final Response response = post(node, "/_bulk", body);
            assertEquals(response.body(), 400, response.status());
        }
    }

    public void testAnEmptyBodyIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        try (ServerlessNode node = started(nodeSettings("streaming-empty", Settings.EMPTY), plane, clock)) {
            final Response response = post(node, "/_bulk", new byte[0]);
            assertEquals(response.body(), 400, response.status());
        }
    }
}
