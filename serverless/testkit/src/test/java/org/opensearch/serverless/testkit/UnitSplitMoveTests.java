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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.reconcile.ReconcileScheduler;
import org.opensearch.serverless.routing.ShardMode;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.split.UnitSplitMover;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Splitting a unit while clients keep writing to it: every write acknowledged before, during and after the split is
 * found once, in the half its id routes to, and the old unit is retired.
 *
 * <p>Runs against {@code FsBlobContainer} only.
 */
public class UnitSplitMoveTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int BEFORE = 40;

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-split-move")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.wal.node_log.enabled", true)
            .build();
    }

    private record Response(int status, String body) {
    }

    public void testAUnitSplitsUnderLiveWritesAndLosesNone() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("split-move"))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true);
            final ReconcileScheduler scheduler = new ReconcileScheduler(
                loop,
                node.threadPool(),
                clock::get,
                TimeValue.timeValueHours(1),
                TimeValue.timeValueHours(1),
                null
            );
            node.setSignals(scheduler);
            scheduler.start();
            try (scheduler) {
                final String create = "{\"settings\":{\"number_of_shards\":4,\""
                    + ShardMode.SETTING
                    + "\":\"adaptive\"},"
                    + "\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}}";
                assertEquals(200, send(node, "PUT", "/grow", create).status());
                for (int doc = 0; doc < BEFORE; doc++) {
                    assertEquals(201, send(node, "PUT", "/grow/_doc/before-" + doc, "{\"msg\":\"before\",\"n\":" + doc + "}").status());
                }
                assertEquals(200, send(node, "DELETE", "/grow/_doc/before-3", null).status());
                loop.publishAll();
                // Written after the publish, so only the log has them when the split starts.
                for (int doc = 0; doc < 5; doc++) {
                    assertEquals(201, send(node, "PUT", "/grow/_doc/unpublished-" + doc, "{\"msg\":\"tail\",\"n\":" + doc + "}").status());
                }

                // A client that keeps writing, and a publisher that keeps publishing, all through the split: what the
                // old unit publishes during the build must not take records out of the tail.
                final AtomicBoolean stop = new AtomicBoolean();
                final List<String> acknowledged = new CopyOnWriteArrayList<>();
                final List<String> refused = new CopyOnWriteArrayList<>();
                final Thread writer = new Thread(() -> {
                    for (int doc = 0; stop.get() == false; doc++) {
                        final String id = "during-" + doc;
                        try {
                            final Response put = send(node, "PUT", "/grow/_doc/" + id, "{\"msg\":\"during\",\"n\":" + doc + "}");
                            (put.status() == 201 ? acknowledged : refused).add(id + " " + put.status() + " " + put.body());
                            if (doc % 5 == 0) {
                                loop.publishAll();
                            }
                        } catch (Exception e) {
                            refused.add(id + " " + e);
                        }
                    }
                }, "split-move-writer");
                writer.start();
                final Optional<UnitSplitMover.Moved> moved;
                try {
                    Thread.sleep(200);
                    moved = new UnitSplitMover(node, plane).setRoutingCacheMillis(300).split("grow", 0, createTempDir());
                    Thread.sleep(200);
                } finally {
                    stop.set(true);
                    writer.join(60_000L);
                }
                assertTrue("the split went through", moved.isPresent());
                assertTrue("the client wrote during the split: " + acknowledged.size(), acknowledged.size() > 0);

                final IndexDescriptor after = plane.describe("grow").orElseThrow();
                assertArrayEquals(new int[] { 1, 2 }, after.unitIds());
                assertNull(after.layout().pending());
                assertFalse("the old unit is retired", after.writableSlot(0));
                assertEquals("the declared count is what it was", 4, after.declaredShards());
                assertTrue(
                    "the old unit is closed here",
                    node.reconciler().openShards().stream().noneMatch(s -> s.getIndexName().equals("grow") && s.id() == 0)
                );
                assertTrue("the old unit's head is given up", plane.heads().read("grow", 0).map(h -> h.ownerNodeId() == null).orElse(true));

                for (int doc = 0; doc < BEFORE; doc++) {
                    final Response got = send(node, "GET", "/grow/_doc/before-" + doc, null);
                    assertEquals(got.body(), doc == 3 ? 404 : 200, got.status());
                }
                for (int doc = 0; doc < 5; doc++) {
                    assertEquals(200, send(node, "GET", "/grow/_doc/unpublished-" + doc, null).status());
                }
                for (String ack : acknowledged) {
                    final String id = ack.substring(0, ack.indexOf(' '));
                    final Response got = send(node, "GET", "/grow/_doc/" + id, null);
                    assertEquals(id + ": " + got.body(), 200, got.status());
                }
                assertEquals(201, send(node, "PUT", "/grow/_doc/after?refresh=true", "{\"msg\":\"after\",\"n\":1}").status());
                final Response count = send(node, "GET", "/grow/_count", null);
                final long expected = (BEFORE - 1) + 5 + acknowledged.size() + 1;
                assertTrue(count.body() + " expected " + expected, count.body().contains("\"count\":" + expected));
                assertTrue("two units searched: " + count.body(), count.body().contains("\"total\":2"));
                for (ShardId open : node.reconciler().openShards()) {
                    if (open.getIndexName().equals("grow")) {
                        final var stats = node.reconciler().shard(open).seqNoStats();
                        assertEquals("no gap in " + open, stats.getMaxSeqNo(), stats.getLocalCheckpoint());
                    }
                }
            }
        }
    }

    public void testSplittingNeedsTheUnitHeldAsAWriterHere() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("split-refused"))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            final IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> new UnitSplitMover(node, plane).split("absent", 0, createTempDir())
            );
            assertTrue(e.getMessage(), e.getMessage().contains("not held as a writer"));
        }
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
