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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.rest.DocumentRouting;
import org.opensearch.serverless.shell.BulkOperation;
import org.opensearch.serverless.shell.BulkOutcome;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.shell.WriteHold;
import org.opensearch.serverless.shell.WriteOutcome;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/** A hold stops a shard's writes until its holder lets them go on here or hands them to somewhere else. */
public class WriteHoldTests extends OpenSearchTestCase {

    private static final long TTL = 600_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";
    private static final String DOC = "{\"msg\":\"m\",\"n\":1}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-write-hold")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.window_millis", 20)
            .build();
    }

    /** Records what it is handed, and answers as if it had applied it. */
    private static final class Recorder implements WriteHold.Target {
        final List<String> ids = new CopyOnWriteArrayList<>();

        @Override
        public WriteOutcome index(
            ShardId shardId,
            String id,
            String routing,
            String source,
            long ifSeqNo,
            long ifPrimaryTerm,
            boolean requireAbsent,
            boolean mayGrowMapping
        ) {
            ids.add(id);
            return new WriteOutcome(1_000L, 1L, 1L, true, false);
        }

        @Override
        public WriteOutcome delete(ShardId shardId, String id, String routing, long ifSeqNo, long ifPrimaryTerm) {
            ids.add("delete:" + id);
            return new WriteOutcome(1_001L, 1L, 2L, false, true);
        }

        @Override
        public List<BulkOutcome> bulk(ShardId shardId, List<BulkOperation> batch) {
            ids.add("bulk:" + batch.size());
            return List.of();
        }

        @Override
        public boolean refresh(String source) {
            ids.add("refresh");
            return true;
        }
    }

    /** Applies what it is handed to another shard on the same node, as a split's halves do. */
    private static final class ToShard implements WriteHold.Target {
        final ServerlessNode node;
        final ShardId to;
        final List<String> refreshes = new CopyOnWriteArrayList<>();

        ToShard(ServerlessNode node, ShardId to) {
            this.node = node;
            this.to = to;
        }

        @Override
        public WriteOutcome index(
            ShardId shardId,
            String id,
            String routing,
            String source,
            long ifSeqNo,
            long ifPrimaryTerm,
            boolean requireAbsent,
            boolean mayGrowMapping
        ) throws IOException {
            return node.writes().index(to, id, routing, source, ifSeqNo, ifPrimaryTerm, requireAbsent, mayGrowMapping);
        }

        @Override
        public WriteOutcome delete(ShardId shardId, String id, String routing, long ifSeqNo, long ifPrimaryTerm) throws IOException {
            return node.writes().delete(to, id, routing, ifSeqNo, ifPrimaryTerm);
        }

        @Override
        public List<BulkOutcome> bulk(ShardId shardId, List<BulkOperation> batch) throws IOException {
            return node.writes().bulkOperations(to, batch);
        }

        @Override
        public boolean refresh(String source) {
            refreshes.add(source);
            return node.writes().refresh(to, source);
        }
    }

    private interface Body {
        void run(ServerlessNode node, ShardId shardId, ExecutorService writers) throws Exception;
    }

    private void withShard(String name, Body body) throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        final ExecutorService writers = Executors.newFixedThreadPool(2);
        try (ServerlessNode node = new ServerlessNode(nodeSettings(name))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            node.leases().renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            body.run(node, node.reconciler().openShards().iterator().next(), writers);
        } finally {
            writers.shutdownNow();
            assertTrue(writers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    /**
     * A write with {@code refresh=true} that a hold forwards lands in the hold's target, so the refresh must happen
     * there: refreshing the shard the write was addressed to left the acknowledged document invisible to search until
     * the next scheduled refresh, which is how a split under live writes could count one document short.
     */
    public void testARefreshAskedOfAForwardedWriteHappensWhereTheWriteLanded() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        // No scheduled refresh, so only the refresh the write asks for can make it visible.
        final IndexDescriptor descriptor = new IndexDescriptor(
            "beta",
            "uuid-beta-000000000",
            2,
            MAPPING,
            Settings.builder().put("index.refresh_interval", "-1").build()
        );
        plane.createIndex(descriptor);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("write-hold-refresh"))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            node.leases().renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("beta", 0);
            loop.want("beta", 1);
            loop.tick(clock.get());
            ShardId held = null;
            ShardId other = null;
            for (ShardId open : node.reconciler().openShards()) {
                if (open.id() == 0) {
                    held = open;
                } else {
                    other = open;
                }
            }
            assertNotNull(held);
            assertNotNull(other);
            final IndexDescriptor current = plane.describe("beta").orElseThrow();
            final List<String> toHeld = new java.util.ArrayList<>();
            for (int i = 0; toHeld.size() < 3; i++) {
                if (DocumentRouting.shardFor(current, "doc-" + i) == 0) {
                    toHeld.add("doc-" + i);
                }
            }

            final WriteHold hold = node.fences().hold(held, 60_000L);
            final ToShard target = new ToShard(node, other);
            assertTrue(hold.forwardTo(target));
            try {
                assertEquals(201, send(node, "PUT", "/beta/_doc/" + toHeld.get(0) + "?refresh=true", DOC).status());
                assertEquals("a single write refreshes where it landed", 1, hits(node, toHeld.get(0)));
                final String bulk = "{\"index\":{\"_index\":\"beta\",\"_id\":\"" + toHeld.get(1) + "\"}}\n" + DOC + "\n";
                final Response bulked = send(node, "POST", "/_bulk?refresh=true", bulk);
                assertEquals(bulked.body(), 200, bulked.status());
                assertFalse(bulked.body(), bulked.body().contains("\"errors\":true"));
                assertEquals("a batch refreshes where it landed", 1, hits(node, toHeld.get(1)));
                assertEquals(2, target.refreshes.size());

                send(node, "PUT", "/beta/_doc/" + toHeld.get(2), DOC);
                assertEquals("without refresh=true nothing is refreshed", 0, hits(node, toHeld.get(2)));
                assertEquals(2, target.refreshes.size());
            } finally {
                hold.close();
            }
            assertFalse("the forwarded writes are not in the addressed shard", node.reads().get(held, toHeld.get(0)).found());
            assertTrue(node.reads().get(other, toHeld.get(0)).found());
        }
    }

    private record Response(int status, String body) {
    }

    private static long hits(ServerlessNode node, String id) throws Exception {
        final Response found = send(node, "GET", "/beta/_search?q=_id:" + id, null);
        assertEquals(found.body(), 200, found.status());
        final java.util.regex.Matcher total = java.util.regex.Pattern.compile("\"total\":\\{\"value\":(\\d+)").matcher(found.body());
        assertTrue(found.body(), total.find());
        return Long.parseLong(total.group(1));
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

    private static void assertWaiting(Future<?> write) throws Exception {
        expectThrows(TimeoutException.class, () -> write.get(200, TimeUnit.MILLISECONDS));
    }

    public void testHeldWritesAreForwardedAfterCutOverAndGoOnHereOnceTheHoldCloses() throws Exception {
        withShard("write-hold-forward", (node, shardId, writers) -> {
            node.writes().index(shardId, "before", DOC);
            final WriteHold hold = node.fences().hold(shardId, 60_000L);
            final Future<WriteOutcome> held = writers.submit(() -> node.writes().index(shardId, "held", DOC));
            assertWaiting(held);
            assertFalse("a held write is not applied", node.reads().get(shardId, "held").found());

            final Recorder target = new Recorder();
            assertTrue("forwarded while the hold was in force", hold.forwardTo(target));
            assertEquals("the held write is answered by the target", 1_000L, held.get(10, TimeUnit.SECONDS).seqNo());
            node.writes().delete(shardId, "before");
            node.writes().bulkOperations(shardId, List.of());
            assertEquals("and so is every later one", List.of("held", "delete:before", "bulk:0"), target.ids);
            assertFalse(node.reads().get(shardId, "held").found());
            assertTrue("a forwarded delete is not applied here", node.reads().get(shardId, "before").found());

            hold.close();
            node.writes().index(shardId, "after", DOC);
            assertTrue("once the hold closes, writes go on here", node.reads().get(shardId, "after").found());
            assertEquals(3, target.ids.size());
            assertNotNull("and the shard can be held again", node.fences().hold(shardId, 60_000L).shardId());
        });
    }

    public void testAResumedHoldLetsItsWritesGoOnHere() throws Exception {
        withShard("write-hold-resume", (node, shardId, writers) -> {
            final WriteHold hold = node.fences().hold(shardId, 60_000L);
            expectThrows(IllegalStateException.class, () -> node.fences().hold(shardId, 60_000L));
            final Future<WriteOutcome> held = writers.submit(() -> node.writes().index(shardId, "held", DOC));
            assertWaiting(held);
            assertTrue(hold.resume());
            held.get(10, TimeUnit.SECONDS);
            assertTrue(node.reads().get(shardId, "held").found());
            assertFalse("a resumed hold cannot forward any more", hold.forwardTo(new Recorder()));
            node.writes().index(shardId, "later", DOC);
            assertTrue(node.reads().get(shardId, "later").found());
        });
    }

    public void testAHoldThatRunsOutOfTimeLetsWritesGoOnAndTellsItsHolder() throws Exception {
        withShard("write-hold-expire", (node, shardId, writers) -> {
            final WriteHold hold = node.fences().hold(shardId, 300L);
            final long started = System.nanoTime();
            node.writes().index(shardId, "held", DOC);
            assertTrue("it waited out the hold", System.nanoTime() - started >= TimeUnit.MILLISECONDS.toNanos(250));
            assertTrue(node.reads().get(shardId, "held").found());
            assertFalse("the holder learns its hold ended without it", hold.forwardTo(new Recorder()));
            node.writes().index(shardId, "later", DOC);
            assertTrue(node.reads().get(shardId, "later").found());
        });
    }

    public void testAHoldWaitsForTheWritesAlreadyUnderWay() throws Exception {
        withShard("write-hold-drain", (node, shardId, writers) -> {
            final List<Future<WriteOutcome>> underWay = new CopyOnWriteArrayList<>();
            for (int i = 0; i < 20; i++) {
                final String id = "w" + i;
                underWay.add(writers.submit(() -> node.writes().index(shardId, id, DOC)));
            }
            final WriteHold hold = node.fences().hold(shardId, 60_000L);
            int applied = 0;
            for (int i = 0; i < 20; i++) {
                if (node.reads().get(shardId, "w" + i).found()) {
                    applied++;
                }
            }
            // Every write is either applied by the time the hold returns, or waits and goes to the target.
            final Recorder target = new Recorder();
            assertTrue(hold.forwardTo(target));
            for (Future<WriteOutcome> write : underWay) {
                write.get(10, TimeUnit.SECONDS);
            }
            assertEquals(20, applied + target.ids.size());
            hold.close();
        });
    }
}
