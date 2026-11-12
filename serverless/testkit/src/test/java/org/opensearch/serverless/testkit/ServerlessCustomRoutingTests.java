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
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.engine.ReplaySpool;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.reconcile.ReconcileScheduler;
import org.opensearch.serverless.rest.DocumentRouting;
import org.opensearch.serverless.routing.ShardMode;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.split.UnitSplitMover;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Custom routing end to end: a routing value places a document instead of its id, is stored with it as
 * {@code _routing}, travels in its node-log record, and so survives a crash replay and a unit split. Covers the legacy
 * layout and both shard modes.
 *
 * <p>Runs against {@code FsBlobContainer} only.
 */
public class ServerlessCustomRoutingTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPINGS = "\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-custom-routing")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.window_millis", 100)
            .build();
    }

    private record Response(int status, String body) {
    }

    private static String create(int shards, String mode, String extraMappings) {
        final String modeSetting = mode == null ? "" : ",\"" + ShardMode.SETTING + "\":\"" + mode + "\"";
        final String mappings = extraMappings == null
            ? MAPPINGS
            : "\"mappings\":{" + extraMappings + ",\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";
        return "{\"settings\":{\"number_of_shards\":" + shards + modeSetting + "}," + mappings + "}";
    }

    // ---- the record formats ----

    /** A record without routing is byte-for-byte what it was, and a record written before routing existed still parses. */
    public void testTheLogRecordCarriesRoutingAndStaysCompatible() throws Exception {
        final WalRecord plain = new WalRecord("a", "{\"x\":1}", 4L, 2L, 1L);
        final String plainJson = new String(
            org.opensearch.core.common.bytes.BytesReference.toBytes(plain.toBytes()),
            StandardCharsets.UTF_8
        );
        assertFalse("no routing field when there is no routing: " + plainJson, plainJson.contains("routing"));
        assertEquals("{\"id\":\"a\",\"source\":\"{\\\"x\\\":1}\",\"seq_no\":4,\"primary_term\":2,\"version\":1}", plainJson);

        // What a node that predates routing wrote: the same bytes, read back with no routing.
        final WalRecord old = WalRecord.fromStream(new ByteArrayInputStream(plainJson.getBytes(StandardCharsets.UTF_8)));
        assertNull(old.routing());
        assertEquals("a", old.placedBy());

        final WalRecord routed = plain.withRouting("tenant-7");
        final WalRecord back = WalRecord.fromStream(
            new ByteArrayInputStream(org.opensearch.core.common.bytes.BytesReference.toBytes(routed.toBytes()))
        );
        assertEquals("tenant-7", back.routing());
        assertEquals("tenant-7", back.placedBy());
        assertEquals(4L, back.seqNo());
        final WalRecord routedDelete = WalRecord.deletion("b", 5L, 2L, 2L).withRouting("tenant-7");
        final WalRecord deleteBack = WalRecord.fromStream(
            new ByteArrayInputStream(org.opensearch.core.common.bytes.BytesReference.toBytes(routedDelete.toBytes()))
        );
        assertTrue(deleteBack.isDeletion());
        assertEquals("tenant-7", deleteBack.routing());
        assertEquals("a new source keeps the routing", "tenant-7", routed.withSource("{}").routing());
        expectThrows(IllegalArgumentException.class, () -> plain.withRouting(""));
    }

    /** The local replay spool keeps routing for both sequenced and legacy records. */
    public void testTheReplaySpoolKeepsRouting() throws Exception {
        final List<WalRecord> records = List.of(
            new WalRecord("a", "{\"n\":1}", 0L, 1L, 1L).withRouting("r1"),
            new WalRecord("b", "{\"n\":2}", 1L, 1L, 1L),
            WalRecord.deletion("c", 2L, 1L, 1L).withRouting("r2"),
            new WalRecord("d", "{\"n\":4}").withRouting("r3")
        );
        try (ReplaySpool spool = ReplaySpool.capture(createTempDir(), consumer -> {
            for (WalRecord record : records) {
                consumer.accept(record);
            }
        })) {
            final List<String> routings = new ArrayList<>();
            try (Translog.Snapshot snapshot = spool.snapshot()) {
                for (Translog.Operation op; (op = snapshot.next()) != null;) {
                    routings.add(op instanceof Translog.Index index ? index.id() + "~" + index.routing() : "delete");
                }
            }
            assertEquals(List.of("a~r1", "b~null", "delete"), routings);
            final List<WalRecord> legacy = new ArrayList<>();
            spool.forEachLegacy(legacy::add);
            assertEquals(1, legacy.size());
            assertEquals("r3", legacy.get(0).routing());
        }
    }

    // ---- the request path ----

    private interface Body {
        void run(ServerlessNode node, MetadataPlane plane, BackgroundReconciler loop) throws Exception;
    }

    private void onOneNode(String name, Body body) throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        try (ServerlessNode node = new ServerlessNode(nodeSettings(name))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true);
            try (
                ReconcileScheduler scheduler = new ReconcileScheduler(
                    loop,
                    node.threadPool(),
                    clock::get,
                    TimeValue.timeValueHours(1),
                    TimeValue.timeValueHours(1),
                    null
                )
            ) {
                node.setSignals(scheduler);
                scheduler.start();
                body.run(node, plane, loop);
            }
        }
    }

    public void testRoutedRequestsInALegacyIndex() throws Exception {
        onOneNode("routing-legacy", (node, plane, loop) -> exerciseRoutedRequests(node, plane, "legacy", null));
    }

    public void testRoutedRequestsInAFixedIndex() throws Exception {
        onOneNode("routing-fixed", (node, plane, loop) -> exerciseRoutedRequests(node, plane, "fixed", "fixed"));
    }

    public void testRoutedRequestsInAnAdaptiveIndex() throws Exception {
        onOneNode("routing-adaptive", (node, plane, loop) -> exerciseRoutedRequests(node, plane, "adaptive", "adaptive"));
    }

    /** Every routed request kind against one index: placement by routing, reads by routing, and a narrowed search. */
    private void exerciseRoutedRequests(ServerlessNode node, MetadataPlane plane, String index, String mode) throws Exception {
        final int shards = 4;
        assertEquals(200, send(node, "PUT", "/" + index, create(shards, mode, null)).status());
        final IndexDescriptor descriptor = plane.describe(index).orElseThrow();
        final String routing = "tenant-a";
        final int routedShard = DocumentRouting.shardFor(descriptor, "any", routing);
        // An id whose own hash lands elsewhere, so finding it proves the routing placed it.
        // An adaptive index starts as one unit, where every id lands with the routing; then any id will do.
        String elsewhere = descriptor.unitIds().length == 1 ? "doc-0" : null;
        for (int i = 0; elsewhere == null; i++) {
            if (DocumentRouting.shardFor(descriptor, "doc-" + i) != routedShard) {
                elsewhere = "doc-" + i;
            }
        }

        final Response put = send(node, "PUT", "/" + index + "/_doc/" + elsewhere + "?routing=" + routing, "{\"msg\":\"one\",\"n\":1}");
        assertEquals(put.body(), 201, put.status());
        final Response got = send(node, "GET", "/" + index + "/_doc/" + elsewhere + "?routing=" + routing, null);
        assertEquals(got.body(), 200, got.status());
        assertTrue(got.body(), got.body().contains("\"_routing\":\"" + routing + "\""));
        assertTrue(got.body(), got.body().contains("\"_shard\":" + routedShard));
        if (descriptor.unitIds().length > 1) {
            assertEquals(
                "without the routing the get looks in the id's shard",
                404,
                send(node, "GET", "/" + index + "/_doc/" + elsewhere, null).status()
            );
        }

        // Bulk: the path's routing is the default, an action line's own routing overrides it. A bulk does not take an
        // unowned shard itself, so one write first takes tenant-b's.
        assertEquals(201, send(node, "PUT", "/" + index + "/_doc/seed-b?routing=tenant-b", "{\"msg\":\"seed\",\"n\":0}").status());
        final StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 6; i++) {
            bulk.append("{\"index\":{\"_id\":\"bulk-").append(i).append("\"}}\n{\"msg\":\"bulk\",\"n\":").append(i).append("}\n");
        }
        bulk.append("{\"index\":{\"_id\":\"other\",\"routing\":\"tenant-b\"}}\n{\"msg\":\"other\",\"n\":9}\n");
        final Response bulked = send(node, "POST", "/" + index + "/_bulk?routing=" + routing + "&refresh=true", bulk.toString());
        assertEquals(bulked.body(), 200, bulked.status());
        assertFalse(bulked.body(), bulked.body().contains("\"errors\":true"));
        assertTrue(send(node, "GET", "/" + index + "/_doc/other?routing=tenant-b", null).body().contains("\"_routing\":\"tenant-b\""));

        // Update with routing, then mget with a per-item routing.
        final Response updated = send(
            node,
            "POST",
            "/" + index + "/_update/bulk-2?routing=" + routing + "&refresh=true",
            "{\"doc\":{\"msg\":\"changed\"}}"
        );
        assertEquals(updated.body(), 200, updated.status());
        final Response mget = send(
            node,
            "POST",
            "/" + index + "/_mget",
            "{\"docs\":[{\"_id\":\"bulk-2\",\"routing\":\"" + routing + "\"},{\"_id\":\"other\",\"routing\":\"tenant-b\"}]}"
        );
        assertEquals(mget.body(), 200, mget.status());
        assertTrue(mget.body(), mget.body().contains("changed"));
        assertFalse(mget.body(), mget.body().contains("\"found\":false"));

        // Delete with routing.
        assertEquals(200, send(node, "DELETE", "/" + index + "/_doc/bulk-5?routing=" + routing + "&refresh=true", null).status());
        assertEquals(404, send(node, "GET", "/" + index + "/_doc/bulk-5?routing=" + routing, null).status());

        // A routed search visits the routed shard only.
        send(node, "POST", "/" + index + "/_refresh", null);
        final Response routedSearch = send(node, "GET", "/" + index + "/_search?routing=" + routing + "&q=msg:*", null);
        assertEquals(routedSearch.body(), 200, routedSearch.status());
        assertTrue(routedSearch.body(), routedSearch.body().contains("\"total\":1,"));
        final int otherShard = DocumentRouting.shardFor(descriptor, "any", "tenant-b");
        final String expectedTotal = otherShard == routedShard ? "\"value\":8" : "\"value\":6";
        assertTrue(routedSearch.body(), routedSearch.body().contains(expectedTotal));
        final Response both = send(node, "GET", "/" + index + "/_search?routing=" + routing + ",tenant-b&q=msg:*", null);
        assertTrue(both.body(), both.body().contains("\"value\":8"));
        final Response all = send(node, "GET", "/" + index + "/_search?q=msg:*", null);
        assertTrue(all.body(), all.body().contains("\"total\":" + descriptor.unitIds().length + ","));
    }

    /** A mapping that requires routing refuses a document without it, as core does. */
    public void testRequiredRoutingIsEnforced() throws Exception {
        onOneNode("routing-required", (node, plane, loop) -> {
            assertEquals(200, send(node, "PUT", "/strict", create(2, "fixed", "\"_routing\":{\"required\":true}")).status());
            final Response refused = send(node, "PUT", "/strict/_doc/x", "{\"msg\":\"x\",\"n\":1}");
            assertEquals(refused.body(), 400, refused.status());
            assertTrue(refused.body(), refused.body().contains("routing_missing_exception"));
            assertEquals(201, send(node, "PUT", "/strict/_doc/x?routing=r", "{\"msg\":\"x\",\"n\":1}").status());
            final Response bulk = send(node, "POST", "/strict/_bulk", "{\"index\":{\"_id\":\"y\"}}\n{\"msg\":\"y\",\"n\":2}\n");
            assertTrue(bulk.body(), bulk.body().contains("routing_missing_exception"));
        });
    }

    // ---- crash replay ----

    public void testRoutedWritesSurviveACrashInEveryMode() throws Exception {
        for (String mode : new String[] { null, "fixed", "adaptive" }) {
            routedWritesSurviveACrash(mode);
        }
    }

    /**
     * A writer acknowledges routed writes and a routed delete that only its node log holds, then dies. The successor
     * replays the log, and every routed document is where its routing says, with its {@code _routing}.
     */
    private void routedWritesSurviveACrash(String mode) throws Exception {
        final String index = "crash-" + (mode == null ? "legacy" : mode);
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final ServerlessNode first = new ServerlessNode(nodeSettings(index + "-first"));
        final ServerlessNode second = new ServerlessNode(nodeSettings(index + "-second"));
        try {
            first.startup().start();
            first.setMetadataPlane(plane);
            assertEquals(200, send(first, "PUT", "/" + index, create(4, mode, null)).status());
            final IndexDescriptor descriptor = plane.describe(index).orElseThrow();
            final BackgroundReconciler firstLoop = new BackgroundReconciler(first, plane);
            takeAll(first, plane, clock, firstLoop, index, descriptor);

            for (int i = 0; i < 8; i++) {
                assertEquals(
                    201,
                    send(first, "PUT", "/" + index + "/_doc/published-" + i + "?routing=r" + (i % 3), "{\"msg\":\"p\",\"n\":" + i + "}")
                        .status()
                );
            }
            firstLoop.publishAll();
            // Only the node log holds these.
            for (int i = 0; i < 8; i++) {
                assertEquals(
                    201,
                    send(first, "PUT", "/" + index + "/_doc/logged-" + i + "?routing=r" + (i % 3), "{\"msg\":\"l\",\"n\":" + i + "}")
                        .status()
                );
            }
            assertEquals(200, send(first, "DELETE", "/" + index + "/_doc/published-1?routing=r1", null).status());
            assertEquals(
                200,
                send(first, "POST", "/" + index + "/_bulk?routing=r0", "{\"index\":{\"_id\":\"bulked\"}}\n{\"msg\":\"b\",\"n\":0}\n")
                    .status()
            );
            assertTrue("the first writer logged through its node log", first.nodeLogWriter().committer().blobs() > 0);

            // The first writer never lets go; its lease runs out on the shared clock.
            clock.addAndGet(2 * TTL);
            second.startup().start();
            second.setMetadataPlane(plane);
            takeAll(second, plane, clock, new BackgroundReconciler(second, plane), index, descriptor);

            for (int i = 0; i < 8; i++) {
                for (String prefix : new String[] { "published-", "logged-" }) {
                    final String id = prefix + i;
                    final String routing = "r" + (i % 3);
                    final Response got = send(second, "GET", "/" + index + "/_doc/" + id + "?routing=" + routing, null);
                    if (id.equals("published-1")) {
                        assertEquals("the routed delete was replayed: " + got.body(), 404, got.status());
                        continue;
                    }
                    assertEquals(mode + " " + id + ": " + got.body(), 200, got.status());
                    assertTrue(got.body(), got.body().contains("\"_routing\":\"" + routing + "\""));
                    // And it is in the shard its routing names, read straight from that shard.
                    final ShardId placed = new ShardId(index, descriptor.uuid(), DocumentRouting.shardFor(descriptor, id, routing));
                    assertTrue(mode + " " + id + " in " + placed, second.reads().get(placed, id).found());
                }
            }
            assertEquals(200, send(second, "GET", "/" + index + "/_doc/bulked?routing=r0", null).status());
        } finally {
            closeQuietly(second);
            closeQuietly(first);
        }
    }

    private static void takeAll(
        ServerlessNode node,
        MetadataPlane plane,
        AtomicLong clock,
        BackgroundReconciler loop,
        String index,
        IndexDescriptor descriptor
    ) throws Exception {
        node.leases().renewLease(plane);
        plane.membership().refresh();
        for (int unit : descriptor.unitIds()) {
            loop.want(index, unit);
        }
        loop.tick(clock.get());
        assertEquals(
            descriptor.unitIds().length,
            node.reconciler().openShards().stream().filter(s -> s.getIndexName().equals(index)).count()
        );
    }

    private static void closeQuietly(ServerlessNode node) {
        try {
            node.close();
        } catch (Exception e) {
            // It lost every shard it held; closing it may report that.
        }
    }

    // ---- unit split ----

    /**
     * Routed documents, some published and some only in the log tail, follow their routing into the halves of a split,
     * including when their id alone would send them to the other half.
     */
    public void testRoutedDocumentsFollowTheirRoutingThroughASplit() throws Exception {
        onOneNode("routing-split", (node, plane, loop) -> {
            assertEquals(200, send(node, "PUT", "/grow", create(4, "adaptive", null)).status());
            final IndexDescriptor before = plane.describe("grow").orElseThrow();
            assertEquals("an adaptive index starts as one unit", 1, before.unitIds().length);
            final var split = before.layout().planSplit(0);
            final var after = before.layout().apply(split);

            // Routing values for each half, and ids that hash to the other half, so only routing explains the placement.
            final String leftRouting = valueIn(after, split.left().id(), "tenant-");
            final String rightRouting = valueIn(after, split.right().id(), "tenant-");
            final List<String[]> written = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                final boolean left = i % 2 == 0;
                final String routing = left ? leftRouting : rightRouting;
                final String id = valueIn(after, left ? split.right().id() : split.left().id(), "p" + i + "-");
                assertEquals(
                    201,
                    send(node, "PUT", "/grow/_doc/" + id + "?routing=" + routing, "{\"msg\":\"p\",\"n\":" + i + "}").status()
                );
                written.add(new String[] { id, routing });
            }
            loop.publishAll();
            for (int i = 0; i < 6; i++) {
                final boolean left = i % 2 == 0;
                final String routing = left ? leftRouting : rightRouting;
                final String id = valueIn(after, left ? split.right().id() : split.left().id(), "t" + i + "-");
                assertEquals(
                    201,
                    send(node, "PUT", "/grow/_doc/" + id + "?routing=" + routing, "{\"msg\":\"t\",\"n\":" + i + "}").status()
                );
                written.add(new String[] { id, routing });
            }
            final String[] deleted = written.get(written.size() - 1);
            assertEquals(200, send(node, "DELETE", "/grow/_doc/" + deleted[0] + "?routing=" + deleted[1], null).status());

            final Optional<UnitSplitMover.Moved> moved = new UnitSplitMover(node, plane).setRoutingCacheMillis(300)
                .split("grow", 0, createTempDir());
            assertTrue("the split went through", moved.isPresent());
            final IndexDescriptor switched = plane.describe("grow").orElseThrow();
            assertArrayEquals(new int[] { split.left().id(), split.right().id() }, switched.unitIds());

            for (String[] doc : written) {
                final Response got = send(node, "GET", "/grow/_doc/" + doc[0] + "?routing=" + doc[1], null);
                if (doc == deleted) {
                    assertEquals("the routed delete in the tail went to the routed half: " + got.body(), 404, got.status());
                    continue;
                }
                assertEquals(doc[0] + ": " + got.body(), 200, got.status());
                assertTrue(got.body(), got.body().contains("\"_routing\":\"" + doc[1] + "\""));
                final int half = switched.layout().unitForRouting(doc[1]).id();
                assertTrue(doc[0] + " in unit " + half, node.reads().get(new ShardId("grow", switched.uuid(), half), doc[0]).found());
            }
            send(node, "POST", "/grow/_refresh", null);
            final Response leftSearch = send(node, "GET", "/grow/_search?routing=" + leftRouting + "&q=msg:*", null);
            assertTrue(leftSearch.body(), leftSearch.body().contains("\"total\":1,"));
            assertTrue(leftSearch.body(), leftSearch.body().contains("\"value\":6"));
        });
    }

    /** Finds a value with a prefix that routes to a unit of the layout. */
    private static String valueIn(org.opensearch.serverless.routing.UnitLayout layout, int unit, String prefix) {
        for (int i = 0;; i++) {
            if (layout.unitForRouting(prefix + i).id() == unit) {
                return prefix + i;
            }
        }
    }

    private static Response send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(
                    URI.create(
                        "http://"
                            + address.getAddress()
                            + ":"
                            + address.getPort()
                            + path.replace("*", URLEncoder.encode("*", StandardCharsets.UTF_8))
                    )
                )
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
