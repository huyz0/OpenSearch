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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.reconcile.ReconcileScheduler;
import org.opensearch.serverless.rest.DocumentRouting;
import org.opensearch.serverless.routing.RoutingHash;
import org.opensearch.serverless.routing.ShardMode;
import org.opensearch.serverless.routing.UnitLayout;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Indices created with a shard mode, end to end on one node: the mode lands in the descriptor as a unit
 * layout, documents route by the 64-bit hash of their id through it, and the user still sees the shard
 * count they declared. An index created without a mode is unchanged.
 *
 * <p>Runs against {@code FsBlobContainer} only.
 */
public class ServerlessShardModeTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPINGS = "\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-shard-mode")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private record Response(int status, String body) {
    }

    private interface Body {
        void run(ServerlessNode node, MetadataPlane plane, BackgroundReconciler loop) throws Exception;
    }

    private void onOneNode(String name, Body body) throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
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

    private static String create(int shards, String mode) {
        final String modeSetting = mode == null ? "" : ",\"" + ShardMode.SETTING + "\":\"" + mode + "\"";
        return "{\"settings\":{\"number_of_shards\":" + shards + modeSetting + "}," + MAPPINGS + "}";
    }

    /** A fixed index has one unit per declared shard, and each document lands in the unit its id's hash names. */
    public void testAFixedIndexRoutesDocumentsByTheirHash() throws Exception {
        onOneNode("fixed", (node, plane, loop) -> {
            assertEquals(200, send(node, "PUT", "/fixed", create(4, "fixed")).status());
            final IndexDescriptor descriptor = plane.describe("fixed").orElseThrow();
            assertNotNull(descriptor.layout());
            assertEquals(ShardMode.FIXED, descriptor.layout().mode());
            assertEquals(4, descriptor.numberOfShards());
            assertEquals(4, descriptor.declaredShards());

            final Set<Integer> used = new HashSet<>();
            for (int doc = 0; doc < 24; doc++) {
                final String id = "doc-" + doc;
                final int expected = RoutingHash.declaredShard(RoutingHash.of(id), 4);
                assertEquals(expected, DocumentRouting.shardFor(descriptor, id));
                used.add(expected);
                final Response put = send(node, "PUT", "/fixed/_doc/" + id + "?refresh=true", "{\"msg\":\"hello\",\"n\":" + doc + "}");
                assertEquals(put.body(), 201, put.status());
                assertTrue(put.body(), put.body().contains("\"_shard\":" + expected) || put.body().contains("\"_id\":\"" + id + "\""));
            }
            assertTrue("24 ids should spread over more than one of 4 units: " + used, used.size() > 1);
            for (int doc = 0; doc < 24; doc++) {
                final Response got = send(node, "GET", "/fixed/_doc/doc-" + doc, null);
                assertEquals(got.body(), 200, got.status());
                assertTrue(got.body(), got.body().contains("\"found\":true"));
            }
            final Response count = send(node, "GET", "/fixed/_count", null);
            assertTrue(count.body(), count.body().contains("\"count\":24"));
            final Response searched = send(node, "GET", "/fixed/_search?q=msg:hello", null);
            assertTrue(searched.body(), searched.body().contains("\"total\":4"));
        });
    }

    /** A restored index keeps its unit layout, so its documents are found where they were routed. */
    public void testARestoredIndexKeepsItsLayout() throws Exception {
        onOneNode("restore", (node, plane, loop) -> {
            assertEquals(200, send(node, "PUT", "/kept", create(3, "fixed")).status());
            for (int doc = 0; doc < 9; doc++) {
                assertEquals(
                    201,
                    send(node, "PUT", "/kept/_doc/doc-" + doc + "?refresh=true", "{\"msg\":\"kept\",\"n\":" + doc + "}").status()
                );
            }
            // A snapshot captures published commits, so every unit publishes first.
            loop.publishAll();
            assertEquals(200, send(node, "PUT", "/_snapshot/backups", "{\"type\":\"fs\"}").status());
            final Response captured = send(node, "PUT", "/_snapshot/backups/one", "{\"indices\":\"kept\"}");
            assertEquals(captured.body(), 200, captured.status());
            final Response restored = send(
                node,
                "POST",
                "/_snapshot/backups/one/_restore",
                "{\"rename_pattern\":\"kept\",\"rename_replacement\":\"again\"}"
            );
            assertEquals(restored.body(), 200, restored.status());
            final IndexDescriptor original = plane.describe("kept").orElseThrow();
            final IndexDescriptor copy = plane.describe("again").orElseThrow();
            assertEquals(original.layout(), copy.layout());
            assertFalse(
                "the layout is not a setting core sees",
                copy.extraSettings().keySet().stream().anyMatch(k -> k.contains("layout"))
            );
            for (int doc = 0; doc < 9; doc++) {
                final Response got = send(node, "GET", "/again/_doc/doc-" + doc, null);
                assertTrue(got.body(), got.body().contains("\"found\":true"));
            }
        });
    }

    /**
     * An index whose first unit was split: documents route to the two live units, and search, gets, health
     * and the catalog cover those two and not the retired slot.
     */
    public void testASplitLayoutServesOnlyItsLiveUnits() throws Exception {
        onOneNode("split", (node, plane, loop) -> {
            final UnitLayout initial = UnitLayout.initial(ShardMode.ADAPTIVE, 4);
            final UnitLayout split = initial.apply(initial.planSplit(0));
            final IndexDescriptor descriptor = new IndexDescriptor(
                "halves",
                "uuid-halves-00000000",
                1,
                "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}",
                null
            ).withLayout(split);
            plane.createIndex(descriptor);
            assertEquals(3, descriptor.numberOfShards());
            final Set<Integer> used = new HashSet<>();
            for (int doc = 0; doc < 20; doc++) {
                final String id = "doc-" + doc;
                final int unit = DocumentRouting.shardFor(descriptor, id);
                assertTrue("routed to a live unit: " + unit, unit == 1 || unit == 2);
                used.add(unit);
                final Response put = send(node, "PUT", "/halves/_doc/" + id + "?refresh=true", "{\"msg\":\"half\",\"n\":" + doc + "}");
                assertEquals(put.body(), 201, put.status());
            }
            assertEquals(Set.of(1, 2), used);
            for (int doc = 0; doc < 20; doc++) {
                assertTrue(send(node, "GET", "/halves/_doc/doc-" + doc, null).body().contains("\"found\":true"));
            }
            final Response count = send(node, "GET", "/halves/_count", null);
            assertTrue(count.body(), count.body().contains("\"count\":20"));
            assertTrue("two units searched, not three slots: " + count.body(), count.body().contains("\"total\":2"));
            final Response health = send(node, "GET", "/_cluster/health/halves?level=indices", null);
            assertEquals(health.body(), 200, health.status());
            assertFalse("the retired slot is not reported: " + health.body(), health.body().contains("\"active_shards\":3"));
            final Response catalog = send(node, "GET", "/_serverless/shards/halves", null);
            assertEquals(catalog.body(), 200, catalog.status());
            assertFalse("the retired slot is not listed: " + catalog.body(), catalog.body().contains("\"shard\":0"));
            assertTrue(catalog.body(), catalog.body().contains("\"shard\":1") && catalog.body().contains("\"shard\":2"));
        });
    }

    /** An adaptive index is one unit whatever it declared, and still reports the declared count. */
    public void testAnAdaptiveIndexIsOneUnitAndReportsTheDeclaredCount() throws Exception {
        onOneNode("adaptive", (node, plane, loop) -> {
            assertEquals(200, send(node, "PUT", "/grow", create(6, "adaptive")).status());
            final IndexDescriptor descriptor = plane.describe("grow").orElseThrow();
            assertEquals(ShardMode.ADAPTIVE, descriptor.layout().mode());
            assertEquals("one unit, one physical shard", 1, descriptor.numberOfShards());
            assertEquals(6, descriptor.declaredShards());

            final Response settings = send(node, "GET", "/grow/_settings", null);
            assertTrue(settings.body(), settings.body().contains("\"number_of_shards\":\"6\""));
            assertTrue(settings.body(), settings.body().contains("\"serverless.shard_mode\":\"adaptive\""));

            // A single-document write activates the unit; a batch to a shard nobody owns yet is refused.
            final Response first = send(node, "PUT", "/grow/_doc/doc-0", "{\"msg\":\"grow\",\"n\":0}");
            assertEquals(first.body(), 201, first.status());
            assertTrue(first.body(), first.body().contains("\"_shard\":0") || first.body().contains("\"result\":\"created\""));
            final StringBuilder bulk = new StringBuilder();
            for (int doc = 1; doc < 30; doc++) {
                assertEquals(0, DocumentRouting.shardFor(descriptor, "doc-" + doc));
                bulk.append("{\"index\":{\"_id\":\"doc-").append(doc).append("\"}}\n");
                bulk.append("{\"msg\":\"grow\",\"n\":").append(doc).append("}\n");
            }
            assertEquals(0, DocumentRouting.shardFor(descriptor, "doc-0"));
            final Response written = send(node, "POST", "/grow/_bulk?refresh=true", bulk.toString());
            assertEquals(written.body(), 200, written.status());
            assertTrue(written.body(), written.body().contains("\"errors\":false"));
            final Response got = send(node, "GET", "/grow/_doc/doc-17", null);
            assertTrue(got.body(), got.body().contains("\"found\":true"));
            final Response count = send(node, "GET", "/grow/_count", null);
            assertTrue(count.body(), count.body().contains("\"count\":30"));
        });
    }

    /** Without a mode nothing changes: no layout, core's routing, and a descriptor as it was written before. */
    public void testAnIndexWithoutAModeKeepsItsRouting() throws Exception {
        onOneNode("plain", (node, plane, loop) -> {
            assertEquals(200, send(node, "PUT", "/plain", create(3, null)).status());
            final IndexDescriptor descriptor = plane.describe("plain").orElseThrow();
            assertNull(descriptor.layout());
            assertEquals(3, descriptor.numberOfShards());
            assertFalse(descriptor.toBytes().utf8ToString().contains("unit_layout"));
            for (int doc = 0; doc < 20; doc++) {
                final String id = "doc-" + doc;
                assertEquals(
                    org.opensearch.cluster.routing.OperationRouting.generateShardId(descriptor.routingMetadata(), id, null),
                    DocumentRouting.shardFor(descriptor, id)
                );
            }
            final Response settings = send(node, "GET", "/plain/_settings", null);
            assertFalse(settings.body(), settings.body().contains("shard_mode"));
        });
    }

    /** A mode that is not one is refused at creation, and the mode cannot be changed afterwards. */
    public void testTheModeIsValidatedAndSetOnlyAtCreation() throws Exception {
        onOneNode("validate", (node, plane, loop) -> {
            final Response bad = send(node, "PUT", "/bad", create(2, "elastic"));
            assertEquals(bad.body(), 400, bad.status());
            assertTrue(bad.body(), bad.body().contains("[fixed] or [adaptive]"));
            assertTrue(plane.describe("bad").isEmpty());

            assertEquals(200, send(node, "PUT", "/set", create(2, "fixed")).status());
            final Response change = send(node, "PUT", "/set/_settings", "{\"" + ShardMode.SETTING + "\":\"adaptive\"}");
            assertEquals(change.body(), 501, change.status());
            assertTrue(change.body(), change.body().contains("cannot be changed"));
            assertEquals(ShardMode.FIXED, plane.describe("set").orElseThrow().layout().mode());
        });
    }

    private static Response send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", path.contains("_bulk") ? "application/x-ndjson" : "application/json")
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
