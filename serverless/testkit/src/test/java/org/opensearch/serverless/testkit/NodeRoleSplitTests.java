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
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A fleet split into an ingest pool and a search pool, with a node of neither role in front: whichever node a client
 * reaches, a write lands on an ingest node and a search is answered, so an ingress needs no knowledge of shards.
 *
 * <p>The case that used to fail is the first write to a shard nobody holds, sent to a node that cannot take it: that
 * node tried to take the shard itself and refused the write.
 */
public class NodeRoleSplitTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"keyword\"}}}";

    private Settings nodeSettings(String name, String roles) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-role-split")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .build();
    }

    private static ServerlessNode joined(ServerlessNode node, MetadataPlane plane, List<ReconcileScheduler> schedulers) throws Exception {
        node.startup().start();
        node.setMetadataPlane(plane);
        node.leases().renewLease(plane);
        final ReconcileScheduler scheduler = new ReconcileScheduler(
            new BackgroundReconciler(node, plane).setDemandDrivenActivation(true),
            node.threadPool(),
            plane.clock(),
            TimeValue.timeValueMillis(TTL / ReconcileScheduler.RENEWALS_PER_TTL),
            TimeValue.timeValueMillis(250),
            ReconcileScheduler.DEFAULT_BACKSTOP_INTERVAL
        );
        node.setSignals(scheduler);
        schedulers.add(scheduler.start());
        return node;
    }

    public void testEmptyRolesMakeACoordinatorOnlyNode() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("roles-none", ""))) {
            assertEquals(Set.of(), node.roles());
        }
    }

    public void testAnUnknownRoleIsRefused() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ServerlessNode(nodeSettings("roles-typo", "ingest,serach")).close()
        );
        assertTrue(e.getMessage(), e.getMessage().contains("serach"));
    }

    public void testWritesAndSearchesThroughANodeOfAnyRoleReachTheRightPool() throws Exception {
        final MetadataPlane plane = new MetadataPlane(
            new FsBlobStore(8192, createTempDir(), false),
            BlobPath.cleanPath(),
            System::currentTimeMillis,
            TTL
        );
        for (String index : List.of("via-coordinator", "via-search", "bulk-coordinator", "bulk-search")) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index, 1, MAPPING, null));
        }
        final List<ReconcileScheduler> schedulers = new ArrayList<>();
        try (
            ServerlessNode ingest = joined(new ServerlessNode(nodeSettings("split-ingest", "ingest")), plane, schedulers);
            ServerlessNode search = joined(new ServerlessNode(nodeSettings("split-search", "search")), plane, schedulers);
            ServerlessNode coordinator = joined(new ServerlessNode(nodeSettings("split-coordinator", "")), plane, schedulers)
        ) {
            plane.membership().refresh();
            final String ingestId = ingest.localNode().getId();

            // The first write to each index reaches a node that cannot take its shard.
            for (var entry : List.of(List.of(coordinator, "via-coordinator"), List.of(search, "via-search"))) {
                final ServerlessNode through = (ServerlessNode) entry.get(0);
                final String index = (String) entry.get(1);
                final Response put = send(through, "PUT", "/" + index + "/_doc/1?refresh=true", "{\"msg\":\"first\"}");
                assertEquals(index + ": " + put.body(), 201, put.status());
                assertEquals("the ingest node owns it", ingestId, plane.heads().read(index, 0).orElseThrow().ownerNodeId());
            }
            for (var entry : List.of(List.of(coordinator, "bulk-coordinator"), List.of(search, "bulk-search"))) {
                final ServerlessNode through = (ServerlessNode) entry.get(0);
                final String index = (String) entry.get(1);
                final Response bulk = send(
                    through,
                    "POST",
                    "/" + index + "/_bulk?refresh=true",
                    "{\"index\":{\"_id\":\"1\"}}\n{\"msg\":\"first\"}\n{\"index\":{\"_id\":\"2\"}}\n{\"msg\":\"second\"}\n"
                );
                assertEquals(index + ": " + bulk.body(), 200, bulk.status());
                assertFalse(index + ": " + bulk.body(), bulk.body().contains("\"errors\":true"));
                assertEquals("the ingest node owns it", ingestId, plane.heads().read(index, 0).orElseThrow().ownerNodeId());
            }
            assertTrue("the coordinator holds no shard", coordinator.reconciler().openShards().isEmpty());

            // Searches through every node find the writes, and a get through the coordinator reads the owner.
            for (ServerlessNode through : List.of(coordinator, search, ingest)) {
                for (String index : List.of("via-coordinator", "via-search", "bulk-coordinator", "bulk-search")) {
                    assertBusy(() -> {
                        final Response hits = send(through, "GET", "/" + index + "/_search?q=msg:first", null);
                        assertEquals(hits.body(), 200, hits.status());
                        assertTrue(through.localNode().getName() + " " + index + ": " + hits.body(), hits.body().contains("\"value\":1"));
                    });
                }
            }
            final Response update = send(coordinator, "POST", "/via-search/_update/1", "{\"doc\":{\"msg\":\"updated\"}}");
            assertEquals(update.body(), 200, update.status());
            final Response get = send(coordinator, "GET", "/via-search/_doc/1", null);
            assertEquals(get.body(), 200, get.status());
            assertTrue(get.body(), get.body().contains("\"found\":true"));
            assertTrue("the get reads the owner, which has the update: " + get.body(), get.body().contains("updated"));
            assertTrue("the coordinator still holds no shard", coordinator.reconciler().openShards().isEmpty());
        } finally {
            for (ReconcileScheduler scheduler : schedulers) {
                scheduler.close();
            }
        }
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest.BodyPublisher payload = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", body != null && body.contains("\n{") ? "application/x-ndjson" : "application/json")
                .method(method, payload)
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
