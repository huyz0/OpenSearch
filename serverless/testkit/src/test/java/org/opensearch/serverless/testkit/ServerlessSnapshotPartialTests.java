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
import org.opensearch.serverless.repository.SnapshotRecord;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A take with {@code partial=true} and {@code serverless.snapshots.take.partial_shards} keeps an index whose shards
 * published nothing and records them as failed; a restore of such an index needs {@code partial=true}.
 */
public class ServerlessSnapshotPartialTests extends OpenSearchTestCase {

    private static final long TTL = 300_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name, boolean partialShards) {
        final Settings.Builder builder = Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-snapshot-partial")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest");
        if (partialShards) {
            builder.put("serverless.snapshots.take.partial_shards", true);
        }
        return builder.build();
    }

    private interface Body {
        void run(ServerlessNode node, MetadataPlane plane) throws Exception;
    }

    /** One published one-shard index "one", and "two", which has two shards and has published nothing. */
    private void withIndices(boolean partialShards, Body body) throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        counting = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(counting, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("one", "uuid-one-0000000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("two", "uuid-two-0000000000", 2, MAPPING, null));
        try (
            ServerlessNode node = new ServerlessNode(
                nodeSettings("partial-" + randomAlphaOfLength(5).toLowerCase(java.util.Locale.ROOT), partialShards)
            )
        ) {
            node.startup().start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("one", 0);
            loop.tick(clock.get());
            assertEquals(201, send(node, "PUT", "/one/_doc/1?refresh=true", "{\"msg\":\"doc\"}").status());
            loop.tick(clock.get());
            assertEquals(200, send(node, "PUT", "/_snapshot/backups", null).status());
            body.run(node, plane);
        }
    }

    private CountingBlobStore counting;

    private static Response take(ServerlessNode node, String name, String indices, boolean partial) throws Exception {
        return send(node, "PUT", "/_snapshot/backups/" + name, "{\"indices\":\"" + indices + "\",\"partial\":" + partial + "}");
    }

    public void testWithTheSettingOffPartialStillLeavesTheIndexOut() throws Exception {
        withIndices(false, (node, plane) -> {
            assertEquals(200, take(node, "s", "one,two", true).status());
            final SnapshotRecord record = plane.snapshot("backups", "s").orElseThrow();
            assertEquals(java.util.Set.of("one"), record.indices().keySet());
            final Response info = send(node, "GET", "/_snapshot/backups/s", null);
            assertTrue(info.body(), info.body().contains("\"state\":\"SUCCESS\""));
        });
    }

    public void testWithTheSettingOnAnUnpublishedIndexIsKeptWithItsFailedShards() throws Exception {
        withIndices(true, (node, plane) -> {
            assertEquals(200, take(node, "s", "one,two", true).status());
            final SnapshotRecord record = plane.snapshot("backups", "s").orElseThrow();
            assertEquals(List.of(0, 1), record.indices().get("two").failedShards());
            assertTrue(record.indices().get("two").shards().isEmpty());
            assertEquals(1, record.indices().get("one").shards().size());
            assertTrue(record.indices().get("one").failedShards().isEmpty());
            final Response info = send(node, "GET", "/_snapshot/backups/s", null);
            assertTrue(info.body(), info.body().contains("\"state\":\"PARTIAL\""));
            assertTrue(info.body(), info.body().contains("\"shards\":{\"total\":3,\"failed\":2,\"successful\":1}"));
            assertTrue(info.body(), info.body().contains("\"shard_id\":1"));
            assertTrue(info.body(), info.body().contains("\"index_uuid\":\"uuid-two-0000000000\""));
            assertFalse("no node is known for a shard that never published", info.body().contains("node_id"));
            // The list shows the exact count and none of the entries.
            final Response list = send(node, "GET", "/_snapshot/backups/_all", null);
            assertTrue(list.body(), list.body().contains("\"failed\":2"));
            assertFalse(list.body(), list.body().contains("shard_id"));
        });
    }

    /** With the setting on, a take reads the remaining manifests of an index with a failed shard: one more read here. */
    public void testPartialTakeBudget() throws Exception {
        final long[] cost = new long[2];
        for (int on = 0; on < 2; on++) {
            final int mode = on;
            withIndices(on == 1, (node, plane) -> {
                final long before = counting.total();
                assertEquals(200, take(node, "s", "one,two", true).status());
                cost[mode] = counting.total() - before;
            });
        }
        RequestBudgets.check("snapshot.take_partial.extra_requests", cost[1] - cost[0], "partial take with the setting on minus off");
    }

    public void testWithoutPartialAnUnpublishedShardStillFailsTheTake() throws Exception {
        withIndices(true, (node, plane) -> {
            final Response refused = take(node, "s", "one,two", false);
            assertEquals(refused.body(), 409, refused.status());
            assertTrue(plane.snapshot("backups", "s").isEmpty());
        });
    }

    public void testATakeThatCapturedNothingAnywhereIsRefusedWhateverTheSetting() throws Exception {
        for (boolean on : new boolean[] { false, true }) {
            withIndices(on, (node, plane) -> {
                final Response refused = take(node, "s", "two", true);
                assertEquals(refused.body(), 409, refused.status());
                assertTrue(refused.body(), refused.body().contains("nothing_published"));
                assertTrue(plane.snapshot("backups", "s").isEmpty());
            });
        }
    }

    public void testARestoreOfAFailedIndexNeedsPartialAndGivesBackWholeIndexWithEmptyShards() throws Exception {
        withIndices(true, (node, plane) -> {
            assertEquals(200, take(node, "s", "one,two", true).status());
            final Response refused = send(
                node,
                "POST",
                "/_snapshot/backups/s/_restore?wait_for_completion=true",
                "{\"indices\":\"two\",\"rename_pattern\":\"two\",\"rename_replacement\":\"copy\"}"
            );
            assertEquals(refused.body(), 500, refused.status());
            assertTrue(refused.body(), refused.body().contains("snapshot_restore_exception"));
            assertTrue(refused.body(), refused.body().contains("partial=true"));
            assertTrue(plane.describe("copy").isEmpty());
            assertEquals(400, send(node, "POST", "/_snapshot/backups/s/_restore", "{\"partial\":\"maybe\"}").status());

            final Response restored = send(
                node,
                "POST",
                "/_snapshot/backups/s/_restore?wait_for_completion=true",
                "{\"indices\":\"one,two\",\"partial\":true,\"rename_pattern\":\"(one|two)\",\"rename_replacement\":\"c-$1\"}"
            );
            assertEquals(restored.body(), 200, restored.status());
            assertTrue(restored.body(), restored.body().contains("\"shards\":{\"total\":3,\"failed\":2,\"successful\":1}"));
            assertEquals(2, plane.describe("c-two").orElseThrow().numberOfShards());
            assertEquals(200, send(node, "GET", "/c-one/_doc/1", null).status());
        });
    }

    public void testPartialOnASnapshotWithNoFailedShardChangesNothing() throws Exception {
        withIndices(true, (node, plane) -> {
            assertEquals(200, take(node, "s", "one", true).status());
            final Response restored = send(
                node,
                "POST",
                "/_snapshot/backups/s/_restore?wait_for_completion=true",
                "{\"partial\":true,\"rename_pattern\":\"one\",\"rename_replacement\":\"again\"}"
            );
            assertEquals(restored.body(), 200, restored.status());
            assertTrue(restored.body(), restored.body().contains("\"failed\":0"));
            assertTrue(send(node, "GET", "/_snapshot/backups/s", null).body().contains("\"state\":\"SUCCESS\""));
        });
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
