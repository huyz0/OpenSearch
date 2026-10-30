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
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.BulkByScrollTask;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shard.ShardOperations;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

public class DeleteByQueryResponsePolicyTests extends OpenSearchTestCase {
    private static BulkByScrollResponse answer(long total, long deleted, boolean timedOut) {
        final var status = new BulkByScrollTask.Status(
            null,
            total,
            0,
            0,
            deleted,
            total == 0 ? 0 : 1,
            0,
            0,
            0,
            0,
            TimeValue.ZERO,
            Float.POSITIVE_INFINITY,
            null,
            TimeValue.ZERO
        );
        return new BulkByScrollResponse(TimeValue.timeValueMillis(17), status, List.of(), List.of(), timedOut);
    }

    private void withNode(PolicyFilterPlugin plugin, org.opensearch.common.CheckedBiConsumer<ServerlessNode, MetadataPlane, Exception> work)
        throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "dbq-response-policy")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings, List.of(plugin))) {
                node.start();
                node.setMetadataPlane(metadata);
                metadata.createIndex(new IndexDescriptor("original", "uuid-original", 1, "{}", null));
                final var shard = node.activateWriter(metadata, "original", 0).orElseThrow();
                new ShardOperations(node, metadata).index("original", "keep", "{\"value\":1}", true);
                node.publishShard(shard, metadata.heads().read("original", 0).orElseThrow().term());
                work.accept(node, metadata);
            }
        }
    }

    private static HttpResponse<String> delete(ServerlessNode node) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            return client.send(
                HttpRequest.newBuilder(
                    URI.create("http://" + address.getAddress() + ":" + address.getPort() + "/original/_delete_by_query")
                )
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"query\":{\"match_all\":{}}}"))
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            );
        }
    }

    public void testFiltersSeeTheActualNativeDeleteByQueryResponse() throws Exception {
        withNode(PolicyFilterPlugin.response(DeleteByQueryAction.NAME, response -> {
            final var result = (BulkByScrollResponse) response;
            assertEquals(1L, result.getDeleted());
            assertEquals(1L, result.getTotal());
            return response;
        }), (node, metadata) -> {
            final var result = delete(node);
            assertEquals(result.body(), 200, result.statusCode());
        });
    }

    public void testResponsePoliciesCanReplaceCounters() throws Exception {
        withNode(PolicyFilterPlugin.response(DeleteByQueryAction.NAME, response -> answer(0, 0, false)), (node, metadata) -> {
            final var result = delete(node);
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"deleted\":0"));
            assertTrue(result.body(), result.body().contains("\"matched\":0"));
            assertFalse(new ShardOperations(node, metadata).get("original", "keep").document().found());
        });
    }

    public void testCachedResponsesDoNotDeleteDocuments() throws Exception {
        withNode(PolicyFilterPlugin.cached(DeleteByQueryAction.NAME, answer(0, 0, false)), (node, metadata) -> {
            final var result = delete(node);
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(new ShardOperations(node, metadata).get("original", "keep").document().found());
        });
    }

    public void testLibraryUsesCachedNativeCountersWithoutDeleting() throws Exception {
        withNode(PolicyFilterPlugin.cached(DeleteByQueryAction.NAME, answer(7, 3, false)), (node, metadata) -> {
            final var operations = new ShardOperations(node, metadata);
            final var result = operations.deleteByQuery(
                "original",
                org.opensearch.index.query.QueryBuilders.matchAllQuery(),
                Long.MAX_VALUE,
                false
            );
            assertEquals(7L, result.matched());
            assertEquals(3L, result.deleted());
            assertTrue(operations.get("original", "keep").document().found());
        });
    }

    public void testCachedTimeoutDeterminesHttpStatus() throws Exception {
        withNode(PolicyFilterPlugin.cached(DeleteByQueryAction.NAME, answer(0, 0, true)), (node, metadata) -> {
            final var result = delete(node);
            assertEquals(result.body(), 408, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"timed_out\":true"));
            assertTrue(new ShardOperations(node, metadata).get("original", "keep").document().found());
        });
    }

    public void testCachedNativeSerializerIsUsed() throws Exception {
        final var cached = new BulkByScrollResponse(TimeValue.ZERO, answer(0, 0, false).getStatus(), List.of(), List.of(), false) {
            @Override
            public org.opensearch.core.xcontent.XContentBuilder toXContent(
                org.opensearch.core.xcontent.XContentBuilder builder,
                org.opensearch.core.xcontent.ToXContent.Params params
            ) throws java.io.IOException {
                return builder.field("policy_result", "safe");
            }
        };
        withNode(PolicyFilterPlugin.cached(DeleteByQueryAction.NAME, cached), (node, metadata) -> {
            final var result = delete(node);
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"policy_result\":\"safe\""));
            assertTrue(new ShardOperations(node, metadata).get("original", "keep").document().found());
        });
    }

    public void testCachedFailureWithNoDocumentIdPreservesItsStatus() throws Exception {
        final var failure = new org.opensearch.action.bulk.BulkItemResponse.Failure(
            "hidden",
            null,
            new org.opensearch.OpenSearchStatusException("policy conflict", org.opensearch.core.rest.RestStatus.CONFLICT)
        );
        final var cached = new BulkByScrollResponse(TimeValue.ZERO, answer(0, 0, false).getStatus(), List.of(failure), List.of(), false);
        withNode(PolicyFilterPlugin.cached(DeleteByQueryAction.NAME, cached), (node, metadata) -> {
            final var result = delete(node);
            assertEquals(result.body(), 409, result.statusCode());
            assertTrue(result.body(), result.body().contains("policy conflict"));
            assertTrue(new ShardOperations(node, metadata).get("original", "keep").document().found());
        });
    }

    public void testResponseDenialPrecedesTheHttpAnswer() throws Exception {
        withNode(PolicyFilterPlugin.denyAfter(DeleteByQueryAction.NAME), (node, metadata) -> {
            final var result = delete(node);
            assertEquals(result.body(), 403, result.statusCode());
        });
    }

}
