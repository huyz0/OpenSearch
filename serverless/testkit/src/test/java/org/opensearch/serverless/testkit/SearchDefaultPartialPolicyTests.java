/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

public class SearchDefaultPartialPolicyTests extends OpenSearchTestCase {
    private static java.util.Map<String, Object> json(String source) {
        return org.opensearch.common.xcontent.XContentHelper.convertToMap(
            new org.opensearch.core.common.bytes.BytesArray(source),
            false,
            org.opensearch.common.xcontent.XContentType.JSON
        ).v2();
    }

    public void testNativeSearchHonorsConfiguredStrictDefault() throws Exception {
        withFailure(new org.opensearch.plugins.Plugin() {
        }, (node, source) -> {
            final var request = new SearchRequest("docs", "numbers").source(source);
            assertNull(request.allowPartialSearchResults());
            final var failure = expectThrows(Exception.class, () -> node.client().search(request).actionGet());
            assertNotNull(
                failure.toString(),
                org.opensearch.ExceptionsHelper.unwrap(failure, org.opensearch.index.query.QueryShardException.class)
            );
        });
    }

    public void testRestSearchHonorsConfiguredStrictDefault() throws Exception {
        withFailure(new org.opensearch.plugins.Plugin() {
        }, (node, source) -> {
            final var answer = send(node, "/docs,numbers/_search", source.toString());
            assertEquals(answer.body(), 400, answer.statusCode());
        });
    }

    public void testBatchSearchHonorsConfiguredStrictDefault() throws Exception {
        withFailure(new org.opensearch.plugins.Plugin() {
        }, (node, source) -> {
            final var answer = send(node, "/docs,numbers/_msearch", "{}\n" + source + "\n");
            assertEquals(answer.body(), 200, answer.statusCode());
            final var replies = (List<?>) json(answer.body()).get("responses");
            final var first = (java.util.Map<?, ?>) replies.get(0);
            assertEquals(400, ((Number) first.get("status")).intValue());
        });
    }

    public void testExplicitNativePolicyOverridesStrictDefault() throws Exception {
        withFailure(new org.opensearch.plugins.Plugin() {
        }, (node, source) -> {
            final var reply = node.client()
                .search(new SearchRequest("docs", "numbers").source(source).allowPartialSearchResults(true))
                .actionGet();
            assertEquals(2, reply.getSuccessfulShards());
            assertEquals(1, reply.getFailedShards());
        });
    }

    public void testFilterCanOverrideConfiguredStrictDefault() throws Exception {
        withFailure(new PartialSearchPolicyTests.PolicyPlugin(true), (node, source) -> {
            final var answer = send(node, "/docs,numbers/_search", source.toString());
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"failed\":1"));
        });
    }

    private void withFailure(
        org.opensearch.plugins.Plugin plugin,
        org.opensearch.common.CheckedBiConsumer<ServerlessNode, SearchSourceBuilder, Exception> work
    ) throws Exception {
        withNodeAndMetadata(plugin, (node, metadata) -> {
            node.client()
                .admin()
                .indices()
                .create(
                    new org.opensearch.action.admin.indices.create.CreateIndexRequest("numbers").mapping(
                        "{\"properties\":{\"msg\":{\"type\":\"long\"}}}",
                        org.opensearch.common.xcontent.XContentType.JSON
                    )
                )
                .actionGet();
            final var numbers = node.activateWriter(metadata, "numbers", 0).orElseThrow();
            node.client()
                .index(
                    new org.opensearch.action.index.IndexRequest("numbers").id("number")
                        .source("{\"msg\":42}", org.opensearch.common.xcontent.XContentType.JSON)
                )
                .actionGet();
            node.reconciler().shard(numbers).refresh("partial-results-test");
            final var source = new SearchSourceBuilder().query(QueryBuilders.rangeQuery("msg").gte("aaa"));
            assertFalse(node.searchService().defaultAllowPartialSearchResults());
            work.accept(node, source);
        });
    }

    private void withNodeAndMetadata(
        org.opensearch.plugins.Plugin plugin,
        org.opensearch.common.CheckedBiConsumer<ServerlessNode, MetadataPlane, Exception> work
    ) throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "search-execution-options")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest,search")
                .put(org.opensearch.search.SearchService.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey(), false)
                .build();
            try (var node = new ServerlessNode(settings, List.of(plugin))) {
                node.start();
                node.setMetadataPlane(metadata);
                metadata.createIndex(new IndexDescriptor("docs", "uuid-docs", 2, "{\"properties\":{\"msg\":{\"type\":\"text\"}}}", null));
                for (int i = 0; i < 2; i++) {
                    final var shard = node.activateWriter(metadata, "docs", i).orElseThrow();
                    node.index(shard, "doc-" + i, "{\"msg\":\"visible\"}");
                    node.reconciler().shard(shard).refresh("search-options-test");
                }
                work.accept(node, metadata);
            }
        }
    }

    private static HttpResponse<String> send(ServerlessNode node, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final var request = HttpRequest.newBuilder(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
            return client.send(request, HttpResponse.BodyHandlers.ofString());
        }
    }
}
