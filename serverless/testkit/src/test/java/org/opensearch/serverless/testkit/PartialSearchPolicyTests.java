/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchAction;
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
import java.util.concurrent.atomic.AtomicReference;

public class PartialSearchPolicyTests extends OpenSearchTestCase {
    private static java.util.Map<String, Object> json(String source) {
        return org.opensearch.common.xcontent.XContentHelper.convertToMap(
            new org.opensearch.core.common.bytes.BytesArray(source),
            false,
            org.opensearch.common.xcontent.XContentType.JSON
        ).v2();
    }

    @SuppressWarnings("unchecked")
    private static java.util.Map<String, Object> object(Object value) {
        return (java.util.Map<String, Object>) value;
    }

    public static final class PolicyPlugin extends org.opensearch.plugins.Plugin implements org.opensearch.plugins.ActionPlugin {
        private final boolean policy;
        private final AtomicReference<Boolean> observed = new AtomicReference<>();

        PolicyPlugin(boolean policy) {
            this.policy = policy;
        }

        @Override
        public List<org.opensearch.action.support.ActionFilter> getActionFilters() {
            return List.of(new org.opensearch.action.support.ActionFilter() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                public <Request extends ActionRequest, Response extends org.opensearch.core.action.ActionResponse> void apply(
                    org.opensearch.tasks.Task task,
                    String action,
                    Request request,
                    org.opensearch.action.support.ActionRequestMetadata<Request, Response> metadata,
                    org.opensearch.core.action.ActionListener<Response> listener,
                    org.opensearch.action.support.ActionFilterChain<Request, Response> chain
                ) {
                    if (SearchAction.NAME.equals(action)) {
                        final var search = (SearchRequest) request;
                        observed.set(search.allowPartialSearchResults());
                        search.allowPartialSearchResults(policy);
                    }
                    chain.proceed(task, action, request, listener);
                }
            });
        }
    }

    public void testRestOriginalStrictPolicyStillFails() throws Exception {
        withFailure(new org.opensearch.plugins.Plugin() {
        }, (node, source) -> {
            final var answer = send(node, "/docs,numbers/_search?allow_partial_search_results=false", source.toString());
            assertEquals(answer.body(), 400, answer.statusCode());
        });
    }

    public void testRestFilterCanRelaxTheOriginalStrictPolicy() throws Exception {
        final var plugin = new PolicyPlugin(true);
        withFailure(plugin, (node, source) -> {
            final var answer = send(node, "/docs,numbers/_search?allow_partial_search_results=false", source.toString());
            assertEquals(answer.body(), 200, answer.statusCode());
            assertEquals(Boolean.FALSE, plugin.observed.get());
            assertEquals(1, ((Number) object(json(answer.body()).get("_shards")).get("failed")).intValue());
        });
    }

    public void testRestFilterSeesAndCanTightenTheOriginalPolicy() throws Exception {
        final var plugin = new PolicyPlugin(false);
        withFailure(plugin, (node, source) -> {
            final var answer = send(node, "/docs,numbers/_search?allow_partial_search_results=true", source.toString());
            assertEquals(answer.body(), 400, answer.statusCode());
            assertEquals(Boolean.TRUE, plugin.observed.get());
        });
    }

    public void testMsearchHonorsEachItemsPartialResultsPolicy() throws Exception {
        withFailure(new org.opensearch.plugins.Plugin() {
        }, (node, source) -> {
            final var answer = send(
                node,
                "/docs,numbers/_msearch",
                "{\"allow_partial_search_results\":false}\n" + source + "\n{\"allow_partial_search_results\":true}\n" + source + "\n"
            );
            assertEquals(answer.body(), 200, answer.statusCode());
            final var responses = (List<?>) json(answer.body()).get("responses");
            assertEquals(2, responses.size());
            assertEquals(400, ((Number) object(responses.get(0)).get("status")).intValue());
            assertEquals(200, ((Number) object(responses.get(1)).get("status")).intValue());
            assertEquals(1, ((Number) object(object(responses.get(1)).get("_shards")).get("failed")).intValue());
        });
    }

    public void testMultiTemplateHonorsTheParsedPartialResultsPolicy() throws Exception {
        withFailure(new org.opensearch.plugins.Plugin() {
        }, (node, source) -> {
            final var answer = send(
                node,
                "/docs,numbers/_msearch/template",
                "{\"allow_partial_search_results\":false}\n{\"source\":" + source + "}\n"
            );
            assertEquals(answer.body(), 200, answer.statusCode());
            final var responses = (List<?>) json(answer.body()).get("responses");
            assertEquals(400, ((Number) object(responses.get(0)).get("status")).intValue());
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
