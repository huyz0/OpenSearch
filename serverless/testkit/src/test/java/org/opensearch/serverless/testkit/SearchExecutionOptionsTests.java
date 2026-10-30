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
import org.opensearch.action.search.SearchType;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class SearchExecutionOptionsTests extends OpenSearchTestCase {
    private static SearchRequest ask() {
        return new SearchRequest("docs").source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
    }

    public void testDefaultNativeSearchStillReturnsBothShards() throws Exception {
        withNode(new org.opensearch.plugins.Plugin() {
        }, node -> { assertEquals(2L, node.client().search(ask()).actionGet().getHits().getTotalHits().value()); });
    }

    public void testRestDfsCannotSilentlyUseLocalStatistics() throws Exception {
        assertRestRefused("/docs/_search?search_type=dfs_query_then_fetch");
    }

    public void testRestShardPreferenceCannotSilentlySearchEveryShard() throws Exception {
        assertRestRefused("/docs/_search?preference=_shards:0");
    }

    private void assertRestRefused(String path) throws Exception {
        withNode(new org.opensearch.plugins.Plugin() {
        }, node -> {
            final var response = send(node, path, "{\"query\":{\"match_all\":{}}}");
            assertEquals(response.body(), 501, response.statusCode());
        });
    }

    public void testNativeDfsIsNotDroppedBeforeAdmission() throws Exception {
        assertNativeRefused(ask().searchType(SearchType.DFS_QUERY_THEN_FETCH));
    }

    public void testNativeRoutingIsNotDroppedBeforeAdmission() throws Exception {
        assertNativeRefused(ask().routing("selected-route"));
    }

    public void testNativeScrollIsNotDroppedBeforeAdmission() throws Exception {
        assertNativeRefused(ask().scroll("1m"));
    }

    public void testNativeNamedPipelineIsNotSilentlyOmitted() throws Exception {
        assertNativeRefused(ask().pipeline("required-policy"));
    }

    public void testNativeShardPreferenceIsNotDroppedBeforeAdmission() throws Exception {
        assertNativeRefused(ask().preference("_shards:0"));
    }

    public void testNativeInlinePipelineIsNotSilentlyOmitted() throws Exception {
        final var request = ask();
        request.source()
            .searchPipelineSource(
                Map.of("request_processors", List.of(Map.of("filter_query", Map.of("query", Map.of("match_none", Map.of())))))
            );
        assertNativeRefused(request);
    }

    private void assertNativeRefused(SearchRequest request) throws Exception {
        withNode(new org.opensearch.plugins.Plugin() {
        }, node -> {
            final Exception failure = expectThrows(Exception.class, () -> node.client().search(request).actionGet());
            assertEquals(failure.toString(), RestStatus.NOT_IMPLEMENTED, org.opensearch.ExceptionsHelper.status(failure));
        });
    }

    public static final class ResetOptionsPlugin extends org.opensearch.plugins.Plugin implements org.opensearch.plugins.ActionPlugin {
        private final AtomicReference<SearchRequest> observed = new AtomicReference<>();

        @Override
        public List<org.opensearch.action.support.ActionFilter> getActionFilters() {
            return List.of(new org.opensearch.action.support.ActionFilter() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                @SuppressWarnings("unchecked")
                public <Request extends ActionRequest, Response extends org.opensearch.core.action.ActionResponse> void apply(
                    org.opensearch.tasks.Task task,
                    String action,
                    Request request,
                    org.opensearch.action.support.ActionRequestMetadata<Request, Response> metadata,
                    org.opensearch.core.action.ActionListener<Response> listener,
                    org.opensearch.action.support.ActionFilterChain<Request, Response> chain
                ) {
                    if (SearchAction.NAME.equals(action)) {
                        final var actual = (SearchRequest) request;
                        observed.set(actual);
                        chain.proceed(task, action, (Request) new SearchRequest(actual.indices()).source(actual.source()), listener);
                    } else {
                        chain.proceed(task, action, request, listener);
                    }
                }
            });
        }
    }

    public void testNativeFiltersSeeTheOriginalOptionsAndCanRemoveThem() throws Exception {
        final var plugin = new ResetOptionsPlugin();
        withNode(plugin, node -> {
            final var request = ask().searchType(SearchType.DFS_QUERY_THEN_FETCH)
                .routing("selected-route")
                .pipeline("required-policy")
                .scroll("1m")
                .preference("_shards:0");
            assertEquals(2L, node.client().search(request).actionGet().getHits().getTotalHits().value());
            final var observed = plugin.observed.get();
            assertNotNull(observed);
            assertEquals(SearchType.DFS_QUERY_THEN_FETCH, observed.searchType());
            assertEquals("selected-route", observed.routing());
            assertEquals("required-policy", observed.pipeline());
            assertNotNull(observed.scroll());
            assertEquals("_shards:0", observed.preference());
        });
    }

    private void withNode(org.opensearch.plugins.Plugin plugin, org.opensearch.common.CheckedConsumer<ServerlessNode, Exception> work)
        throws Exception {
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
                work.accept(node);
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
