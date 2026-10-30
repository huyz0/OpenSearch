/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.rest.SearchHandler;
import org.opensearch.serverless.shard.ShardOperations;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** A filter's admitted request and response must not be silently replaced by the original plan. */
public class SearchFilterContractTests extends OpenSearchTestCase {
    private ServerlessNode node(Consumer<SearchRequest> change, SearchResponse cached) throws Exception {
        return node(change, cached, java.util.function.UnaryOperator.identity());
    }

    private ServerlessNode node(
        Consumer<SearchRequest> change,
        SearchResponse cached,
        java.util.function.UnaryOperator<SearchResponse> rewrite
    ) throws Exception {
        return new ServerlessNode(
            Settings.builder()
                .put("node.name", "search-filter-contract")
                .put("path.home", createTempDir())
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build(),
            List.of(new FilterPlugin(change, cached, rewrite))
        );
    }

    public static final class FilterPlugin extends org.opensearch.plugins.Plugin implements org.opensearch.plugins.ActionPlugin {
        private final Consumer<SearchRequest> change;
        private final SearchResponse cached;
        private final java.util.function.UnaryOperator<SearchResponse> rewrite;

        FilterPlugin(Consumer<SearchRequest> change, SearchResponse cached, java.util.function.UnaryOperator<SearchResponse> rewrite) {
            this.change = change;
            this.cached = cached;
            this.rewrite = rewrite;
        }

        @Override
        public List<org.opensearch.action.support.ActionFilter> getActionFilters() {
            return List.of(new org.opensearch.action.support.ActionFilter() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                @SuppressWarnings("unchecked")
                public <
                    Request extends org.opensearch.action.ActionRequest,
                    Response extends org.opensearch.core.action.ActionResponse> void apply(
                        org.opensearch.tasks.Task task,
                        String action,
                        Request request,
                        org.opensearch.action.support.ActionRequestMetadata<Request, Response> metadata,
                        org.opensearch.core.action.ActionListener<Response> listener,
                        org.opensearch.action.support.ActionFilterChain<Request, Response> chain
                    ) {
                    if (request instanceof SearchRequest search) {
                        if (cached != null) {
                            listener.onResponse((Response) cached);
                            return;
                        }
                        change.accept(search);
                        chain.proceed(
                            task,
                            action,
                            request,
                            org.opensearch.core.action.ActionListener.wrap(
                                response -> listener.onResponse((Response) rewrite.apply((SearchResponse) response)),
                                listener::onFailure
                            )
                        );
                        return;
                    }
                    chain.proceed(task, action, request, listener);
                }
            });
        }
    }

    public void testChangedIndicesAreRefusedBeforeExecutingTheOldScope() throws Exception {
        assertUnsupported(request -> request.indices("public-only"));
    }

    public void testUnsupportedRewrittenSourcesAreRefused() throws Exception {
        assertUnsupported(request -> request.source().collapse(new org.opensearch.search.collapse.CollapseBuilder("tenant")));
    }

    public void testChangedSearchTypeIsNotSilentlyIgnored() throws Exception {
        assertUnsupported(request -> request.searchType(org.opensearch.action.search.SearchType.DFS_QUERY_THEN_FETCH));
    }

    private void assertUnsupported(Consumer<SearchRequest> change) throws Exception {
        try (var node = node(change, null)) {
            final var executed = new AtomicBoolean();
            final var failure = expectThrows(
                org.opensearch.OpenSearchStatusException.class,
                () -> SearchHandler.gated(node, List.of("private", "public-only"), new SearchSourceBuilder(), source -> {
                    executed.set(true);
                    return new ShardOperations.SearchOutcome(0L, List.of(), 2, 2);
                })
            );
            assertEquals(org.opensearch.core.rest.RestStatus.NOT_IMPLEMENTED, failure.status());
            assertFalse("an unsupported rewrite must not run against the original indices", executed.get());
        }
    }

    public void testCachedSearchResponseDoesNotRequireAnExecutedOriginal() throws Exception {
        final var cached = SearchHandler.toResponse(new ShardOperations.SearchOutcome(7L, List.of(), 4, 4).withSkipped(2), null, 0L, null);
        try (var node = node(request -> {}, cached)) {
            final var answer = SearchHandler.gated(node, List.of("private"), new SearchSourceBuilder(), source -> {
                fail("the cache must bypass execution");
                return new ShardOperations.SearchOutcome(0L, List.of(), 1, 1);
            });
            assertEquals(7L, answer.total());
            assertEquals(4, answer.shards());
            assertEquals(4, answer.answered());
            assertEquals(2, answer.skipped());
        }
    }

    public void testFilteredResponsesPreserveActualSkippedShards() throws Exception {
        try (var node = node(request -> {}, null)) {
            final var answer = SearchHandler.gated(
                node,
                List.of("private"),
                new SearchSourceBuilder(),
                source -> new ShardOperations.SearchOutcome(0L, List.of(), 4, 4).withSkipped(3)
            );
            assertEquals(3, answer.skipped());
        }
    }

    public void testDirectShardSearchHonorsACachedFilterResponse() throws Exception {
        final var cached = SearchHandler.toResponse(new ShardOperations.SearchOutcome(7L, List.of(), 4, 4).withSkipped(2), null, 0L, null);
        try (var node = node(request -> {}, cached)) {
            final var answer = new ShardOperations(node, null).search("private", new SearchSourceBuilder());
            assertNotNull("the native cache answer must not be discarded", answer);
            assertEquals(7L, answer.total());
            assertEquals(2, answer.skipped());
        }
    }

    public void testRedactionDoesNotEraseActualExecutionFailures() throws Exception {
        final var hidden = SearchHandler.toResponse(new ShardOperations.SearchOutcome(0L, List.of(), 99, 99), null, 0L, null);
        try (var node = node(request -> {}, null, response -> hidden)) {
            final var failures = List.of(new org.opensearch.action.search.ShardSearchFailure(new java.io.IOException("unreachable")));
            final var answer = SearchHandler.gated(
                node,
                List.of("private"),
                new SearchSourceBuilder(),
                source -> new ShardOperations.SearchOutcome(
                    1L,
                    List.of(),
                    2,
                    1,
                    null,
                    org.apache.lucene.search.TotalHits.Relation.EQUAL_TO,
                    Float.NaN,
                    true,
                    true,
                    failures
                ).withSkipped(1)
            );
            assertEquals(2, answer.shards());
            assertEquals(1, answer.answered());
            assertEquals(1, answer.skipped());
            assertEquals(failures, answer.failures());
            assertTrue(answer.timedOut());
            assertEquals(Boolean.TRUE, answer.terminatedEarly());
        }
    }

    public void testARewrittenSecurityQueryStillReachesExecution() throws Exception {
        final var query = org.opensearch.index.query.QueryBuilders.termQuery("tenant", "allowed");
        try (var node = node(request -> request.source(new SearchSourceBuilder().query(query)), null)) {
            final var answer = SearchHandler.gated(node, List.of("private"), new SearchSourceBuilder(), source -> {
                assertSame(query, source.query());
                return new ShardOperations.SearchOutcome(1L, List.of(), 1, 1);
            });
            assertEquals(1L, answer.total());
        }
    }
}
