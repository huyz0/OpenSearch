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

    private static ShardOperations.SearchOutcome failedShard() {
        return new ShardOperations.SearchOutcome(
            1L,
            List.of(),
            2,
            1,
            null,
            org.apache.lucene.search.TotalHits.Relation.EQUAL_TO,
            Float.NaN,
            false,
            null,
            List.of(new org.opensearch.action.search.ShardSearchFailure(new java.io.IOException("unreachable shard")))
        );
    }

    public void testNativeSearchCanDisallowPartialResults() throws Exception {
        try (var node = node(request -> {}, null)) {
            final var request = new SearchRequest("private").allowPartialSearchResults(false);
            final var failure = expectThrows(
                Exception.class,
                () -> SearchHandler.gated(node, request, List.of("private"), new SearchSourceBuilder(), source -> failedShard())
            );
            assertNotNull(failure.toString(), org.opensearch.ExceptionsHelper.unwrap(failure, java.io.IOException.class));
            assertEquals("unreachable shard", org.opensearch.ExceptionsHelper.unwrap(failure, java.io.IOException.class).getMessage());
        }
    }

    public void testFilterCanDisallowPartialResults() throws Exception {
        try (var node = node(request -> request.allowPartialSearchResults(false), null)) {
            final var failure = expectThrows(
                Exception.class,
                () -> SearchHandler.gated(node, List.of("private"), new SearchSourceBuilder(), source -> failedShard())
            );
            assertNotNull(failure.toString(), org.opensearch.ExceptionsHelper.unwrap(failure, java.io.IOException.class));
            assertEquals("unreachable shard", org.opensearch.ExceptionsHelper.unwrap(failure, java.io.IOException.class).getMessage());
        }
    }

    public void testNativeSearchCanAllowPartialResults() throws Exception {
        try (var node = node(request -> {}, null)) {
            final var request = new SearchRequest("private").allowPartialSearchResults(true);
            final var outcome = SearchHandler.gated(node, request, List.of("private"), new SearchSourceBuilder(), source -> failedShard());
            assertEquals(1, outcome.answered());
            assertEquals(1, outcome.failures().size());
        }
    }

    public void testFilterCanRelaxNativePartialResultsPolicy() throws Exception {
        try (var node = node(request -> request.allowPartialSearchResults(true), null)) {
            final var request = new SearchRequest("private").allowPartialSearchResults(false);
            final var outcome = SearchHandler.gated(node, request, List.of("private"), new SearchSourceBuilder(), source -> failedShard());
            assertEquals(1, outcome.answered());
            assertEquals(1, outcome.failures().size());
        }
    }

    private static ShardOperations.SearchOutcome timedOutShard() {
        return new ShardOperations.SearchOutcome(
            1L,
            List.of(),
            1,
            1,
            null,
            org.apache.lucene.search.TotalHits.Relation.EQUAL_TO,
            Float.NaN,
            true,
            null,
            List.of()
        );
    }

    public void testStrictPartialPolicyRejectsTimeoutWithoutShardFailures() throws Exception {
        try (var node = node(request -> {}, null)) {
            final var request = new SearchRequest("private").allowPartialSearchResults(false);
            final var failure = expectThrows(
                org.opensearch.search.query.QueryPhaseExecutionException.class,
                () -> SearchHandler.gated(node, request, List.of("private"), new SearchSourceBuilder(), source -> timedOutShard())
            );
            assertEquals("Time exceeded", failure.getMessage());
        }
    }

    public void testAllowedPartialPolicyKeepsTimeoutWithoutShardFailures() throws Exception {
        try (var node = node(request -> {}, null)) {
            final var request = new SearchRequest("private").allowPartialSearchResults(true);
            final var outcome = SearchHandler.gated(
                node,
                request,
                List.of("private"),
                new SearchSourceBuilder(),
                source -> timedOutShard()
            );
            assertTrue(outcome.timedOut());
            assertTrue(outcome.failures().isEmpty());
        }
    }

    public void testAFilterCanRemoveTotalHitCounts() throws Exception {
        try (var node = node(request -> {}, null, SearchFilterContractTests::withoutTotals)) {
            final var source = new SearchSourceBuilder();
            final ShardOperations.SearchOutcome answer = SearchHandler.gated(
                node,
                List.of("private"),
                source,
                accepted -> new ShardOperations.SearchOutcome(42L, List.of(), 1, 1)
            );
            assertNull(
                "a response filter's omitted count must stay omitted",
                SearchHandler.toResponse(answer, source, 0L, null).getHits().getTotalHits()
            );
            assertEquals("the internal view must not restore the hidden count either", 0L, answer.total());
        }
    }

    public void testDisabledCountingIsNotShownToResponseFilters() throws Exception {
        final var exposed = new AtomicBoolean();
        try (var node = node(request -> {}, null, response -> {
            exposed.set(response.getHits().getTotalHits() != null);
            return response;
        })) {
            SearchHandler.gated(
                node,
                List.of("private"),
                new SearchSourceBuilder().trackTotalHits(false),
                accepted -> new ShardOperations.SearchOutcome(42L, List.of(), 1, 1)
            );
            assertFalse("filters must see the same counting policy the caller requested", exposed.get());
        }
    }

    public void testAnAdmittedCountingRewriteControlsTheResponse() throws Exception {
        try (var node = node(request -> request.source(new SearchSourceBuilder().trackTotalHits(true)), null)) {
            final var original = new SearchSourceBuilder().trackTotalHits(false);
            final ShardOperations.SearchOutcome answer = SearchHandler.gated(
                node,
                List.of("private"),
                original,
                accepted -> new ShardOperations.SearchOutcome(42L, List.of(), 1, 1)
            );
            final var total = SearchHandler.toResponse(answer, original, 0L, null).getHits().getTotalHits();
            assertNotNull("the rewritten request asks for totals", total);
            assertEquals(42L, total.value());
        }
    }

    public void testCachedSearchResponsesCanOmitTotalHits() throws Exception {
        final var cached = withoutTotals(SearchHandler.toResponse(new ShardOperations.SearchOutcome(42L, List.of(), 1, 1), null, 0L, null));
        try (var node = node(request -> {}, cached)) {
            final var source = new SearchSourceBuilder();
            final ShardOperations.SearchOutcome answer = SearchHandler.gated(node, List.of("private"), source, accepted -> {
                throw new AssertionError("the cache must bypass execution");
            });
            assertNull(SearchHandler.toResponse(answer, source, 0L, null).getHits().getTotalHits());
        }
    }

    public void testRedactedCountsDoNotExposeTheOldBound() throws Exception {
        try (var node = node(request -> {}, null, SearchFilterContractTests::withoutTotals)) {
            final var answer = SearchHandler.gated(
                node,
                List.of("private"),
                new SearchSourceBuilder(),
                source -> new ShardOperations.SearchOutcome(
                    42L,
                    List.of(),
                    1,
                    1,
                    null,
                    org.apache.lucene.search.TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO,
                    Float.NaN,
                    false,
                    null,
                    List.of()
                )
            );
            assertEquals(0L, answer.total());
            assertEquals(org.apache.lucene.search.TotalHits.Relation.EQUAL_TO, answer.relation());
        }
    }

    public void testCountEndpointDoesNotRestoreARedactedTotal() throws Exception {
        try (
            var store = new org.opensearch.common.blobstore.fs.FsBlobStore(1024, createTempDir(), false);
            var node = node(request -> {}, null, SearchFilterContractTests::withoutTotals);
            var client = java.net.http.HttpClient.newHttpClient()
        ) {
            final var plane = new org.opensearch.serverless.metadata.MetadataPlane(
                store,
                org.opensearch.common.blobstore.BlobPath.cleanPath(),
                () -> 1_000L,
                30_000L
            );
            plane.createIndex(new org.opensearch.serverless.cluster.IndexDescriptor("counted", "uuid-counted", 1, "{}", null));
            node.start();
            node.setMetadataPlane(plane);
            final var shard = node.activateWriter(plane, "counted", 0).orElseThrow();
            node.index(shard, "one", "{}");
            node.reconciler().shard(shard).refresh("test");
            final var address = node.boundHttpAddress().publishAddress();
            final var request = java.net.http.HttpRequest.newBuilder(
                new java.net.URI("http", null, address.getAddress(), address.getPort(), "/counted/_count", null, null)
            ).timeout(java.time.Duration.ofSeconds(30)).GET().build();
            final var response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
            assertEquals("the endpoint cannot return a count a filter removed: " + response.body(), 501, response.statusCode());
        }
    }

    private static SearchResponse withoutTotals(SearchResponse response) {
        final var hits = new org.opensearch.search.SearchHits(response.getHits().getHits(), null, response.getHits().getMaxScore());
        final var internal = new org.opensearch.search.internal.InternalSearchResponse(
            hits,
            (org.opensearch.search.aggregations.InternalAggregations) response.getAggregations(),
            null,
            null,
            response.isTimedOut(),
            response.isTerminatedEarly(),
            1
        );
        return new SearchResponse(
            internal,
            null,
            response.getTotalShards(),
            response.getSuccessfulShards(),
            response.getSkippedShards(),
            0L,
            response.getShardFailures(),
            SearchResponse.Clusters.EMPTY
        );
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
