/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.shell.ServerlessNode;

import java.io.IOException;

/**
 * Runs a search through the plugins' action filters, and refuses what a filter's rewrite asks for that this system
 * cannot honour. Split out of {@link SearchHandler}, which keeps the public entry points.
 */
final class SearchGate {

    private SearchGate() {}

    /**
     * Runs the search on every shard and merges the answers.
     *
     * <p><b>Merged by score, then cut to {@code size}.</b> This used to append each shard's hits in the
     * order the shards were visited and return all of them, so a three-shard search for {@code size=10}
     * returned up to thirty hits ordered by shard number — the first page of a relevance-ranked search
     * that was neither the first page nor relevance-ranked. Each shard is asked for {@code from + size}
     * because any one of them might own the whole page, and the global window is applied here, once.
     */
    /**
     * Runs a search through the plugins' action filters.
     *
     * @param <T> what the work produces
     * @param serving the node whose filters run
     * @param indices every index the search covers
     * @param source the search as parsed
     * @param work the search, given the source as the filters left it
     * @return what the work produced, as the filters left it
     * @throws IOException if the search fails; a filter's own refusal is a runtime exception and passes
     *     through carrying the plugin's status
     */
    public static <T> T gated(
        ServerlessNode serving,
        java.util.Collection<String> indices,
        SearchSourceBuilder source,
        org.opensearch.common.CheckedFunction<SearchSourceBuilder, T, Exception> work
    ) throws IOException {
        return gated(serving, null, indices, source, null, work);
    }

    /**
     * Runs filters over a native request while preserving its options and resolved authorization scope.
     *
     * @param serving the serving node
     * @param nativeRequest the original native request, or null for an already planned REST request
     * @param indices every resolved index the search covers
     * @param source the planned source
     * @param work the search receiving the admitted source
     * @param <T> the result type
     * @return the filtered search result
     * @throws IOException if the search fails
     */
    public static <T> T gated(
        ServerlessNode serving,
        org.opensearch.action.search.SearchRequest nativeRequest,
        java.util.Collection<String> indices,
        SearchSourceBuilder source,
        org.opensearch.common.CheckedFunction<SearchSourceBuilder, T, Exception> work
    ) throws IOException {
        return gated(serving, nativeRequest, indices, source, null, work);
    }

    /**
     * Runs an already planned search with the caller's partial-results policy visible to filters.
     *
     * @param serving the serving node
     * @param indices the resolved authorization scope
     * @param source the planned source, including any already applied search pipeline
     * @param partialResults the caller's partial-results policy, or null for the default
     * @param work the search receiving the admitted source
     * @param <T> the result type
     * @return the filtered result
     * @throws IOException if the search fails
     */
    public static <T> T gated(
        ServerlessNode serving,
        java.util.Collection<String> indices,
        SearchSourceBuilder source,
        Boolean partialResults,
        org.opensearch.common.CheckedFunction<SearchSourceBuilder, T, Exception> work
    ) throws IOException {
        return gated(serving, null, indices, source, partialResults, work);
    }

    static <T> T gated(
        ServerlessNode serving,
        org.opensearch.action.search.SearchRequest nativeRequest,
        java.util.Collection<String> indices,
        SearchSourceBuilder source,
        Boolean partialResults,
        org.opensearch.common.CheckedFunction<SearchSourceBuilder, T, Exception> work
    ) throws IOException {
        final var gate = serving.services().actionGate();
        final java.util.Set<String> resolvedScope = gate.isEmpty() ? null : java.util.Set.copyOf(indices);
        final var visible = nativeRequest == null
            ? new org.opensearch.action.search.SearchRequest(indices.toArray(new String[0]), source)
            : new org.opensearch.action.search.SearchRequest(nativeRequest).indices(indices.toArray(new String[0]))
                .source(source)
                // Core's copy constructor omits the named pipeline.
                .pipeline(nativeRequest.pipeline());
        if (nativeRequest == null && partialResults != null) {
            visible.allowPartialSearchResults(partialResults);
        }
        try {
            return gate.run(
                org.opensearch.action.search.SearchAction.NAME,
                // Every index the search covers, so a filter evaluating privileges sees all of them
                // rather than the first one named.
                visible,
                // The source as the filters left it: a document-level security query a filter folded
                // into the request reaches every shard, rather than being silently ignored.
                admitted -> {
                    if (admitted instanceof org.opensearch.action.search.SearchRequest == false) {
                        throw unsupportedFilterRewrite("a search must remain a search request");
                    }
                    final var rewritten = (org.opensearch.action.search.SearchRequest) admitted;
                    if (resolvedScope != null
                        && resolvedScope.equals(java.util.Set.copyOf(java.util.List.of(rewritten.indices()))) == false) {
                        throw unsupportedFilterRewrite("changing indices after the search scope has been resolved");
                    }
                    if (rewritten.searchType() != org.opensearch.action.search.SearchType.QUERY_THEN_FETCH
                        // The routing the caller asked for has already chosen the shards; a filter may not change it.
                        || java.util.Objects.equals(rewritten.routing(), visible.routing()) == false
                        || rewritten.scroll() != null
                        || rewritten.pipeline() != null
                    // Shards a plugin selected itself are searched as it asked; a filter may not select them.
                        || (selectsShards(rewritten.preference())
                            && java.util.Objects.equals(rewritten.preference(), visible.preference()) == false)) {
                        throw new org.opensearch.OpenSearchStatusException(
                            "the search requests unsupported execution options",
                            RestStatus.NOT_IMPLEMENTED
                        );
                    }
                    final SearchSourceBuilder accepted = rewritten.source() == null ? new SearchSourceBuilder() : rewritten.source();
                    if (nativeRequest != null && accepted.searchPipelineSource() != null) {
                        throw new org.opensearch.OpenSearchStatusException(
                            "native inline search pipelines are not supported",
                            RestStatus.NOT_IMPLEMENTED
                        );
                    }
                    final String unsupported = SearchHandler.whatCannotBeMerged(accepted);
                    if (unsupported != null || accepted.pointInTimeBuilder() != null) {
                        throw unsupportedFilterRewrite(unsupported == null ? "adding a point in time after planning" : unsupported);
                    }
                    if (rewritten.allowPartialSearchResults() == null) {
                        rewritten.allowPartialSearchResults(serving.searchService().defaultAllowPartialSearchResults());
                    }
                    final T result = work.apply(accepted);
                    if (result instanceof org.opensearch.serverless.shard.SearchOutcome outcome) {
                        if (Boolean.FALSE.equals(rewritten.allowPartialSearchResults()) && outcome.failures().isEmpty() == false) {
                            final Throwable cause = outcome.failures().get(0).getCause();
                            if (cause instanceof Exception failure) {
                                throw failure;
                            }
                            throw new IOException("search shard failed", cause);
                        }
                        if (Boolean.FALSE.equals(rewritten.allowPartialSearchResults()) && outcome.timedOut()) {
                            throw new org.opensearch.search.query.QueryPhaseExecutionException(null, "Time exceeded");
                        }
                        outcome.withTotalHitsTracking(
                            accepted.trackTotalHitsUpTo() == null
                                || accepted.trackTotalHitsUpTo() != org.opensearch.search.internal.SearchContext.TRACK_TOTAL_HITS_DISABLED
                        );
                    }
                    return result;
                },
                searchView()
            );
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    static boolean selectsShards(String preference) {
        return preference != null && preference.startsWith("_shards");
    }

    static org.opensearch.OpenSearchStatusException unsupportedFilterRewrite(String reason) {
        return new org.opensearch.OpenSearchStatusException(
            "cannot apply action-filter search rewrite: " + reason,
            RestStatus.NOT_IMPLEMENTED
        );
    }

    /**
     * How a search result is shown to a filter, and how the filter's answer is read back.
     *
     * <p><b>This is what makes field- and document-level security possible here.</b> A filter that wraps
     * the listener now receives a real {@code SearchResponse} and may hand back a different one — with
     * hits removed, or with fields redacted from their source — and that is the answer the caller gets.
     * Before this, every operation reported only that it had happened, so a filter could refuse a search
     * and could not change one.
     *
     * <p><b>Coverage is not the filter's to change, and is taken from the outcome either way.</b> How many
     * shards answered is a fact about this node's fan-out; a filter that dropped hits has not made an
     * index unreachable, and letting a rewritten response carry its own shard counts would let a redaction
     * masquerade as a partial answer.
     *
     * <p>The generic type is erased at the seam — the gate is generic over what the work returns, and only
     * a search passes this view — so the cast is checked by the one call site rather than by the compiler.
     */
    @SuppressWarnings("unchecked")
    static <T> org.opensearch.serverless.shell.ActionGate.ResponseView<T> searchView() {
        return (org.opensearch.serverless.shell.ActionGate.ResponseView<T>) SEARCH_VIEW;
    }

    static final org.opensearch.serverless.shell.ActionGate.ResponseView<org.opensearch.serverless.shard.SearchOutcome> SEARCH_VIEW =
        new org.opensearch.serverless.shell.ActionGate.ResponseView<>() {

            @Override
            public org.opensearch.core.action.ActionResponse show(org.opensearch.serverless.shard.SearchOutcome outcome) {
                return SearchHandler.toResponse(outcome, null, 0L, null);
            }

            @Override
            public org.opensearch.serverless.shard.SearchOutcome read(
                org.opensearch.core.action.ActionResponse response,
                org.opensearch.serverless.shard.SearchOutcome original
            ) {
                if (response instanceof org.opensearch.action.search.SearchResponse answered) {
                    final org.apache.lucene.search.TotalHits total = answered.getHits().getTotalHits();
                    return new org.opensearch.serverless.shard.SearchOutcome(
                        total == null ? 0L : total.value(),
                        java.util.List.of(answered.getHits().getHits()),
                        original == null ? answered.getTotalShards() : original.shards(),
                        original == null ? answered.getSuccessfulShards() : original.answered(),
                        (org.opensearch.search.aggregations.InternalAggregations) answered.getAggregations(),
                        total == null ? org.apache.lucene.search.TotalHits.Relation.EQUAL_TO : total.relation(),
                        answered.getHits().getMaxScore(),
                        original == null ? answered.isTimedOut() : original.timedOut(),
                        original == null ? answered.isTerminatedEarly() : original.terminatedEarly(),
                        original == null ? java.util.List.of(answered.getShardFailures()) : original.failures()
                    ).withSkipped(original == null ? answered.getSkippedShards() : original.skipped()).withTotalHitsTracking(total != null);
                }
                // A filter that replaced the response with something that is not a search answer.
                // Taking the original would silently undo whatever it meant to do, so this refuses
                // instead: a filter doing something the shell cannot honour must be visible.
                throw new IllegalStateException(
                    "an action filter answered a search with " + response.getClass().getName() + ", which is not a search response"
                );
            }
        };
}
