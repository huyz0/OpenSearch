/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.core.common.Strings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.ShardFetchRequest;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Runs one shard's half of a search through the reused {@link SearchService}.
 *
 * <p>Shared by the local path and the forwarded one so that a shard queried over the network and a
 * shard queried in-process cannot answer differently. Two implementations of "search one shard" is the
 * kind of duplication that stays consistent right up until it matters.
 */
public final class ShardQuery {

    private ShardQuery() {}

    /** The longest one shard's query or fetch may take before the search fails rather than hangs. */
    private static final org.opensearch.common.unit.TimeValue WAIT = org.opensearch.common.unit.TimeValue.timeValueMinutes(5);

    /** One shard's answer. */
    public static final class Result {

        private final long total;
        private final List<SearchHit> hits;
        private final org.opensearch.search.aggregations.InternalAggregations aggregations;
        private final org.apache.lucene.search.TotalHits.Relation relation;
        private final float maxScore;
        private final boolean timedOut;
        private final Boolean terminatedEarly;

        Result(long total, List<SearchHit> hits) {
            this(total, hits, null);
        }

        Result(long total, List<SearchHit> hits, org.opensearch.search.aggregations.InternalAggregations aggregations) {
            this(total, hits, aggregations, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO, Float.NaN, false, null);
        }

        /**
         * Creates a result carrying everything the query phase reported about itself.
         *
         * <p>These four used to be dropped on the floor between the shard and the caller: a shard that
         * timed out, or stopped early because the caller asked it to, or stopped counting past a ceiling,
         * said so in its {@code QuerySearchResult} and nothing read it. The response then claimed
         * {@code timed_out: false} and {@code relation: eq} as constants, which were true only because the
         * information that could make them false was thrown away here.
         *
         * @param total how many matched, or a lower bound when counting stopped early
         * @param hits the fetched hits
         * @param aggregations the unreduced aggregations, or null
         * @param relation whether {@code total} is exact or a lower bound
         * @param maxScore the best score in this shard's top docs, or NaN when unscored
         * @param timedOut whether the shard hit the search timeout
         * @param terminatedEarly whether {@code terminate_after} stopped it, or null when not asked
         */
        public Result(
            long total,
            List<SearchHit> hits,
            org.opensearch.search.aggregations.InternalAggregations aggregations,
            org.apache.lucene.search.TotalHits.Relation relation,
            float maxScore,
            boolean timedOut,
            Boolean terminatedEarly
        ) {
            this.total = total;
            this.hits = hits;
            this.aggregations = aggregations;
            this.relation = relation;
            this.maxScore = maxScore;
            this.timedOut = timedOut;
            this.terminatedEarly = terminatedEarly;
        }

        /**
         * Returns whether {@link #total()} is exact or a lower bound.
         *
         * @return the relation
         */
        public org.apache.lucene.search.TotalHits.Relation relation() {
            return relation;
        }

        /**
         * Returns the best score among this shard's top docs.
         *
         * @return the score, or NaN when the query was not scored
         */
        public float maxScore() {
            return maxScore;
        }

        /**
         * Returns whether the shard ran out of time.
         *
         * @return true if the search timeout was hit
         */
        public boolean timedOut() {
            return timedOut;
        }

        /**
         * Returns whether {@code terminate_after} stopped the shard.
         *
         * @return true or false when it was asked for, null when it was not
         */
        public Boolean terminatedEarly() {
            return terminatedEarly;
        }

        /**
         * Returns this shard's unreduced aggregations, or null if the request asked for none.
         *
         * <p>Unreduced is the point: a shard's terms aggregation holds that shard's counts, and adding two
         * of them together is the coordinating node's job. Returning them already combined would be
         * returning an answer computed over a third of the index.
         *
         * @return the aggregations, or null
         */
        public org.opensearch.search.aggregations.InternalAggregations aggregations() {
            return aggregations;
        }

        /**
         * Returns how many documents matched in this shard.
         *
         * @return the total
         */
        public long total() {
            return total;
        }

        /**
         * Returns the returned hits.
         *
         * @return the hits
         */
        public List<SearchHit> hits() {
            return hits;
        }
    }

    /**
     * Queries one shard and fetches its hits.
     *
     * @param searchService the node's search service
     * @param shardId the shard to query
     * @param field the field to match
     * @param value the text to match
     * @param size the maximum hits to return
     * @return the shard's total and hits
     * @throws IOException if the query fails
     */
    public static Result execute(SearchService searchService, ShardId shardId, String field, String value, int size) throws IOException {
        return execute(
            searchService,
            shardId,
            new SearchSourceBuilder().query(QueryBuilders.matchQuery(field, value)).size(size).trackTotalHits(true).fetchSource(true)
        );
    }

    /**
     * Puts each hit's score back on it, from the top docs it came from.
     *
     * <p>A fetched hit arrives with a score of {@code NaN}. In a full search that does not matter, because
     * {@code SearchPhaseController} copies the scores across while it merges shards; this queries a shard
     * directly and so has to do it itself. Nothing noticed until hits from several shards had to be ranked
     * against each other — before that they were concatenated in shard order, where an absent score
     * changes nothing and is invisible.
     *
     * <p>Positional, because the fetch is asked for exactly the top docs' ids, in their order.
     *
     * @param hits the fetched hits
     * @param scoreDocs the top docs they were fetched for
     * @return the same hits, scored
     */
    private static List<SearchHit> withScores(
        SearchHit[] hits,
        org.apache.lucene.search.ScoreDoc[] scoreDocs,
        org.opensearch.search.DocValueFormat[] sortFormats,
        ShardId shardId
    ) {
        for (SearchHit hit : hits) {
            // Which shard, and therefore which index, this hit came from.
            //
            // A fetched hit does not know: SearchHit's index field is transient and is set from the shard
            // target, which a full search sets during the fetch phase and this does not. It did not matter
            // while a search covered one index, because the handler could name it from the request. It does
            // now, and telling a caller that a hit from logs-b came from "logs-a,logs-b" would be a lie
            // that reads like a formatting detail.
            hit.shard(new org.opensearch.search.SearchShardTarget(null, shardId, null, OriginalIndices.NONE));
        }
        for (int i = 0; i < hits.length && i < scoreDocs.length; i++) {
            hits[i].score(scoreDocs[i].score);
            // And its sort values, when the query was sorted. Lucene hands each hit's sort keys back on the
            // FieldDoc; without copying them here the coordinating node would have nothing to merge shards
            // on but score, which for a sorted query is the wrong key entirely. They travel with the hit,
            // because SearchHit serialises them itself.
            if (scoreDocs[i] instanceof org.apache.lucene.search.FieldDoc fieldDoc && sortFormats != null) {
                hits[i].sortValues(new org.opensearch.search.SearchSortValues(fieldDoc.fields, sortFormats));
            }
        }
        return List.of(hits);
    }

    /**
     * Queries one shard with a caller-supplied source and fetches its hits.
     *
     * <p>The overload above builds a one-field match and is what the {@code ?q=} shorthand still uses.
     * This one takes whatever the client sent, which is the difference between a query language and a
     * single hard-coded query.
     *
     * <p><b>Nothing here merges anything.</b> A shard answers for itself: its {@code size} is its own, and
     * its scores are comparable with another shard's only because both were computed against the same
     * query. Combining them is the caller's job, and doing it correctly is why the caller has to ask each
     * shard for {@code from + size} rather than {@code size}.
     *
     * @param searchService the node's search service
     * @param shardId the shard to query
     * @param source the query, size and everything else the client asked for
     * @return the shard's total and hits
     * @throws IOException if the query fails
     */
    public static Result execute(SearchService searchService, ShardId shardId, SearchSourceBuilder source) throws IOException {
        return execute(searchService, shardId, source, System.currentTimeMillis());
    }

    /**
     * Runs a client's query against one shard at a moment the caller fixes.
     *
     * <p><b>One {@code now} per request, not per shard.</b> Core evaluates {@code now} once on the
     * coordinator for every shard of a request; a shard that evaluated it on its own clock at its own
     * moment answered {@code now-1s} with a different boundary than its sibling did, and a
     * {@code date_histogram} with {@code extended_bounds: now} reduced buckets computed against different
     * nows. The coordinator takes the instant and passes it here and to every forwarded shard.
     *
     * <p><b>A shard that runs out of time reports it rather than failing.</b> With partial results refused
     * at the shard, core's query phase throws on a timeout instead of setting {@code timed_out}, so the
     * flag could never travel. The caller's own {@code allow_partial_search_results} is applied where the
     * caller is -- by the coordinator, on the merged answer -- which is the only place it means anything.
     *
     * @param searchService the node's search service
     * @param shardId the shard to query
     * @param source the query, size and everything else the client asked for
     * @param nowInMillis the request's {@code now}, fixed by the coordinator
     * @return the shard's total and hits
     * @throws IOException if the query fails
     */
    public static Result execute(SearchService searchService, ShardId shardId, SearchSourceBuilder source, long nowInMillis)
        throws IOException {
        final SearchRequest searchRequest = new SearchRequest(shardId.getIndexName()).allowPartialSearchResults(true).source(source);
        final ShardSearchRequest shardRequest = new ShardSearchRequest(
            // The index this request came for, rather than OriginalIndices.NONE.
            //
            // NONE was fine for as long as the request was only ever read in this process. The shard
            // request cache serialises it to build a cache key -- which it does for a size:0 aggregation,
            // exactly the shape most aggregations have -- and writing NONE trips an assertion inside
            // OriginalIndices. Naming the index is also simply what the request is: a search of one index.
            new OriginalIndices(new String[] { shardId.getIndexName() }, org.opensearch.action.support.IndicesOptions.strictExpandOpen()),
            searchRequest,
            shardId,
            1,
            AliasFilter.EMPTY,
            1.0f,
            nowInMillis,
            null,
            Strings.EMPTY_ARRAY
        );
        final SearchShardTask task = new SearchShardTask(0, "serverless", "serverless", "serverless", null, Collections.emptyMap());

        final PlainActionFuture<SearchPhaseResult> queryFuture = PlainActionFuture.newFuture();
        searchService.executeQueryPhase(shardRequest, true, task, queryFuture, ThreadPool.Names.SEARCH, false);
        final SearchPhaseResult queryResult = queryFuture.actionGet(WAIT);
        final org.opensearch.common.lucene.search.TopDocsAndMaxScore topDocs = queryResult.queryResult().topDocs();
        final long total = topDocs.topDocs.totalHits.value();
        final org.apache.lucene.search.TotalHits.Relation relation = topDocs.topDocs.totalHits.relation();
        final float maxScore = topDocs.maxScore;
        final boolean timedOut = queryResult.queryResult().searchTimedOut();
        final Boolean terminatedEarly = queryResult.queryResult().terminatedEarly();

        // A single-shard request resolves to query-and-fetch, so the hits are already here and the
        // reader context has already been freed. Asking for it again produced "No search context found",
        // a 404 that reads like a missing document rather than like a double fetch.
        final org.apache.lucene.search.ScoreDoc[] scoreDocs = queryResult.queryResult().topDocs().topDocs.scoreDocs;
        final org.opensearch.search.DocValueFormat[] sortFormats = queryResult.queryResult().sortValueFormats();
        // Taken before anything else touches the result: consumeAggs is single-use and throws on a second
        // call, which is how it makes "who owns these" unambiguous.
        final org.opensearch.search.aggregations.InternalAggregations aggregations = queryResult.queryResult().hasAggs()
            ? queryResult.queryResult().consumeAggs().expand()
            : null;
        if (queryResult.fetchResult() != null && queryResult.fetchResult().hits() != null) {
            return new Result(
                total,
                withScores(queryResult.fetchResult().hits().getHits(), scoreDocs, sortFormats, shardId),
                aggregations,
                relation,
                maxScore,
                timedOut,
                terminatedEarly
            );
        }

        final List<Integer> docIds = new ArrayList<>();
        for (var scoreDoc : scoreDocs) {
            docIds.add(scoreDoc.doc);
        }
        if (docIds.isEmpty()) {
            searchService.freeReaderContext(queryResult.getContextId());
            // No hits does not mean no aggregations: a terms aggregation over an index with size 0 is the
            // ordinary way to ask for one.
            return new Result(total, List.of(), aggregations, relation, maxScore, timedOut, terminatedEarly);
        }
        try {
            final PlainActionFuture<FetchSearchResult> fetchFuture = PlainActionFuture.newFuture();
            searchService.executeFetchPhase(new ShardFetchRequest(queryResult.getContextId(), docIds, null), task, fetchFuture);
            return new Result(
                total,
                withScores(fetchFuture.actionGet(WAIT).hits().getHits(), scoreDocs, sortFormats, shardId),
                aggregations,
                relation,
                maxScore,
                timedOut,
                terminatedEarly
            );
        } finally {
            // The reader is pinned until this runs; leaking one keeps a commit's files alive forever.
            searchService.freeReaderContext(queryResult.getContextId());
        }
    }
}
