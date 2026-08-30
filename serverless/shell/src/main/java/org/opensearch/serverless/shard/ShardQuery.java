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

    /** One shard's answer. */
    public static final class Result {

        private final long total;
        private final List<SearchHit> hits;

        Result(long total, List<SearchHit> hits) {
            this.total = total;
            this.hits = hits;
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
    private static List<SearchHit> withScores(SearchHit[] hits, org.apache.lucene.search.ScoreDoc[] scoreDocs) {
        for (int i = 0; i < hits.length && i < scoreDocs.length; i++) {
            hits[i].score(scoreDocs[i].score);
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
        final SearchRequest searchRequest = new SearchRequest(shardId.getIndexName()).allowPartialSearchResults(false).source(source);
        final ShardSearchRequest shardRequest = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            shardId,
            1,
            AliasFilter.EMPTY,
            1.0f,
            System.currentTimeMillis(),
            null,
            Strings.EMPTY_ARRAY
        );
        final SearchShardTask task = new SearchShardTask(0, "serverless", "serverless", "serverless", null, Collections.emptyMap());

        final PlainActionFuture<SearchPhaseResult> queryFuture = PlainActionFuture.newFuture();
        searchService.executeQueryPhase(shardRequest, true, task, queryFuture, ThreadPool.Names.SEARCH, false);
        final SearchPhaseResult queryResult = queryFuture.actionGet();
        final long total = queryResult.queryResult().topDocs().topDocs.totalHits.value();

        // A single-shard request resolves to query-and-fetch, so the hits are already here and the
        // reader context has already been freed. Asking for it again produced "No search context found",
        // a 404 that reads like a missing document rather than like a double fetch.
        final org.apache.lucene.search.ScoreDoc[] scoreDocs = queryResult.queryResult().topDocs().topDocs.scoreDocs;
        if (queryResult.fetchResult() != null && queryResult.fetchResult().hits() != null) {
            return new Result(total, withScores(queryResult.fetchResult().hits().getHits(), scoreDocs));
        }

        final List<Integer> docIds = new ArrayList<>();
        for (var scoreDoc : scoreDocs) {
            docIds.add(scoreDoc.doc);
        }
        if (docIds.isEmpty()) {
            searchService.freeReaderContext(queryResult.getContextId());
            return new Result(total, List.of());
        }
        try {
            final PlainActionFuture<FetchSearchResult> fetchFuture = PlainActionFuture.newFuture();
            searchService.executeFetchPhase(new ShardFetchRequest(queryResult.getContextId(), docIds, null), task, fetchFuture);
            return new Result(total, withScores(fetchFuture.actionGet().hits().getHits(), scoreDocs));
        } finally {
            // The reader is pinned until this runs; leaking one keeps a commit's files alive forever.
            searchService.freeReaderContext(queryResult.getContextId());
        }
    }
}
