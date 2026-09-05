/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.opensearch.common.lease.Releasables;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchService;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;

/**
 * Explains one document's score against one query, on one shard.
 *
 * <p><b>Why this is servable here at all.</b> Every classic endpoint this shell refused was refused for
 * needing something a cluster provides. Explain does not: core's {@code TransportExplainAction} puts all of
 * its cluster work in {@code resolveRequest} and {@code shards}, which decide <em>which</em> shard answers —
 * the one job this shell already does for itself, from the shard-head. What is left, {@code shardOperation},
 * touches a search context, one term lookup and Lucene's own {@code explain}. That is copied here rather than
 * reinterpreted.
 *
 * <p><b>And why it does not cost what a search costs.</b> A search contacts every shard of an index because
 * any of them might hold a hit. An explain names a document, and a named document lives on exactly one shard,
 * so this is a single-shard operation with no fan-out and no merge — strictly cheaper than the search whose
 * scoring it is explaining. Nothing here enumerates anything.
 *
 * <p>Shared by the local path and the forwarded one for the reason {@link ShardQuery} is: two implementations
 * of "explain on one shard" would stay consistent right up until they mattered.
 */
public final class ShardExplain {

    private ShardExplain() {}

    /**
     * What explaining found.
     *
     * @param exists whether the document is in this shard at all
     * @param explanation Lucene's account of the score, or null when the document does not exist
     */
    public record Outcome(boolean exists, Explanation explanation) {
    }

    /**
     * Explains a document against a query on one shard.
     *
     * <p>A document that exists but does not match is <em>not</em> a failure and not an absence: Lucene
     * returns a non-matching explanation saying which clause failed, and that is the more useful half of what
     * this endpoint is for. Collapsing it into "not found" would answer the easy question and drop the hard
     * one.
     *
     * @param searchService the node's search service
     * @param shardId the shard the document belongs to
     * @param id the document id
     * @param query the query to score it against
     * @return whether it exists and how it scored
     * @throws IOException if the shard cannot be read
     */
    public static Outcome execute(SearchService searchService, ShardId shardId, String id, QueryBuilder query) throws IOException {
        final ShardSearchRequest request = new ShardSearchRequest(shardId, System.currentTimeMillis(), AliasFilter.EMPTY);
        final SearchContext context = searchService.createSearchContext(request, SearchService.NO_TIMEOUT);
        Engine.GetResult result = null;
        try {
            final Term uid = new Term(IdFieldMapper.NAME, Uid.encodeId(id));
            // realtime false, matching core: the explanation has to come from a searcher, and a document
            // visible only in the translog has no Lucene document to explain.
            result = context.indexShard().get(new Engine.Get(false, false, id, uid));
            if (result.exists() == false) {
                return new Outcome(false, null);
            }
            context.parsedQuery(context.getQueryShardContext().toQuery(query));
            context.preProcess(true);
            final int topLevelDocId = result.docIdAndVersion().docId + result.docIdAndVersion().docBase;
            return new Outcome(true, context.searcher().explain(context.query(), topLevelDocId));
        } finally {
            Releasables.close(result, context);
        }
    }
}
