/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;

/** Indexing and searching against one shard, without the coordination layer. */
public final class ShardOps {

    private ShardOps() {}

    /**
     * Indexes one document, failing loudly on a non-success result.
     *
     * @param shard the target shard
     * @param id the document id
     * @param source the document source
     * @throws IOException if indexing fails
     */
    public static void indexDoc(IndexShard shard, String id, String source) throws IOException {
        final Engine.IndexResult result = shard.applyIndexOperationOnPrimary(
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            new SourceToParse(shard.shardId().getIndexName(), id, new BytesArray(source), XContentType.JSON),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0,
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false
        );
        // s0-findings.md F5: MAPPING_UPDATE_REQUIRED is a return value, not an exception. A caller that
        // ignores it indexes nothing and reports no error.
        if (result.getResultType() != Engine.Result.Type.SUCCESS) {
            throw new AssertionError("indexing " + id + " returned " + result.getResultType() + " rather than SUCCESS");
        }
        shard.sync();
    }

    /**
     * Runs a match query against one shard and returns the hit count.
     *
     * @param searchService the node's search service
     * @param shardId the shard to query
     * @param field the field to match
     * @param text the text to match
     * @return the total hit count, which is zero when the seam is broken
     * @throws Exception if the query fails
     */
    public static long hits(SearchService searchService, ShardId shardId, String field, String text) throws Exception {
        final SearchRequest searchRequest = new SearchRequest(shardId.getIndexName()).allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().query(QueryBuilders.matchQuery(field, text)).trackTotalHits(true));
        final ShardSearchRequest request = new ShardSearchRequest(
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
        final PlainActionFuture<SearchPhaseResult> future = PlainActionFuture.newFuture();
        searchService.executeQueryPhase(
            request,
            false,
            new SearchShardTask(0, "serverless", "serverless", "serverless", null, Collections.emptyMap()),
            ActionListener.wrap(future::onResponse, future::onFailure),
            ThreadPool.Names.SEARCH,
            false
        );
        final TotalHits totalHits = future.actionGet().queryResult().topDocs().topDocs.totalHits;
        return totalHits.value();
    }
}
