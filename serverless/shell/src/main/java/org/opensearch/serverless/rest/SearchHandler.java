/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.core.common.Strings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.ShardFetchRequest;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * {@code GET|POST /{index}/_search} — querying the shards this node can actually reach.
 *
 * <p><b>The response always reports shard coverage, and that is the point of this class.</b> A node
 * serves the shards it holds; with no cross-node fan-out yet it may hold some of an index and not the
 * rest. Returning a hit count without saying how much of the index it came from would be the exact
 * failure {@code HANDOFF.md} records eight times — a confident answer computed over a fraction of the
 * data, indistinguishable from a complete one.
 *
 * <p>So {@code _shards.total} and {@code _shards.searched} are always present, and a partial search
 * additionally sets {@code "complete": false}. A caller that ignores those is choosing to; a caller
 * that reads them cannot be misled.
 */
public final class SearchHandler extends BaseRestHandler {

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;

    /**
     * Creates the handler.
     *
     * @param node supplies the node serving the request
     * @param plane supplies the metadata plane
     */
    public SearchHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
    }

    @Override
    public String getName() {
        return "serverless_search_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/{index}/_search"), new Route(RestRequest.Method.POST, "/{index}/_search"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String index = request.param("index");
        final String q = request.param("q");
        final int size = request.paramAsInt("size", 10);

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        if (q == null || q.contains(":") == false) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "bad_query", "q must be given as field:value")
            );
        }
        final Optional<IndexDescriptor> descriptor = metadata.describe(index);
        if (descriptor.isEmpty()) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index)
            );
        }

        final String field = q.substring(0, q.indexOf(':'));
        final String value = q.substring(q.indexOf(':') + 1);

        final ServerlessNode serving = node.get();
        final Set<ShardId> reachable = new LinkedHashSet<>();
        for (ShardId shardId : serving.reconciler().openShards()) {
            if (shardId.getIndexName().equals(index)) {
                reachable.add(shardId);
            }
        }

        final int searched = reachable.size();
        final int shards = descriptor.get().numberOfShards();

        // Off the HTTP thread. executeQueryPhase hands work to the search pool and this waits for it;
        // waiting on the transport thread that is meant to be reading the next request resets the
        // connection, which surfaces to the client as RST_STREAM rather than as anything diagnosable.
        return channel -> serving.threadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                respond(channel, serving, reachable, field, value, size, searched, shards);
            } catch (Exception e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a search failure", nested);
                }
            }
        });
    }

    private void respond(
        org.opensearch.rest.RestChannel channel,
        ServerlessNode serving,
        Set<ShardId> reachable,
        String field,
        String value,
        int size,
        int searched,
        int shards
    ) throws IOException {
        long total = 0;
        final List<SearchHit> collected = new ArrayList<>();
        for (ShardId shardId : reachable) {
            final long[] shardTotal = new long[1];
            collected.addAll(queryShard(serving.searchService(), shardId, field, value, size, shardTotal));
            total += shardTotal[0];
        }
        final long hitTotal = total;
        {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.startObject("_shards");
                builder.field("total", shards);
                builder.field("searched", searched);
                builder.field("unreachable", shards - searched);
                builder.endObject();
                // Stated, not implied. A caller reading only "total" would otherwise have no way to
                // tell a complete answer from one computed over a fraction of the index.
                builder.field("complete", searched == shards);
                builder.startObject("hits");
                builder.startObject("total");
                builder.field("value", hitTotal);
                builder.endObject();
                builder.startArray("hits");
                for (SearchHit hit : collected) {
                    builder.startObject();
                    builder.field("_id", hit.getId());
                    if (hit.getSourceAsString() != null) {
                        builder.field("_source", hit.getSourceAsString());
                    }
                    builder.endObject();
                }
                builder.endArray();
                builder.endObject();
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        }
    }

    /** Query then fetch against one shard, through the reused {@link SearchService}. */
    private List<SearchHit> queryShard(SearchService searchService, ShardId shardId, String field, String value, int size, long[] total)
        throws IOException {
        final SearchRequest searchRequest = new SearchRequest(shardId.getIndexName()).allowPartialSearchResults(false)
            .source(
                new SearchSourceBuilder().query(QueryBuilders.matchQuery(field, value)).size(size).trackTotalHits(true).fetchSource(true)
            );
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
        // keepStatesInContext, because the fetch below needs the reader the query opened.
        searchService.executeQueryPhase(shardRequest, true, task, queryFuture, ThreadPool.Names.SEARCH, false);
        final SearchPhaseResult queryResult = queryFuture.actionGet();
        total[0] = queryResult.queryResult().topDocs().topDocs.totalHits.value();

        // A single-shard request resolves to query-and-fetch, so the hits are already here and the
        // reader context has already been freed. Asking for it again produced
        // "No search context found for id [1]" -- a 404 that reads like the document is missing rather
        // than like the caller fetched twice.
        if (queryResult.fetchResult() != null && queryResult.fetchResult().hits() != null) {
            return List.of(queryResult.fetchResult().hits().getHits());
        }

        final List<Integer> docIds = new ArrayList<>();
        for (var scoreDoc : queryResult.queryResult().topDocs().topDocs.scoreDocs) {
            docIds.add(scoreDoc.doc);
        }
        if (docIds.isEmpty()) {
            searchService.freeReaderContext(queryResult.getContextId());
            return List.of();
        }
        try {
            final PlainActionFuture<FetchSearchResult> fetchFuture = PlainActionFuture.newFuture();
            searchService.executeFetchPhase(new ShardFetchRequest(queryResult.getContextId(), docIds, null), task, fetchFuture);
            return List.of(fetchFuture.actionGet().hits().getHits());
        } finally {
            // The reader is pinned until this runs; leaking one keeps a commit's files alive forever.
            searchService.freeReaderContext(queryResult.getContextId());
        }
    }
}
