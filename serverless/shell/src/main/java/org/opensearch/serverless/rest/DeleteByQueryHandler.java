/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shard.ShardOperations;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * {@code POST /{index}/_delete_by_query} — every document a query matches, removed.
 *
 * <p>Parsing and refusal only; the operation itself is {@link ShardOperations#deleteByQuery}, gated once
 * under its own action name rather than once per document — see that method's javadoc for why matching is
 * frozen and deleting is not, and why it walks one shard at a time by {@code _doc} order rather than
 * through the cross-shard {@code search_after} merge {@code _search} uses.
 *
 * <p><b>Only {@code query}, and it is required.</b> No {@code script} — no engine is registered, the same
 * as everywhere else. No {@code sort}, {@code size} or {@code aggs} — this endpoint chooses how it walks
 * a shard and does not offer a caller a window into that. No implicit {@code match_all}: a caller who
 * means to delete everything says so, explicitly, with {@code {"query":{"match_all":{}}}}.
 */
public final class DeleteByQueryHandler extends BaseRestHandler {

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;

    /**
     * Creates the handler.
     *
     * @param node supplies the node
     * @param plane supplies the metadata plane
     */
    public DeleteByQueryHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
    }

    @Override
    public String getName() {
        return "serverless_delete_by_query_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/{index}/_delete_by_query"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Every parameter and the body read before any early return, or BaseRestHandler turns a
        // deliberate refusal into a 400 about an unconsumed parameter.
        final String index = request.param("index");
        final long maxDocs = request.paramAsLong("max_docs", Long.MAX_VALUE);
        final boolean refresh = request.paramAsBoolean("refresh", false);

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        final ServerlessNode serving = node.get();
        if (request.hasContent() == false) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "missing_query",
                    "a delete_by_query body is required, naming a 'query'; there is no implicit match_all"
                )
            );
        }

        final SearchSourceBuilder source;
        try (
            XContentParser parser = org.opensearch.common.xcontent.XContentType.JSON.xContent()
                .createParser(
                    serving.searchXContentRegistry(),
                    org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    request.content().streamInput()
                )
        ) {
            source = SearchSourceBuilder.fromXContent(parser, true);
        } catch (Exception e) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "bad_query", "could not parse the query: " + e.getMessage())
            );
        }
        if (source.query() == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "missing_query",
                    "a delete_by_query body needs a 'query'; there is no implicit match_all"
                )
            );
        }
        final String unsupported = firstUnsupportedField(source);
        if (unsupported != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_delete_by_query",
                    "'" + unsupported + "' is not supported in delete_by_query: only 'query' is read"
                )
            );
        }

        final var query = source.query();
        return channel -> serving.threadPool().executor(ThreadPool.Names.WRITE).execute(() -> {
            try {
                final var operations = new ShardOperations(serving, metadata);
                final var outcome = operations.deleteByQuery(index, query, maxDocs, refresh);
                respond(channel, outcome.matched(), outcome.deleted());
            } catch (ShardOperations.NoSuchIndexException e) {
                sendError(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index);
            } catch (Exception e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a delete_by_query failure", nested);
                }
            }
        });
    }

    /** Returns the name of the first field this endpoint does not read, or null if there is none. */
    private static String firstUnsupportedField(SearchSourceBuilder source) {
        if (source.sorts() != null) {
            return "sort";
        }
        if (source.size() != -1) {
            return "size";
        }
        if (source.from() > 0) {
            return "from";
        }
        if (source.aggregations() != null) {
            return "aggs";
        }
        if (source.searchAfter() != null) {
            return "search_after";
        }
        if (source.postFilter() != null) {
            return "post_filter";
        }
        if (source.fetchSource() != null) {
            return "_source";
        }
        return null;
    }

    private void sendError(org.opensearch.rest.RestChannel channel, RestStatus status, String type, String reason) {
        try {
            channel.sendResponse(IndexAdminHandler.error(channel, status, type, reason));
        } catch (IOException nested) {
            logger.error("failed to report a delete_by_query failure", nested);
        }
    }

    private void respond(org.opensearch.rest.RestChannel channel, long matched, long deleted) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("matched", matched);
            builder.field("deleted", deleted);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }
}
