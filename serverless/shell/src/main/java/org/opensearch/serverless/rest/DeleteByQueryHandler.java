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
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
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
 * <p><b>Only {@code query}, and it is required.</b> No {@code script}: this endpoint deletes matches
 * rather than running update scripts. No {@code sort}, {@code size} or {@code aggs} — it chooses how it walks
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
        final boolean refresh = IndexAdminHandler.refresh(request);
        final boolean waitForCompletion = request.paramAsBoolean("wait_for_completion", true);
        final String routing = request.param("routing");
        final QueryBuilder urlQuery = org.opensearch.rest.action.RestActions.urlParamsToQueryBuilder(request);
        final String unsupportedSelection = readUnsupportedSelection(request);
        // Core's default: the first document that changed since the query was taken stops the operation.
        final String conflicts = request.param("conflicts", "abort");
        // Hints, each with one possible answer here. The operation runs inline over a frozen view and
        // deletes through the ordinary write path: there is no scroll to size, no task to throttle or
        // slice, one copy of each shard for preference to choose between, and a conflict is a document
        // changed under a delete that reads its target once -- proceed is what happens.
        for (String hint : new String[] {
            "scroll_size",
            "scroll",
            "slices",
            "requests_per_second",
            "timeout",
            "wait_for_active_shards",
            "search_timeout",
            "search_type",
            "preference",
            "request_cache",
            "ignore_unavailable",
            "allow_no_indices",
            "expand_wildcards",
            "stats",
            "version",
            "_source",
            "_source_includes",
            "_source_excludes",
            "df",
            "analyzer",
            "analyze_wildcard",
            "default_operator",
            "lenient" }) {
            request.param(hint);
        }

        if (unsupportedSelection != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_delete_by_query",
                    "'" + unsupportedSelection + "' is not supported in delete_by_query: only the query and max_docs select documents"
                )
            );
        }

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        if (waitForCompletion == false) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_delete_by_query",
                    "wait_for_completion=false asks for a task to poll, and there is no cluster-wide task registry here; "
                        + "the operation runs inline and answers when it is done"
                )
            );
        }
        if (routing != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_delete_by_query",
                    "routing is not supported: a document is placed by its id alone"
                )
            );
        }
        if ("abort".equals(conflicts) == false && "proceed".equals(conflicts) == false) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "illegal_argument_exception",
                    "conflicts may not be [" + conflicts + "], valid values are: [abort, proceed]"
                )
            );
        }
        final boolean abortOnConflict = "abort".equals(conflicts);
        final ServerlessNode serving = node.get();
        if (request.hasContent() == false && urlQuery == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "missing_query",
                    "a delete_by_query body is required, naming a 'query'; there is no implicit match_all"
                )
            );
        }

        final QueryBody body;
        if (request.hasContent() == false) {
            return channel -> run(channel, serving, metadata, index, urlQuery, maxDocs, refresh, abortOnConflict);
        }
        try (
            XContentParser parser = org.opensearch.common.xcontent.XContentType.JSON.xContent()
                .createParser(
                    serving.searchXContentRegistry(),
                    org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    request.content().streamInput()
                )
        ) {
            body = readQueryBody(parser);
        } catch (Exception e) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "bad_query", "could not parse the query: " + e.getMessage())
            );
        }
        if (body.query() == null && urlQuery == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "missing_query",
                    "a delete_by_query body needs a 'query'; there is no implicit match_all"
                )
            );
        }
        final String unsupported = body.unsupported();
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

        final QueryBuilder query = urlQuery == null ? body.query() : urlQuery;
        return channel -> run(channel, serving, metadata, index, query, maxDocs, refresh, abortOnConflict);
    }

    private void run(
        org.opensearch.rest.RestChannel channel,
        ServerlessNode serving,
        MetadataPlane metadata,
        String index,
        org.opensearch.index.query.QueryBuilder query,
        long maxDocs,
        boolean refresh,
        boolean abortOnConflict
    ) {
        serving.threadPool().executor(ThreadPool.Names.WRITE).execute(() -> {
            try {
                final var operations = new ShardOperations(serving, metadata, true);
                final var outcome = operations.deleteByQuery(index, query, maxDocs, refresh, abortOnConflict);
                respond(channel, outcome);
            } catch (ShardOperations.NoSuchIndexException e) {
                sendError(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + e.index());
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a delete_by_query failure", nested);
                }
            }
        });
    }

    private static String readUnsupportedSelection(RestRequest request) {
        String unsupported = null;
        for (String field : new String[] { "sort", "from", "size", "terminate_after", "min_score" }) {
            if (request.param(field) != null && unsupported == null) {
                unsupported = field;
            }
        }
        return unsupported;
    }

    private record QueryBody(QueryBuilder query, String unsupported) {
    }

    /** Parses exactly the supported query body without accepting and then discarding selection options. */
    private static QueryBody readQueryBody(XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IOException("expected a query object");
        }
        QueryBuilder query = null;
        String unsupported = null;
        boolean sawQuery = false;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IOException("expected a query field");
            }
            final String field = parser.currentName();
            if (parser.nextToken() == null) {
                throw new IOException("unexpected end of query body");
            }
            if ("query".equals(field)) {
                if (sawQuery) {
                    throw new IOException("duplicate query field");
                }
                sawQuery = true;
                query = org.opensearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder(parser);
            } else {
                if (unsupported == null) {
                    unsupported = field;
                }
                parser.skipChildren();
            }
        }
        if (parser.nextToken() != null) {
            throw new IOException("trailing content after query body");
        }
        return new QueryBody(query, unsupported);
    }

    private void sendError(org.opensearch.rest.RestChannel channel, RestStatus status, String type, String reason) {
        try {
            channel.sendResponse(IndexAdminHandler.error(channel, status, type, reason));
        } catch (IOException nested) {
            logger.error("failed to report a delete_by_query failure", nested);
        }
    }

    /** Renders the result after the response filters, preserving the legacy matched counter. */
    private void respond(org.opensearch.rest.RestChannel channel, ShardOperations.DeleteByQueryOutcome outcome) throws IOException {
        final var response = outcome.response();
        RestStatus status = response.isTimedOut() ? RestStatus.REQUEST_TIMEOUT : RestStatus.OK;
        for (var failure : response.getBulkFailures()) {
            if (failure.getStatus().getStatus() > status.getStatus()) {
                status = failure.getStatus();
            }
        }
        for (var failure : response.getSearchFailures()) {
            if (failure.getStatus().getStatus() > status.getStatus()) {
                status = failure.getStatus();
            }
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            response.toXContent(
                builder,
                new org.opensearch.core.xcontent.ToXContent.DelegatingMapParams(
                    java.util.Map.of("include_created", "false", "include_updated", "false"),
                    channel.request()
                )
            );
            builder.field("matched", response.getTotal());
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(status, builder));
        }
    }
}
