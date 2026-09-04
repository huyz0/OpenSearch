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
        final boolean refresh = IndexAdminHandler.refresh(request);
        final boolean waitForCompletion = request.paramAsBoolean("wait_for_completion", true);
        final String routing = request.param("routing");
        final String q = request.param("q");
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
            "terminate_after",
            "size",
            "from",
            "sort",
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
        if (request.hasContent() == false && q == null) {
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
        if (request.hasContent() == false) {
            // q= alone, parsed by core's own query-string parser as it is on _search.
            final SearchSourceBuilder fromQuery = new SearchSourceBuilder().query(
                org.opensearch.index.query.QueryBuilders.queryStringQuery(q)
            );
            final var query = fromQuery.query();
            return channel -> run(channel, serving, metadata, index, query, maxDocs, refresh, abortOnConflict);
        }
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
        final long startNanos = System.nanoTime();
        serving.threadPool().executor(ThreadPool.Names.WRITE).execute(() -> {
            try {
                final var operations = new ShardOperations(serving, metadata);
                final var outcome = operations.deleteByQuery(index, query, maxDocs, refresh, abortOnConflict);
                respond(channel, index, outcome, java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
            } catch (ShardOperations.NoSuchIndexException e) {
                sendError(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index);
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
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

    /**
     * Writes the answer in core's {@code BulkByScrollResponse} shape.
     *
     * <p>Every field a classic response carries, with the value this implementation actually has for it:
     * the operation runs inline, in one pass, unthrottled, and a document changed under it is deleted
     * rather than counted as a conflict, so the counters that describe throttling and retries are the
     * zeros they truly are rather than absent. {@code matched} is this shell's older name for
     * {@code total} and stays for the callers that read it.
     */
    private void respond(
        org.opensearch.rest.RestChannel channel,
        String index,
        ShardOperations.DeleteByQueryOutcome outcome,
        long tookMillis
    ) throws IOException {
        final long matched = outcome.matched();
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("took", tookMillis);
            builder.field("timed_out", false);
            builder.field("total", matched);
            builder.field("matched", matched);
            builder.field("deleted", outcome.deleted());
            builder.field("batches", matched == 0 ? 0 : 1);
            builder.field("version_conflicts", outcome.versionConflicts());
            builder.field("noops", 0);
            builder.startObject("retries");
            builder.field("bulk", 0);
            builder.field("search", 0);
            builder.endObject();
            builder.field("throttled_millis", 0);
            builder.field("requests_per_second", -1.0f);
            builder.field("throttled_until_millis", 0);
            builder.startArray("failures");
            if (outcome.abortedOn() != null) {
                // Core's shape for the conflict that stopped the operation under conflicts=abort.
                builder.startObject();
                builder.field("index", index);
                builder.field("id", outcome.abortedOn());
                builder.startObject("cause");
                builder.field("type", "version_conflict_engine_exception");
                builder.field("reason", "[" + outcome.abortedOn() + "]: version conflict, the document changed after the query was taken");
                builder.field("index", index);
                builder.endObject();
                builder.field("status", RestStatus.CONFLICT.getStatus());
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(outcome.abortedOn() == null ? RestStatus.OK : RestStatus.CONFLICT, builder));
        }
    }
}
