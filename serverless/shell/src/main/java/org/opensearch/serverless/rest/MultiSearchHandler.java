/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.transport.client.node.NodeClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * {@code POST /_msearch} — several searches in one request.
 *
 * <p>Both compared serverless products ship it. The saving is round trips, not shard work: each search still
 * fans out to the shards it names, so a client batching ten searches pays one HTTP request instead of ten and
 * exactly the same object-store cost. That is worth being clear about, because the endpoint's name suggests a
 * bulk discount it does not give.
 *
 * <p><b>One search's failure is that search's failure.</b> A batch where the third query is malformed returns
 * results for the other nine and an error object in the third slot, with HTTP 200 — the same accounting
 * {@code _bulk} uses, and for the same reason: failing the batch would make it less useful than the loop it
 * replaces. A client that checks only the HTTP status is wrong here as it is in classic OpenSearch.
 *
 * <p><b>The response body is {@code SearchHandler}'s.</b> Each element is written by
 * {@code SearchHandler#renderInto}, so a query answered here and the same query answered by {@code _search}
 * produce the same object. A second renderer would agree with the first until somebody changed one of them.
 */
public final class MultiSearchHandler extends BaseRestHandler {

    private final Supplier<MetadataPlane> plane;
    private final Supplier<org.opensearch.serverless.shell.ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the serving node
     */
    public MultiSearchHandler(Supplier<MetadataPlane> plane, Supplier<org.opensearch.serverless.shell.ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    @Override
    public String getName() {
        return "serverless_msearch_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.POST, "/_msearch"),
            new Route(RestRequest.Method.GET, "/_msearch"),
            new Route(RestRequest.Method.POST, "/{index}/_msearch"),
            new Route(RestRequest.Method.GET, "/{index}/_msearch")
        );
    }

    /**
     * One search in the batch: what it names, what it asks, and how it is refused if it is.
     *
     * @param index the index expression, or null when none was given anywhere
     * @param source the parsed search, or null when it did not parse
     * @param ignoreUnavailable the line's own ignore_unavailable
     * @param status the refusal's status, or null when the line is runnable
     * @param type the refusal's type, or null
     * @param reason the refusal's reason, or null
     */
    record Sub(String index, SearchSourceBuilder source, boolean ignoreUnavailable, RestStatus status, String type, String reason) {
        static Sub refuse(String index, RestStatus status, String type, String reason) {
            return new Sub(index, null, false, status, type, reason);
        }

        /**
         * One runnable line, with the defaults and refusals a lone {@code _search} applies.
         *
         * @param index the index expression
         * @param source the parsed search
         * @param ignoreUnavailable the line's own ignore_unavailable
         * @return the line, refused if it must be
         */
        static Sub of(String index, SearchSourceBuilder source, boolean ignoreUnavailable) {
            SearchHandler.applyDefaults(source, false);
            final String unsupported = SearchHandler.whatCannotBeMerged(source);
            if (unsupported != null) {
                return refuse(index, RestStatus.NOT_IMPLEMENTED, "unsupported_search", unsupported);
            }
            if (source.pointInTimeBuilder() != null) {
                return refuse(
                    index,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_search",
                    "a point in time is searched through _search, not inside _msearch"
                );
            }
            return new Sub(index, source, ignoreUnavailable, null, null, null);
        }
    }

    /** One line's answer, held until every line has run so the batch's own {@code took} can be written first. */
    private record Item(org.opensearch.action.search.SearchResponse response, boolean complete, List<String> skipped, RestStatus status,
        String type, String reason) {
    }

    private static final java.util.Set<String> RESPONSE_PARAMS = java.util.Set.of(
        org.opensearch.rest.action.search.RestSearchAction.TYPED_KEYS_PARAM,
        org.opensearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM
    );

    @Override
    protected java.util.Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String defaultIndex = request.param("index");
        // Hints a classic coordinator takes and this batch cannot: the lines run one after another on one
        // node, so how many to run at once has one possible answer, and the rest describe a fan-out whose
        // width is fixed. Consumed so a client library that always sends them is not turned away.
        request.param("max_concurrent_searches");
        request.param("max_concurrent_shard_requests");
        request.param("pre_filter_shard_size");
        request.param("search_type");
        request.param("ccs_minimize_roundtrips");
        final boolean requestIgnoreUnavailable = request.paramAsBoolean("ignore_unavailable", false);
        request.param("allow_no_indices");
        request.param("expand_wildcards");
        if (request.hasContent() == false) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "missing_body",
                    "a multi-search request is newline-delimited: a header line naming the index, then the query"
                )
            );
        }

        final var serving0 = node.get();
        if (serving0 == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        final List<Sub> batch;
        try {
            batch = parse(request.content().utf8ToString(), defaultIndex, requestIgnoreUnavailable, serving0.searchXContentRegistry());
        } catch (BadBatch e) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "malformed_request", e.getMessage())
            );
        }
        if (batch.isEmpty()) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "empty_request", "the body contained no searches")
            );
        }

        final MetadataPlane metadata = plane.get();
        final var serving = node.get();
        if (metadata == null || serving == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }

        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                answer(channel, metadata, serving, batch, request);
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a multi-search failure", nested);
                }
            }
        });
    }

    void answer(
        org.opensearch.rest.RestChannel channel,
        MetadataPlane metadata,
        org.opensearch.serverless.shell.ServerlessNode serving,
        List<Sub> batch,
        org.opensearch.core.xcontent.ToXContent.Params params
    ) throws IOException {
        final long startNanos = System.nanoTime();
        final List<Item> items = new ArrayList<>();
        for (Sub sub : batch) {
            final long each = System.nanoTime();
            try {
                if (sub.status() != null) {
                    items.add(new Item(null, false, List.of(), sub.status(), sub.type(), sub.reason()));
                    continue;
                }
                // The same resolution a lone _search does, refusals included. This used to look each name
                // up and silently drop the ones it could not find, so a line naming a misspelt index
                // answered from the others and looked complete.
                final SearchHandler.Resolution resolved = SearchHandler.resolveIndices(
                    metadata,
                    serving,
                    sub.index(),
                    sub.ignoreUnavailable()
                );
                if (resolved.refused()) {
                    items.add(new Item(null, false, List.of(), resolved.status(), resolved.type(), resolved.reason()));
                    continue;
                }
                // Under the same gate a lone _search runs under, so a privilege evaluator sees every line.
                final var outcome = SearchHandler.gated(
                    serving,
                    resolved.indices().keySet(),
                    sub.source(),
                    admitted -> SearchFanout.run(serving, metadata, resolved.indices(), admitted)
                );
                items.add(
                    new Item(
                        SearchHandler.toResponse(
                            outcome,
                            sub.source(),
                            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - each),
                            null
                        ),
                        outcome.complete(),
                        resolved.skipped(),
                        null,
                        null,
                        null
                    )
                );
            } catch (org.opensearch.serverless.metadata.DescriptorStore.TooManyMatchesException e) {
                items.add(new Item(null, false, List.of(), RestStatus.BAD_REQUEST, "too_many_indices", e.getMessage()));
            } catch (Exception e) {
                // This search's problem, not the batch's. The slot carries the failure so a caller
                // pairing responses to requests by position still can.
                items.add(
                    new Item(
                        null,
                        false,
                        List.of(),
                        RestStatus.INTERNAL_SERVER_ERROR,
                        "search_failed",
                        e.getMessage() == null ? e.toString() : e.getMessage()
                    )
                );
            }
        }
        // took first, then the responses, each carrying its own status: the order and the shape core's
        // MultiSearchResponse writes, which a client's parser is built for.
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("took", java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
            builder.startArray("responses");
            for (Item item : items) {
                if (item.response() == null) {
                    error(builder, item.status(), item.type(), item.reason());
                    continue;
                }
                builder.startObject();
                SearchHandler.renderInto(builder, item.skipped(), item.response(), item.complete(), params);
                builder.field("status", item.response().status().getStatus());
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private static void error(XContentBuilder builder, RestStatus status, String type, String reason) throws IOException {
        builder.startObject();
        builder.startObject("error");
        builder.field("type", type);
        builder.field("reason", reason);
        builder.endObject();
        builder.field("status", status.getStatus());
        builder.endObject();
    }

    /**
     * The most searches one batch may carry.
     *
     * <p>Each line is a full fan-out with its own working set, run one after another on one thread; a
     * batch with no bound was a way to hold a thread and a window of hits per line for as long as the
     * caller liked. Core bounds a batch by its thread count; this bounds it by a number.
     */
    static final int MAX_SEARCHES = 100;

    /** A body that is not newline-delimited pairs at all, which fails the request rather than one search. */
    private static final class BadBatch extends Exception {
        BadBatch(String message) {
            super(message);
        }
    }

    private static List<Sub> parse(String body, String defaultIndex, boolean requestIgnoreUnavailable, NamedXContentRegistry registry)
        throws BadBatch {
        final List<Sub> batch = new ArrayList<>();
        final String[] lines = body.split("\n");
        int at = 0;
        while (at < lines.length) {
            while (at < lines.length && lines[at].isBlank()) {
                at++;
            }
            if (at >= lines.length) {
                break;
            }
            if (batch.size() >= MAX_SEARCHES) {
                throw new BadBatch("a batch may carry at most " + MAX_SEARCHES + " searches");
            }
            final String header = lines[at++];
            String index = defaultIndex;
            boolean ignoreUnavailable = requestIgnoreUnavailable;
            String refusedFor = null;
            try (XContentParser parser = parser(header, NamedXContentRegistry.EMPTY)) {
                final Map<String, Object> parsed = parser.map();
                // Both spellings core accepts. The rest of the header is read for what it is: routing
                // would change which shards are asked and is refused; preference, search_type,
                // request_cache, allow_no_indices and expand_wildcards are hints with one possible answer
                // here and are consumed without effect, the same as on a lone _search.
                final Object named = parsed.containsKey("index") ? parsed.get("index") : parsed.get("indices");
                if (named instanceof String one) {
                    index = one;
                } else if (named instanceof List<?> many && many.isEmpty() == false) {
                    index = String.join(",", many.stream().map(String::valueOf).toList());
                }
                if (parsed.get("ignore_unavailable") != null) {
                    ignoreUnavailable = Boolean.parseBoolean(String.valueOf(parsed.get("ignore_unavailable")));
                }
                if (parsed.get("routing") != null) {
                    refusedFor = "routing is not supported: a document here is placed by its id alone";
                }
            } catch (Exception e) {
                throw new BadBatch("line " + at + ": the header must be an object naming the index");
            }
            if (at >= lines.length) {
                throw new BadBatch("line " + at + ": a header must be followed by a query body");
            }
            final String queryLine = lines[at++];
            if (index == null) {
                batch.add(
                    Sub.refuse(null, RestStatus.BAD_REQUEST, "malformed_request", "no index given for this search and none in the path")
                );
                continue;
            }
            if (refusedFor != null) {
                batch.add(Sub.refuse(index, RestStatus.NOT_IMPLEMENTED, "unsupported_search", refusedFor));
                continue;
            }
            // The node's own registry, not an empty one: a query body names aggregations, queries and
            // suggesters that only a populated registry can resolve. An empty registry fails every query
            // with "named objects are not supported", which looks like a malformed request and is not.
            try (XContentParser parser = parser(queryLine, registry)) {
                // The same defaults and refusals a lone _search makes. Without them collapse, suggest and
                // profile were silently dropped inside a batch and refused outside one, and search_after
                // went unvalidated -- two answers for one query depending on which endpoint carried it.
                batch.add(Sub.of(index, SearchSourceBuilder.fromXContent(parser, true), ignoreUnavailable));
            } catch (Exception e) {
                // The batch is well-formed; this query is not. That is this search's failure.
                batch.add(
                    Sub.refuse(index, RestStatus.BAD_REQUEST, "malformed_request", "could not parse the search body: " + e.getMessage())
                );
            }
        }
        return batch;
    }

    private static XContentParser parser(String line, NamedXContentRegistry registry) throws IOException {
        return org.opensearch.common.xcontent.XContentType.JSON.xContent()
            .createParser(
                registry,
                org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                new ByteArrayInputStream(line.getBytes(StandardCharsets.UTF_8))
            );
    }
}
