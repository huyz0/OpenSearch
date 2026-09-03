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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.transport.client.node.NodeClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    /** One search in the batch: what it names, and what it asks. */
    private record Sub(String index, SearchSourceBuilder source, String badRequest) {
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String defaultIndex = request.param("index");
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
            batch = parse(request.content().utf8ToString(), defaultIndex, serving0.searchXContentRegistry());
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

        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.SEARCH).execute(() -> {
            try {
                answer(channel, metadata, serving, batch);
            } catch (Exception e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a multi-search failure", nested);
                }
            }
        });
    }

    private void answer(
        org.opensearch.rest.RestChannel channel,
        MetadataPlane metadata,
        org.opensearch.serverless.shell.ServerlessNode serving,
        List<Sub> batch
    ) throws IOException {
        final long startNanos = System.nanoTime();
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startArray("responses");
            for (Sub sub : batch) {
                final long each = System.nanoTime();
                try {
                    if (sub.badRequest() != null) {
                        error(builder, RestStatus.BAD_REQUEST, "malformed_request", sub.badRequest());
                        continue;
                    }
                    final Map<String, Integer> indices = resolve(metadata, serving, sub.index());
                    if (indices.isEmpty()) {
                        error(builder, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + sub.index());
                        continue;
                    }
                    final var outcome = SearchFanout.run(serving, metadata, indices, sub.source());
                    SearchHandler.renderInto(
                        builder,
                        indices,
                        List.of(),
                        outcome,
                        java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - each)
                    );
                } catch (org.opensearch.serverless.metadata.DescriptorStore.TooManyMatchesException e) {
                    error(builder, RestStatus.BAD_REQUEST, "too_many_indices", e.getMessage());
                } catch (Exception e) {
                    // This search's problem, not the batch's. The slot carries the failure so a caller
                    // pairing responses to requests by position still can.
                    error(
                        builder,
                        RestStatus.INTERNAL_SERVER_ERROR,
                        "search_failed",
                        e.getMessage() == null ? e.toString() : e.getMessage()
                    );
                }
            }
            builder.endArray();
            builder.field("took", java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private static Map<String, Integer> resolve(
        MetadataPlane metadata,
        org.opensearch.serverless.shell.ServerlessNode serving,
        String requested
    ) throws IOException {
        final Map<String, Integer> indices = new LinkedHashMap<>();
        for (String name : IndexPatterns.expand(metadata, requested, serving.patternCap())) {
            final Optional<IndexDescriptor> descriptor = metadata.describe(name);
            if (descriptor.isPresent()) {
                indices.put(name, descriptor.get().numberOfShards());
            }
        }
        return indices;
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

    /** A body that is not newline-delimited pairs at all, which fails the request rather than one search. */
    private static final class BadBatch extends Exception {
        BadBatch(String message) {
            super(message);
        }
    }

    private static List<Sub> parse(String body, String defaultIndex, NamedXContentRegistry registry) throws BadBatch {
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
            final String header = lines[at++];
            String index = defaultIndex;
            try (XContentParser parser = parser(header, NamedXContentRegistry.EMPTY)) {
                final Map<String, Object> parsed = parser.map();
                if (parsed.get("index") instanceof String named) {
                    index = named;
                } else if (parsed.get("index") instanceof List<?> many && many.isEmpty() == false) {
                    index = String.join(",", many.stream().map(String::valueOf).toList());
                }
            } catch (Exception e) {
                throw new BadBatch("line " + at + ": the header must be an object naming the index");
            }
            if (at >= lines.length) {
                throw new BadBatch("line " + at + ": a header must be followed by a query body");
            }
            final String queryLine = lines[at++];
            if (index == null) {
                batch.add(new Sub(null, null, "no index given for this search and none in the path"));
                continue;
            }
            // The node's own registry, not an empty one: a query body names aggregations, queries and
            // suggesters that only a populated registry can resolve. An empty registry fails every query
            // with "named objects are not supported", which looks like a malformed request and is not.
            try (XContentParser parser = parser(queryLine, registry)) {
                final SearchSourceBuilder source = SearchSourceBuilder.fromXContent(parser, true);
                // The same defaults _search applies. Without them size stays at -1, which fetches no hits
                // while still reporting a total -- an answer that looks like "matched, but returned
                // nothing" and is really "nobody said how many to return".
                if (source.size() < 0) {
                    source.size(10);
                }
                if (source.from() < 0) {
                    source.from(0);
                }
                source.trackTotalHits(true);
                batch.add(new Sub(index, source, null));
            } catch (Exception e) {
                // The batch is well-formed; this query is not. That is this search's failure.
                batch.add(new Sub(index, null, "could not parse the search body: " + e.getMessage()));
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
