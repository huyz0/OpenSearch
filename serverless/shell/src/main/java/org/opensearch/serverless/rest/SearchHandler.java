/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
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
        // Every parameter read before any early return, or BaseRestHandler turns a refusal into a 400
        // about an unconsumed parameter instead of the one being made here.
        final String index = request.param("index");
        final String q = request.param("q");
        final int sizeParam = request.paramAsInt("size", 10);
        final int fromParam = request.paramAsInt("from", 0);
        final boolean hasBody = request.hasContent();

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }

        final ServerlessNode serving = node.get();
        final SearchSourceBuilder source;
        if (hasBody) {
            try (
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(
                        serving.searchXContentRegistry(),
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        request.content().streamInput()
                    )
            ) {
                source = SearchSourceBuilder.fromXContent(parser, true);
            } catch (Exception e) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.BAD_REQUEST,
                        "bad_query",
                        "could not parse the search body: " + e.getMessage()
                    )
                );
            }
        } else if (q != null && q.contains(":")) {
            // The shorthand this handler was born with, kept because it is what every existing caller
            // uses and because a one-field match is genuinely the common case.
            final String field = q.substring(0, q.indexOf(':'));
            final String value = q.substring(q.indexOf(':') + 1);
            source = new SearchSourceBuilder().query(org.opensearch.index.query.QueryBuilders.matchQuery(field, value));
        } else {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "bad_query",
                    "send a search body, or use the q=field:value shorthand"
                )
            );
        }

        // Defaults that belong to the request rather than to the shard, applied only where the body did
        // not speak. A body's own size wins; the query parameter is the fallback it always was.
        if (source.size() < 0) {
            source.size(sizeParam);
        }
        if (source.from() < 0) {
            source.from(fromParam);
        }
        source.trackTotalHits(true);

        final String unsupported = whatCannotBeMerged(source);
        if (unsupported != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_IMPLEMENTED, "unsupported_search", unsupported)
            );
        }

        final Optional<IndexDescriptor> descriptor = metadata.describe(index);
        if (descriptor.isEmpty()) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index)
            );
        }
        final int shards = descriptor.get().numberOfShards();

        // Membership is the address book and the placement input, so refresh once per search rather
        // than per shard.
        try {
            metadata.membership().refresh();
        } catch (Exception e) {
            logger.warn("could not refresh membership before searching", e);
        }

        // Off the HTTP thread. executeQueryPhase hands work to the search pool and this waits for it;
        // waiting on the transport thread that is meant to be reading the next request resets the
        // connection, which surfaces to the client as RST_STREAM rather than as anything diagnosable.
        return channel -> serving.threadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                respond(channel, serving, metadata, index, source, shards);
            } catch (Exception e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a search failure", nested);
                }
            }
        });
    }

    /**
     * Names the part of a search this cannot answer correctly across shards, or null if it can.
     *
     * <p><b>Refused rather than ignored, and the distinction is the whole of D2.</b> Every feature below
     * parses happily and runs happily on a single shard; what none of them survives is being run on
     * several shards and having the answers concatenated. An aggregation needs a reduce step this does
     * not have — returning one shard's buckets as if they were the index's would be a number that is
     * wrong and looks right, which is the worst thing this system could hand anybody. A sort needs the
     * merge to compare sort values rather than scores. {@code search_after} and {@code collapse} need
     * coordination across shards that does not exist here.
     *
     * <p>Cheap to lift later, one at a time, and each will need its own merge and its own test. Until
     * then the honest answer to "can you do this" is no.
     *
     * @param source the parsed search
     * @return why it cannot be served, or null
     */
    private static String whatCannotBeMerged(SearchSourceBuilder source) {
        if (source.aggregations() != null) {
            return "aggregations are not supported: results from several shards would have to be reduced, and this does not reduce them";
        }
        if (source.searchAfter() != null) {
            return "search_after is not supported";
        }
        if (source.collapse() != null) {
            return "collapse is not supported: it would have to be applied across shards, not within one";
        }
        if (source.suggest() != null) {
            return "suggest is not supported";
        }
        if (source.profile()) {
            return "profile is not supported";
        }
        return null;
    }

    /**
     * Runs the search on every shard and merges the answers.
     *
     * <p><b>Merged by score, then cut to {@code size}.</b> This used to append each shard's hits in the
     * order the shards were visited and return all of them, so a three-shard search for {@code size=10}
     * returned up to thirty hits ordered by shard number — the first page of a relevance-ranked search
     * that was neither the first page nor relevance-ranked. Each shard is asked for {@code from + size}
     * because any one of them might own the whole page, and the global window is applied here, once.
     */
    /**
     * Runs a search through the plugins' action filters.
     *
     * @throws IOException if the search fails; a filter's own refusal is a runtime exception and passes
     *     through carrying the plugin's status
     */
    private static <T> T gated(
        ServerlessNode serving,
        String index,
        SearchSourceBuilder source,
        org.opensearch.common.CheckedSupplier<T, Exception> work
    ) throws IOException {
        try {
            return serving.actionGate()
                .run(
                    org.opensearch.action.search.SearchAction.NAME,
                    new org.opensearch.action.search.SearchRequest(new String[] { index }, source),
                    work
                );
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void respond(
        org.opensearch.rest.RestChannel channel,
        ServerlessNode serving,
        MetadataPlane metadata,
        String index,
        SearchSourceBuilder source,
        int shards
    ) throws IOException {
        // Filtered here for the same reason DocumentHandler is: this handler formats over the shared
        // fan-out rather than going through ShardOperations, so the gate has to meet it where it works.
        final var outcome = gated(serving, index, source, () -> SearchFanout.run(serving, metadata, index, shards, source));
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startObject("_shards");
            builder.field("total", outcome.shards());
            builder.field("searched", outcome.answered());
            builder.field("unreachable", outcome.shards() - outcome.answered());
            builder.endObject();
            // Stated, not implied. A caller reading only "total" would otherwise have no way to tell a
            // complete answer from one computed over a fraction of the index.
            builder.field("complete", outcome.complete());
            builder.startObject("hits");
            builder.startObject("total");
            builder.field("value", outcome.total());
            builder.endObject();
            builder.startArray("hits");
            for (SearchHit hit : outcome.hits()) {
                builder.startObject();
                builder.field("_index", index);
                builder.field("_id", hit.getId());
                if (Float.isNaN(hit.getScore()) == false) {
                    builder.field("_score", hit.getScore());
                }
                final String hitSource = hit.getSourceAsString();
                if (hitSource != null && hitSource.isEmpty() == false) {
                    // Raw, so a document comes back as an object. It used to be written with
                    // builder.field(String, String), which returned the whole source as one escaped
                    // string that every client had to unescape before it could read a document.
                    builder.rawField(
                        "_source",
                        new java.io.ByteArrayInputStream(hitSource.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                        XContentType.JSON
                    );
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
