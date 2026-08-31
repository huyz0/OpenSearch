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

        // Several indices, named one by one.
        //
        // A comma-separated list is resolved by looking each name up, which costs one register read per
        // name and no listing. A pattern is not, and cannot be: resolving `logs-*` means enumerating the
        // deployment's indices, which is the operation §6.3 refuses on a request path and the reason
        // RefusingIndexNameExpressionResolver exists. Refusing it here, by name, is better than a wildcard
        // that quietly matched only the indices this node happened to know about.
        final java.util.List<String> names = java.util.List.of(index.split(",", -1));
        for (String name : names) {
            if (name.isEmpty() || name.indexOf('*') >= 0 || name.indexOf('?') >= 0) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.NOT_IMPLEMENTED,
                        "unsupported_search",
                        "index patterns are not supported: matching ["
                            + name
                            + "] would mean enumerating every index in the deployment, which this system "
                            + "does not offer on a request path. Name the indices you want."
                    )
                );
            }
        }
        // Whether a name that is not there is a mistake or an expectation.
        //
        // Refusing is the default and stays the default: a search over three indices where one does not
        // exist, answering 200 and looking complete, is the confident empty answer this surface exists to
        // avoid, and a typo is far more likely than an absence somebody planned for.
        //
        // But a caller searching yesterday's and today's index, on a day that has only just started, is
        // saying something different — they know one of these may not exist yet and they mean it. That is
        // exactly what ignore_unavailable means everywhere else, and refusing to offer it forces them to
        // either guess or make two requests. It has to be asked for explicitly, which is what makes the
        // distinction real rather than a default nobody chose.
        final boolean ignoreUnavailable = request.paramAsBoolean("ignore_unavailable", false);
        final java.util.LinkedHashMap<String, Integer> indices = new java.util.LinkedHashMap<>();
        final java.util.List<String> skipped = new java.util.ArrayList<>();
        for (String name : names) {
            final Optional<IndexDescriptor> found = metadata.describe(name);
            if (found.isEmpty()) {
                if (ignoreUnavailable) {
                    skipped.add(name);
                    continue;
                }
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + name)
                );
            }
            indices.put(name, found.get().numberOfShards());
        }
        if (indices.isEmpty()) {
            // Every name was absent. Answering 200 with no hits here would be the very thing the flag is
            // not for: the caller allowed for *some* of their indices to be missing, not all of them, and
            // an empty answer over nothing at all is indistinguishable from an empty answer over
            // everything.
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_FOUND,
                    "index_not_found",
                    "none of the indices named exist: " + String.join(", ", names)
                )
            );
        }

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
                respond(channel, serving, metadata, indices, skipped, source);
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
        if (source.searchAfter() != null) {
            // The two things that make a cursor a cursor. Without a sort there is no order for "after" to
            // be after, and OpenSearch's own answer to search_after with from is the same refusal: the
            // cursor is the offset, and having both is a request that means two different things at once.
            if (source.sorts() == null || source.sorts().isEmpty()) {
                return "search_after needs a sort: without one there is no order for a cursor to be a position in";
            }
            if (source.from() > 0) {
                return "search_after and from cannot both be given: the cursor is the offset";
            }
            if (source.searchAfter().length != source.sorts().size()) {
                return "search_after must carry one value per sort key, and this carries "
                    + source.searchAfter().length
                    + " for "
                    + source.sorts().size();
            }
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
        java.util.Collection<String> indices,
        SearchSourceBuilder source,
        org.opensearch.common.CheckedSupplier<T, Exception> work
    ) throws IOException {
        try {
            return serving.actionGate()
                .run(
                    org.opensearch.action.search.SearchAction.NAME,
                    // Every index the search covers, so a filter evaluating privileges sees all of them
                    // rather than the first one named.
                    new org.opensearch.action.search.SearchRequest(indices.toArray(new String[0]), source),
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
        java.util.Map<String, Integer> indices,
        java.util.List<String> skipped,
        SearchSourceBuilder source
    ) throws IOException {
        // Filtered here for the same reason DocumentHandler is: this handler formats over the shared
        // fan-out rather than going through ShardOperations, so the gate has to meet it where it works.
        final var outcome = gated(serving, indices.keySet(), source, () -> SearchFanout.run(serving, metadata, indices, source));
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
            if (skipped.isEmpty() == false) {
                // Named, not merely counted. A caller who asked to ignore what is missing still has to be
                // able to tell which of their indices this answer does not cover -- otherwise the flag
                // turns a visible absence into an invisible one, which is worse than the refusal it
                // replaced.
                builder.startArray("skipped");
                for (String name : skipped) {
                    builder.value(name);
                }
                builder.endArray();
            }
            builder.startObject("hits");
            builder.startObject("total");
            builder.field("value", outcome.total());
            builder.endObject();
            builder.startArray("hits");
            for (SearchHit hit : outcome.hits()) {
                builder.startObject();
                // The hit's own index, not the request's: a search over several indices returns hits from
                // several, and telling a caller they all came from the first one named would be a lie that
                // reads like a formatting detail.
                builder.field("_index", hit.getIndex() == null ? String.join(",", indices.keySet()) : hit.getIndex());
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
            if (outcome.aggregations() != null) {
                // Rendered by the aggregations themselves, which is the only way the shape matches what a
                // classic node returns: every aggregation type knows its own output, and a hand-written
                // renderer would agree with them until somebody used one it had not met.
                outcome.aggregations().toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }
}
