/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.index.shard.ShardId;
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
import java.util.ArrayList;
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
        if (source.sorts() != null && source.sorts().isEmpty() == false) {
            return "sort is not supported: hits from several shards are merged by score, and a custom sort would need its own merge";
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
    private void respond(
        org.opensearch.rest.RestChannel channel,
        ServerlessNode serving,
        MetadataPlane metadata,
        String index,
        SearchSourceBuilder source,
        int shards
    ) throws IOException {
        final int from = Math.max(0, source.from());
        final int size = Math.max(0, source.size());
        // What each shard is asked for. A shard cannot know how its hits rank against another's, so it
        // has to offer enough to cover the whole window on its own.
        final SearchSourceBuilder perShard = source.shallowCopy();
        perShard.from(0);
        perShard.size(from + size);

        // Every shard at once, up to a bound. This loop used to run the shards one after another, so a
        // query's latency was the sum of its shards rather than the slowest of them -- and the shape of
        // the loop was the only reason. Each task answers for exactly one shard and swallows nothing: a
        // shard that cannot be reached comes back null and shows up in the coverage this already reports.
        final List<java.util.concurrent.Callable<ShardAnswer>> tasks = new ArrayList<>(shards);
        for (int shard = 0; shard < shards; shard++) {
            final int number = shard;
            tasks.add(() -> askOneShard(serving, metadata, index, number, perShard));
        }
        final List<ShardAnswer> answers;
        try {
            answers = Fanout.run(serving.threadPool().executor(ThreadPool.Names.GENERIC), Fanout.DEFAULT_CONCURRENCY, tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while searching " + index, e);
        }

        long total = 0;
        int answered = 0;
        final List<SearchHit> merged = new ArrayList<>();
        for (ShardAnswer answer : answers) {
            if (answer == null) {
                continue;
            }
            total += answer.total;
            merged.addAll(answer.hits);
            answered++;
        }

        merged.sort((a, b) -> Float.compare(score(b), score(a)));
        final List<SearchHit> page = merged.stream().skip(from).limit(size).collect(java.util.stream.Collectors.toList());

        final long hitTotal = total;
        final int searched = answered;
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
            for (SearchHit hit : page) {
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

    private static float score(SearchHit hit) {
        return Float.isNaN(hit.getScore()) ? Float.NEGATIVE_INFINITY : hit.getScore();
    }

    /** One shard's contribution: what it matched, and what it returned. */
    private static final class ShardAnswer {
        private final long total;
        private final List<SearchHit> hits;

        ShardAnswer(long total, List<SearchHit> hits) {
            this.total = total;
            this.hits = hits;
        }
    }

    private static final org.apache.logging.log4j.Logger LOG = org.apache.logging.log4j.LogManager.getLogger(SearchHandler.class);

    /**
     * Answers for exactly one shard, wherever it happens to live.
     *
     * <p>Lifted out of the fan-out loop unchanged in behaviour: serve it here if it is open here,
     * otherwise pick a reader by placement, otherwise fall back to the shard's owner.
     *
     * @param serving the node running the search
     * @param metadata the metadata plane
     * @param index the index
     * @param shard the shard number
     * @param perShard the query, sized to cover the whole window on its own
     * @return what the shard answered, or null if no candidate could answer for it
     * @throws IOException if the shard is here and querying it fails
     */
    private static ShardAnswer askOneShard(
        ServerlessNode serving,
        MetadataPlane metadata,
        String index,
        int shard,
        SearchSourceBuilder perShard
    ) throws IOException {
        final ShardId local = localShard(serving, index, shard);
        if (local != null) {
            final var result = org.opensearch.serverless.shard.ShardQuery.execute(serving.searchService(), local, perShard);
            return new ShardAnswer(result.total(), result.hits());
        }
        // Not here. Choose a reader by placement -- NOT the shard's owner, which is the writer: routing
        // searches to writers would couple search capacity to write capacity and make per-index search
        // scale-to-zero meaningless. Placement is a cache-affinity hint, so the owner remains a last
        // resort for the case where no search node exists at all.
        final List<String> targets = new ArrayList<>(
            org.opensearch.serverless.cluster.ReaderPlacement.candidatesFor(
                index,
                shard,
                metadata.membership().current(),
                ServerlessNode.ROLE_SEARCH,
                2
            )
        );
        metadata.heads().read(index, shard).map(h -> h.ownerNodeId()).ifPresent(owner -> {
            if (owner != null && targets.contains(owner) == false) {
                targets.add(owner);
            }
        });

        for (String target : targets) {
            try {
                if (serving.localNode().getId().equals(target)) {
                    // We are the placement for this shard but do not hold it yet. Open it here rather
                    // than asking ourselves over the network.
                    final ShardId opened = serving.serveAsReader(metadata, index, shard);
                    final var mine = org.opensearch.serverless.shard.ShardQuery.execute(serving.searchService(), opened, perShard);
                    return new ShardAnswer(mine.total(), mine.hits());
                }
                // The search bound, not the write bound: a peer that is merely busy should cost latency,
                // not coverage.
                final var peer = serving.router().peer(target, serving.router().searchForwardTimeout());
                if (peer.isEmpty()) {
                    continue;
                }
                final var answer = serving.router()
                    .forwardSearch(peer.get(), new org.opensearch.serverless.transport.ForwardedSearchRequest(index, shard, perShard));
                return new ShardAnswer(answer.total(), answer.hits());
            } catch (Exception e) {
                // Try the next candidate. A shard that failed to answer is not a shard with no matches,
                // so it only counts as searched if one of them succeeded.
                LOG.warn("shard " + shard + " of " + index + " was not served by " + target, e);
            }
        }
        return null;
    }

    private static ShardId localShard(ServerlessNode serving, String index, int shard) {
        return serving.reconciler()
            .openShards()
            .stream()
            .filter(s -> s.getIndexName().equals(index) && s.id() == shard)
            .findFirst()
            .orElse(null);
    }

}
