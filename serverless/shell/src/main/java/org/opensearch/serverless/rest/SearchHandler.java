/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.SearchHit;
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
                respond(channel, serving, metadata, index, field, value, size, shards);
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
        MetadataPlane metadata,
        String index,
        String field,
        String value,
        int size,
        int shards
    ) throws IOException {
        long total = 0;
        int answered = 0;
        final List<String> ids = new ArrayList<>();
        final List<String> sources = new ArrayList<>();

        for (int shard = 0; shard < shards; shard++) {
            final ShardId local = localShard(serving, index, shard);
            if (local != null) {
                final var result = org.opensearch.serverless.shard.ShardQuery.execute(serving.searchService(), local, field, value, size);
                total += result.total();
                for (SearchHit hit : result.hits()) {
                    ids.add(hit.getId());
                    sources.add(hit.getSourceAsString());
                }
                answered++;
                continue;
            }
            // Not here. Choose a reader by placement -- NOT the shard's owner, which is the writer:
            // routing searches to writers would couple search capacity to write capacity and make
            // per-index search scale-to-zero meaningless. Placement is a cache-affinity hint, so the
            // owner remains a last resort for the case where no search node exists at all.
            final List<String> targets = new ArrayList<>(
                org.opensearch.serverless.cluster.ReaderPlacement.candidatesFor(
                    index,
                    shard,
                    metadata.membership().current(),
                    ServerlessNode.ROLE_SEARCH,
                    2
                )
            );
            final var head = metadata.heads().read(index, shard);
            head.map(h -> h.ownerNodeId()).ifPresent(owner -> {
                if (targets.contains(owner) == false) {
                    targets.add(owner);
                }
            });

            boolean got = false;
            for (String target : targets) {
                try {
                    if (serving.localNode().getId().equals(target)) {
                        // We are the placement for this shard but do not hold it yet. Open it here
                        // rather than asking ourselves over the network.
                        final ShardId opened = serving.serveAsReader(metadata, index, shard);
                        final var mine = org.opensearch.serverless.shard.ShardQuery.execute(
                            serving.searchService(),
                            opened,
                            field,
                            value,
                            size
                        );
                        total += mine.total();
                        for (SearchHit hit : mine.hits()) {
                            ids.add(hit.getId());
                            sources.add(hit.getSourceAsString());
                        }
                        got = true;
                        break;
                    }
                    // The search bound, not the write bound: a peer that is merely busy should cost
                    // latency, not coverage.
                    final var peer = serving.router().peer(target, serving.router().searchForwardTimeout());
                    if (peer.isEmpty()) {
                        continue;
                    }
                    final var answer = serving.router()
                        .forwardSearch(
                            peer.get(),
                            new org.opensearch.serverless.transport.ForwardedSearchRequest(index, shard, field, value, size)
                        );
                    total += answer.total();
                    ids.addAll(answer.ids());
                    sources.addAll(answer.sources());
                    got = true;
                    break;
                } catch (Exception e) {
                    // Try the next candidate. A shard that failed to answer is not a shard with no
                    // matches, so it only counts as searched if one of them succeeded.
                    logger.warn("shard " + shard + " of " + index + " was not served by " + target, e);
                }
            }
            if (got) {
                answered++;
            }
        }

        final long hitTotal = total;
        final int searched = answered;
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
                for (int i = 0; i < ids.size(); i++) {
                    builder.startObject();
                    builder.field("_id", ids.get(i));
                    if (sources.get(i) != null && sources.get(i).isEmpty() == false) {
                        builder.field("_source", sources.get(i));
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

    private static ShardId localShard(ServerlessNode serving, String index, int shard) {
        return serving.reconciler()
            .openShards()
            .stream()
            .filter(s -> s.getIndexName().equals(index) && s.id() == shard)
            .findFirst()
            .orElse(null);
    }

}
