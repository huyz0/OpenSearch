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
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.transport.client.node.NodeClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@code GET /{index}/_doc/{id}} — reading back what was written.
 *
 * <p>Until this existed a document could be written, deleted and searched for, but not read by id. It is
 * the smallest gap in the surface and the most conspicuous one to anyone trying the system.
 *
 * <p><b>A get is routed to the writer, which is the opposite of how a search is routed, and the
 * difference is not an inconsistency.</b> A search fans out to readers because it answers from published
 * commits and placement is a hint a node can override by opening the shard itself. A get cannot work that
 * way: a write is acknowledged once it is in the log and applied to the engine, but it is not searchable
 * until a refresh and not in the object store until a publish. A get served from a published commit would
 * therefore fail to find a document the caller had just been told was written — data loss, as far as
 * anyone can tell from outside. So the owner answers, and it answers realtime.
 *
 * <p><b>When nobody owns the shard, a published commit is the right answer rather than a compromise.</b>
 * An index nobody is writing has no owner and no unpublished writes, so the commit <em>is</em> the current
 * state, and serving it is what lets a get work against an index that has scaled to zero — the same
 * property the search path already has.
 *
 * <p><b>When somebody owns the shard and cannot be reached, this refuses.</b> That is the case worth being
 * careful about: falling back to a published commit there would answer from a copy that is knowably behind
 * a live writer, returning a stale document, or a 404 for a document that exists, while reporting success.
 * A 503 says "ask again", which is true, instead of a wrong answer that looks right.
 */
public final class GetHandler extends BaseRestHandler {

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;

    /**
     * Creates the handler.
     *
     * @param node supplies the node serving the request
     * @param plane supplies the metadata plane
     */
    public GetHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
    }

    @Override
    public String getName() {
        return "serverless_get_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/{index}/_doc/{id}"), new Route(RestRequest.Method.HEAD, "/{index}/_doc/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Every parameter read before any early return, or BaseRestHandler turns a deliberate refusal
        // into a 400 about an unconsumed parameter.
        final String index = request.param("index");
        final String id = request.param("id");
        final boolean bodyless = request.method() == RestRequest.Method.HEAD;

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        final Optional<IndexDescriptor> descriptor = metadata.describe(index);
        if (descriptor.isEmpty()) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index)
            );
        }

        final int shard = DocumentRouting.shardFor(descriptor.get(), id);
        final ServerlessNode serving = node.get();
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GET).execute(() -> {
            try {
                answer(channel, serving, metadata, index, id, shard, bodyless);
            } catch (Exception e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a read failure", nested);
                }
            }
        });
    }

    private void answer(
        org.opensearch.rest.RestChannel channel,
        ServerlessNode serving,
        MetadataPlane metadata,
        String index,
        String id,
        int shard,
        boolean bodyless
    ) throws Exception {
        final ShardId writable = serving.reconciler()
            .openShards()
            .stream()
            .filter(s -> s.getIndexName().equals(index) && s.id() == shard)
            .filter(s -> serving.reconciler().readerShards().contains(s) == false)
            .findFirst()
            .orElse(null);
        if (writable != null) {
            respond(channel, index, shard, serving.get(writable, id), serving.localNode().getId(), true, bodyless);
            return;
        }

        final var head = metadata.heads().read(index, shard);
        final String owner = head.map(h -> h.ownerNodeId()).orElse(null);
        if (owner != null && owner.equals(serving.localNode().getId())) {
            // The head names this node and the shard is not open here as a writer: the activation window,
            // brief and self-resolving. Reading a published commit instead would be answering from a copy
            // this node is in the middle of superseding.
            channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.SERVICE_UNAVAILABLE,
                    "activation_in_progress",
                    "this node is acquiring shard " + shard + " of " + index + "; retry"
                )
            );
            return;
        }
        if (owner != null) {
            try {
                final var peer = serving.router().peer(owner);
                if (peer.isEmpty()) {
                    // A head naming an owner with no live lease is a dead writer nobody has noticed, and
                    // this read is the first thing to prove it. Say so, and do not answer from a commit
                    // that a live writer may already be ahead of.
                    serving.signals().ownershipDoubted(index, shard);
                    channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.SERVICE_UNAVAILABLE,
                            "owner_unreachable",
                            "shard " + shard + " is owned by " + owner + ", which has no reachable lease"
                        )
                    );
                    return;
                }
                final var response = serving.router()
                    .forwardGet(peer.get(), new org.opensearch.serverless.transport.ForwardedGetRequest(index, shard, id));
                respond(channel, index, shard, response.document(), response.ownerNodeId(), true, bodyless);
            } catch (Exception e) {
                // 503 rather than the exception's own status, and rather than a fallback: a forward that
                // failed means the routing was stale, which is a retry, not a reason to read a staler copy.
                serving.signals().ownershipDoubted(index, shard);
                channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.SERVICE_UNAVAILABLE,
                        "forward_failed",
                        "could not forward to " + owner + ", which the shard-head named as owner: " + e.getMessage()
                    )
                );
            }
            return;
        }

        // Nobody owns the shard, so there is no writer holding unpublished writes and the published
        // commit is the current state. Opening it here is what a search would do, for the same reason.
        final ShardId reader = serving.serveAsReader(plane.get(), index, shard);
        respond(channel, index, shard, serving.get(reader, id), serving.localNode().getId(), false, bodyless);
    }

    private void respond(
        org.opensearch.rest.RestChannel channel,
        String index,
        int shard,
        ServerlessNode.Document document,
        String servedBy,
        boolean realtime,
        boolean bodyless
    ) throws IOException {
        final RestStatus status = document.found() ? RestStatus.OK : RestStatus.NOT_FOUND;
        if (bodyless) {
            // HEAD is an existence check, and a body on it would be discarded by any correct client.
            channel.sendResponse(new BytesRestResponse(status, BytesRestResponse.TEXT_CONTENT_TYPE, ""));
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("_index", index);
            builder.field("_id", document.id());
            builder.field("_shard", shard);
            builder.field("found", document.found());
            // Which copy answered, said plainly rather than left to be inferred. A realtime answer comes
            // from the shard's owner and includes writes that are acknowledged but not yet published; a
            // non-realtime one comes from a published commit, which is the whole truth only while nobody
            // is writing. A client that cares about the difference should not have to guess.
            builder.field("realtime", realtime);
            builder.field("_node", servedBy);
            if (document.found()) {
                builder.rawField(
                    "_source",
                    new ByteArrayInputStream(document.source().getBytes(StandardCharsets.UTF_8)),
                    XContentType.JSON
                );
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(status, builder));
        }
    }
}
