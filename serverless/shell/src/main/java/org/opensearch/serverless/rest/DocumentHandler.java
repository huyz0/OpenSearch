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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@code PUT|POST /{index}/_doc/{id}} — the durable write path, reachable by a user.
 *
 * <p>Everything M10 built sat behind a Java method until this existed. A write here appends to the
 * write-ahead log before it is applied, so it survives the writer dying before the next publication.
 *
 * <p><b>A node that does not own the shard refuses and names the owner</b> rather than forwarding.
 * That is the same shape as a lost activation race: losing is a routing instruction, not an error, and
 * telling the caller who won is what stops it guessing. Forwarding over transport is the remaining half
 * of this milestone; until it exists, a client must be able to find the right node, so the response
 * carries it.
 */
public final class DocumentHandler extends BaseRestHandler {

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;

    /**
     * Creates the handler.
     *
     * @param node supplies the node serving the request
     * @param plane supplies the metadata plane
     */
    public DocumentHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
    }

    @Override
    public String getName() {
        return "serverless_document_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.PUT, "/{index}/_doc/{id}"),
            new Route(RestRequest.Method.POST, "/{index}/_doc/{id}"),
            // A deletion routes, forwards and logs exactly like a write, so it belongs on the same
            // handler rather than in a parallel one that would have to be kept in step with it.
            new Route(RestRequest.Method.DELETE, "/{index}/_doc/{id}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Read every parameter before any early return: BaseRestHandler rejects a request whose
        // parameters were not all consumed, which would turn a deliberate refusal into a 400.
        final String index = request.param("index");
        final String id = request.param("id");
        final String source = request.hasContent() ? request.content().utf8ToString() : null;
        final boolean refresh = request.paramAsBoolean("refresh", false);
        final boolean deletion = request.method() == RestRequest.Method.DELETE;

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        if (deletion == false && (source == null || source.isBlank())) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "a document body is required")
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
        final ShardId shardId = serving.reconciler()
            .openShards()
            .stream()
            .filter(s -> s.getIndexName().equals(index) && s.id() == shard)
            .findFirst()
            .orElse(null);

        if (shardId == null || serving.reconciler().readerShards().contains(shardId)) {
            final var head = metadata.heads().read(index, shard);
            final String owner = head.map(h -> h.ownerNodeId()).orElse(null);
            if (owner != null && owner.equals(serving.localNode().getId())) {
                // The head names this node and this node has no open shard. That is not a routing
                // problem, it is the gap inside activation: activateWriter wins the compare-and-swap
                // before it opens the shard, so for a moment the head is true and the node is not ready.
                //
                // Forwarding here would send the write to ourselves, and the receiving side would
                // correctly refuse it -- a 500 for what is a normal, brief, self-resolving state. Say
                // "not yet" instead, which is a status a client retries.
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.SERVICE_UNAVAILABLE,
                        "activation_in_progress",
                        "this node is acquiring shard " + shard + " of " + index + "; retry"
                    )
                );
            }
            if (owner != null) {
                // Forward rather than refuse. The client should not have to know which node owns which
                // shard; that is exactly the knowledge the shard-head exists to hold.
                return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.WRITE).execute(() -> {
                    try {
                        final var peer = serving.router().peer(owner);
                        if (peer.isEmpty()) {
                            // The head names an owner that holds no live lease. That is precisely a
                            // dead writer nobody has noticed yet, and this request is the first thing
                            // in the system to prove it -- so say so rather than waiting for a timer.
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
                        // Filtered here, on the coordinating node, before the write leaves it. The owner
                        // has no idea who the caller is -- an internal transport request carries no
                        // identity -- so this is the only node where the question can be asked.
                        final var ack = gated(
                            serving,
                            deletion,
                            index,
                            id,
                            source,
                            () -> serving.router()
                                .forwardIndex(
                                    peer.get(),
                                    new org.opensearch.serverless.transport.ForwardedIndexRequest(
                                        index,
                                        shard,
                                        id,
                                        source == null ? "" : source,
                                        refresh,
                                        deletion
                                    )
                                )
                        );
                        respond(channel, index, id, shard, ack.ownerNodeId(), deletion, true);
                    } catch (Exception e) {
                        // The owner was reachable and still refused or failed. Either it lost the shard
                        // between our read and its receipt, or it is going away. Same conclusion: the
                        // head we routed on is not to be trusted.
                        serving.signals().ownershipDoubted(index, shard);
                        try {
                            // 503, not the exception's own status. A forward that fails means routing was
                            // stale, and stale routing is a retry -- rendering it as a 500 tells a client
                            // the write is hopeless when the correct answer is "ask again in a moment".
                            // The cause is carried in the message so nothing is hidden by saying so.
                            channel.sendResponse(
                                IndexAdminHandler.error(
                                    channel,
                                    RestStatus.SERVICE_UNAVAILABLE,
                                    "forward_failed",
                                    "could not forward to " + owner + ", which the shard-head named as owner: " + e.getMessage()
                                )
                            );
                        } catch (IOException nested) {
                            logger.error("failed to report a forwarding failure", nested);
                        }
                    }
                });
            }
            // No owner at all. Nothing will fix this except some node activating the shard, and the
            // only reason anyone would is that a write arrived -- which just happened.
            serving.signals().ownershipDoubted(index, shard);
            return channel -> {
                try (XContentBuilder builder = channel.newBuilder()) {
                    builder.startObject();
                    builder.field("error", "not_the_writer");
                    builder.field("index", index);
                    builder.field("shard", shard);
                    // Name the owner. A bare "wrong node" makes the client guess, and guessing at
                    // ownership is how two writers end up believing the same thing.
                    builder.field("owner_node_id", owner);
                    builder.field(
                        "reason",
                        owner == null
                            ? "no node currently owns this shard; activate it before writing"
                            : "this node does not own the shard; send the write to the named owner"
                    );
                    builder.field("status", RestStatus.MISDIRECTED_REQUEST.getStatus());
                    builder.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.MISDIRECTED_REQUEST, builder));
                }
            };
        }

        // Off the HTTP thread: a WAL append is an object-store write, and blocking the thread that
        // should be reading the next request on remote IO is how a node stops answering under load.
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.WRITE).execute(() -> {
            try {
                final boolean removed = gated(serving, deletion, index, id, source, () -> {
                    final boolean found = deletion ? serving.delete(shardId, id) : true;
                    if (deletion == false) {
                        serving.index(shardId, id, source);
                    }
                    return found;
                });
                if (refresh) {
                    // Same meaning as classic OpenSearch: make this write visible to search before
                    // answering. Without it a caller that writes and immediately searches gets zero
                    // hits and no error, which reads as data loss and is not.
                    serving.reconciler().shard(shardId).refresh("serverless-rest-refresh");
                }
                respond(channel, index, id, shard, serving.localNode().getId(), deletion, removed);
            } catch (Exception e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a write failure", nested);
                }
            }
        });
    }

    /**
     * Runs a write through the plugins' action filters.
     *
     * <p><b>Here rather than in {@code ShardOperations}, and that is worth explaining.</b> This handler
     * predates that class and still carries its own routing, forwarding and response vocabulary — a 421
     * naming the owner, a {@code durable} field, the node that actually wrote. Rewriting it onto the shared
     * operations would change what callers see, so the gate comes to it instead. What keeps that from
     * becoming a place somebody forgets is a test that walks every endpoint which reads or changes data and
     * asserts an action name reached a filter for each one.
     */
    private static <T> T gated(
        ServerlessNode serving,
        boolean deletion,
        String index,
        String id,
        String source,
        org.opensearch.common.CheckedSupplier<T, Exception> work
    ) throws Exception {
        final org.opensearch.action.ActionRequest request;
        final String action;
        if (deletion) {
            action = org.opensearch.action.delete.DeleteAction.NAME;
            request = new org.opensearch.action.delete.DeleteRequest(index, id);
        } else {
            action = org.opensearch.action.index.IndexAction.NAME;
            request = new org.opensearch.action.index.IndexRequest(index).id(id)
                .source(source == null ? "{}" : source, org.opensearch.common.xcontent.XContentType.JSON);
        }
        return serving.actionGate().run(action, request, work);
    }

    private void respond(
        org.opensearch.rest.RestChannel channel,
        String index,
        String id,
        int shard,
        String writtenBy,
        boolean deletion,
        boolean found
    ) throws IOException {
        {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("_index", index);
                builder.field("_id", id);
                builder.field("_shard", shard);
                builder.field("result", deletion ? (found ? "deleted" : "not_found") : "created");
                // Says what was actually guaranteed. "created" alone would leave a reader to assume the
                // usual meaning; here the write is in the log before this response exists, and the
                // segment it will live in may not be published yet.
                // Says what was actually guaranteed, and it matters most for a deletion: the tombstone is
                // in the log before this response exists, so a successor replaying that log removes the
                // document again rather than resurrecting it.
                builder.field("durable", "write-ahead log");
                // Which node actually holds the shard. Useful when the write was forwarded, and never
                // misleading when it was not.
                builder.field("_node", writtenBy);
                builder.endObject();
                channel.sendResponse(
                    new BytesRestResponse(deletion ? (found ? RestStatus.OK : RestStatus.NOT_FOUND) : RestStatus.CREATED, builder)
                );
            }
        }
    }
}
