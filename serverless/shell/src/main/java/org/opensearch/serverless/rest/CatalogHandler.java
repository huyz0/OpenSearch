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
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.ShardHead;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The read-only view of the metadata plane: indices, shard-heads, and live nodes.
 *
 * <p>These replace the classic APIs that {@link NotImplementedHandler} refuses. The names are different
 * on purpose. {@code _cat/shards} and {@code _cluster/state} promise a cluster-wide answer computed by
 * whoever is asked; these promise a read of the object store, which is a different and weaker claim, and
 * pretending otherwise is how the eight bugs in {@code HANDOFF.md} happened.
 *
 * <p>{@code /_serverless/nodes} is derived from live leases, not from a member list anyone agreed on —
 * so two nodes may legitimately return different answers, and neither is wrong.
 *
 * <p><b>There is no endpoint that lists indices</b>, not even a paginated one. Enumerating a deployment
 * is an inventory operation and belongs to maintenance, the way S3 offers prefix listings and a
 * scheduled inventory but no "count the objects in this bucket". Both endpoints here are addressed:
 * shards are asked for by index name, and the shard list comes from that index's descriptor rather than
 * from a listing. See {@code IndexDescriptor.MAX_SHARDS} for why that stays true.
 *
 * <p><b>Off the transport thread, and through the action gate.</b> Every read below is an object-store
 * round trip -- a descriptor, a head and a manifest per shard, a membership refresh -- and they used to run
 * on the Netty worker that should have been reading the next request, and past any filter a security
 * plugin had installed. They now run on {@code GENERIC} under the cluster-state and nodes-info actions,
 * which is what these reads are in core's vocabulary.
 */
public final class CatalogHandler extends BaseRestHandler {

    private final Supplier<MetadataPlane> plane;
    private final Supplier<org.opensearch.serverless.shell.ServerlessNode> node;

    /**
     * Creates the handler without a node; reads then run on the calling thread and past the gate.
     *
     * @param plane supplies the metadata plane
     */
    public CatalogHandler(Supplier<MetadataPlane> plane) {
        this(plane, () -> null);
    }

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the serving node, for its thread pool and its action gate
     */
    public CatalogHandler(Supplier<MetadataPlane> plane, Supplier<org.opensearch.serverless.shell.ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    @Override
    public String getName() {
        return "serverless_catalog_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "/_serverless/shards/{index}"),
            new Route(RestRequest.Method.GET, "/_serverless/nodes")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Read every parameter before any early return. BaseRestHandler rejects a request whose
        // parameters were not all consumed, so bailing out before reading {index} turns a deliberate
        // 503 into "contains unrecognized parameter" -- a 400 blaming the caller for our own shortcut.
        final String path = request.path();
        final String requestedIndex = request.param("index");

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }

        if (path.contains("/_serverless/shards/")) {
            final String index = requestedIndex;
            return channel -> dispatch(
                channel,
                org.opensearch.action.admin.cluster.state.ClusterStateAction.NAME,
                new org.opensearch.action.admin.cluster.state.ClusterStateRequest(),
                () -> shards(channel, metadata, index)
            );
        }

        // /_serverless/nodes
        return channel -> dispatch(
            channel,
            org.opensearch.action.admin.cluster.node.info.NodesInfoAction.NAME,
            new org.opensearch.action.admin.cluster.node.info.NodesInfoRequest(),
            () -> nodes(channel, metadata)
        );
    }

    private static void shards(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String index) throws IOException {
        final Optional<IndexDescriptor> descriptor = metadata.describe(index);
        if (descriptor.isEmpty()) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index));
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("index", index);
            builder.startArray("shards");
            for (int shard = 0; shard < descriptor.get().numberOfShards(); shard++) {
                final Optional<ShardHead> head = metadata.heads().read(index, shard);
                builder.startObject();
                builder.field("shard", shard);
                if (head.isEmpty()) {
                    // Never activated is a real and useful state, distinct from unowned. A
                    // pre-provisioned shard nobody has written to costs nothing, not even a head.
                    builder.field("state", "never_activated");
                } else {
                    builder.field("state", head.get().ownerNodeId() == null ? "unowned" : "owned");
                    builder.field("term", head.get().term());
                    builder.field("owner_node_id", head.get().ownerNodeId());
                    builder.field("lease_expires_at_millis", head.get().leaseExpiresAtMillis());
                }
                final var manifest = metadata.segmentPublisher(index, shard).readManifest();
                builder.field("published", manifest.isPresent());
                if (manifest.isPresent()) {
                    builder.field("published_term", manifest.get().term());
                    builder.field("published_files", manifest.get().files().size());
                }
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private static void nodes(org.opensearch.rest.RestChannel channel, MetadataPlane metadata) throws IOException {
        metadata.membership().refreshIfOlderThan(Math.max(1_000L, metadata.leaseTtlMillis() / 2));
        final long now = metadata.clock().getAsLong();
        final List<NodeLease> live = new java.util.ArrayList<>();
        for (NodeLease lease : metadata.membership().current()) {
            // The snapshot keeps a lease until the next full refresh notices its blob is gone; a node that
            // died is not "live" past its own expiry, and this endpoint's name is the claim it makes.
            if (lease.isExpiredAt(now) == false) {
                live.add(lease);
            }
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("count", live.size());
            // Named for what it is: this node's observation of live leases, at this instant.
            builder.field("source", "live_leases_observed_by_this_node");
            builder.startArray("nodes");
            for (NodeLease lease : live) {
                builder.startObject();
                builder.field("node_id", lease.nodeId());
                builder.field("ephemeral_id", lease.ephemeralId());
                builder.field("address", lease.address());
                builder.field("roles", lease.roles().stream().sorted().toArray(String[]::new));
                builder.field("lease_expires_at_millis", lease.expiresAtMillis());
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private void dispatch(
        org.opensearch.rest.RestChannel channel,
        String action,
        org.opensearch.action.ActionRequest actionRequest,
        org.opensearch.common.CheckedRunnable<Exception> work
    ) {
        final var serving = node.get();
        final Runnable run = () -> {
            try {
                IndexAdminHandler.gate(serving, action, actionRequest, () -> {
                    work.run();
                    return null;
                });
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a catalog failure", nested);
                }
            }
        };
        if (serving == null) {
            run.run();
            return;
        }
        serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(run);
    }
}
