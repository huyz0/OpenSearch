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
 */
public final class CatalogHandler extends BaseRestHandler {

    private final Supplier<MetadataPlane> plane;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     */
    public CatalogHandler(Supplier<MetadataPlane> plane) {
        this.plane = plane;
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
            final Optional<IndexDescriptor> descriptor = metadata.describe(index);
            if (descriptor.isEmpty()) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index)
                );
            }
            return channel -> {
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
            };
        }

        // /_serverless/nodes
        metadata.membership().refresh();
        final var live = metadata.membership().current();
        return channel -> {
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
        };
    }
}
