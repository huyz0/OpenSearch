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
import org.opensearch.serverless.metadata.IndexAlreadyExistsException;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Index lifecycle over the metadata plane: {@code PUT}, {@code GET} and {@code DELETE} on {@code /{index}}.
 *
 * <p>Each of these is one object-store operation, not a cluster-state update. Creation is a
 * put-if-absent whose cost does not depend on how many indices already exist — the ceiling
 * {@code plan-area-h-metadata-off-cluster-state.md} measured, where creating the 6,000th index took
 * 98.8 ms because {@code Metadata.Builder.build()} rebuilds name lookups across every index each time.
 *
 * <p>Name uniqueness is arbitrated by the object store rather than by an elected node, so a duplicate
 * create returns 400 without anything having been serialised anywhere.
 */
public final class IndexAdminHandler extends BaseRestHandler {

    private final Supplier<MetadataPlane> plane;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane, which may not exist yet when routes are registered
     */
    public IndexAdminHandler(Supplier<MetadataPlane> plane) {
        this.plane = plane;
    }

    @Override
    public String getName() {
        return "serverless_index_admin_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.PUT, "/{index}"),
            new Route(RestRequest.Method.GET, "/{index}"),
            new Route(RestRequest.Method.DELETE, "/{index}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final MetadataPlane metadata = plane.get();
        final String index = request.param("index");
        if (metadata == null) {
            return channel -> channel.sendResponse(
                error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "this node has no metadata plane configured")
            );
        }

        switch (request.method()) {
            case PUT: {
                final int shards = request.paramAsInt("shards", 1);
                final String mapping = request.hasContent() ? request.content().utf8ToString() : null;
                return channel -> {
                    try {
                        metadata.createIndex(new IndexDescriptor(index, UUID.randomUUID().toString(), shards, mapping, null));
                        try (XContentBuilder builder = channel.newBuilder()) {
                            builder.startObject();
                            builder.field("acknowledged", true);
                            builder.field("index", index);
                            builder.field("shards", shards);
                            builder.endObject();
                            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                        }
                    } catch (IndexAlreadyExistsException e) {
                        channel.sendResponse(error(channel, RestStatus.BAD_REQUEST, "index_already_exists", e.getMessage()));
                    }
                };
            }
            case GET: {
                final Optional<IndexDescriptor> descriptor = metadata.describe(index);
                return channel -> {
                    if (descriptor.isEmpty()) {
                        // Absent, said plainly. An empty body with 200 would be the confident empty
                        // answer this whole surface exists to avoid.
                        channel.sendResponse(error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index));
                        return;
                    }
                    try (XContentBuilder builder = channel.newBuilder()) {
                        builder.startObject();
                        builder.field("index", descriptor.get().name());
                        builder.field("uuid", descriptor.get().uuid());
                        builder.field("shards", descriptor.get().numberOfShards());
                        builder.field("has_mapping", descriptor.get().mapping() != null);
                        builder.endObject();
                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                    }
                };
            }
            case DELETE: {
                final boolean existed = metadata.deleteIndex(index);
                return channel -> {
                    if (existed == false) {
                        channel.sendResponse(error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index));
                        return;
                    }
                    try (XContentBuilder builder = channel.newBuilder()) {
                        builder.startObject();
                        builder.field("acknowledged", true);
                        builder.field("index", index);
                        builder.endObject();
                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                    }
                };
            }
            default:
                return channel -> channel.sendResponse(
                    error(channel, RestStatus.METHOD_NOT_ALLOWED, "method_not_allowed", request.method() + " is not supported here")
                );
        }
    }

    static BytesRestResponse error(org.opensearch.rest.RestChannel channel, RestStatus status, String type, String reason)
        throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("error", type);
            builder.field("reason", reason);
            builder.field("status", status.getStatus());
            builder.endObject();
            return new BytesRestResponse(status, builder);
        }
    }
}
