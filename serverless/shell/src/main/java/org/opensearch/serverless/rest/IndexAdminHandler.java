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
    private final Supplier<org.opensearch.serverless.shell.ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane, which may not exist yet when routes are registered
     */
    public IndexAdminHandler(Supplier<MetadataPlane> plane) {
        this(plane, () -> null);
    }

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the node, whose action gate the plugins' filters live behind
     */
    public IndexAdminHandler(Supplier<MetadataPlane> plane, Supplier<org.opensearch.serverless.shell.ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    /**
     * Runs an administrative operation through the plugins' action filters.
     *
     * <p>Creating and deleting an index are the two operations here that change anything, and a plugin
     * evaluating privileges needs to see them under the names it already knows. A node with no filters
     * pays nothing.
     */
    /**
     * Runs work off the transport thread, or inline when there is no node to borrow a pool from.
     *
     * <p>Everything here touches the object store, and the thread that reads HTTP is not the thread to wait
     * on remote IO from.
     */
    private void dispatch(org.opensearch.rest.RestChannel channel, org.opensearch.common.CheckedRunnable<Exception> work) {
        final var serving = node.get();
        if (serving == null) {
            runQuietly(channel, work);
            return;
        }
        serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> runQuietly(channel, work));
    }

    private void runQuietly(org.opensearch.rest.RestChannel channel, org.opensearch.common.CheckedRunnable<Exception> work) {
        try {
            work.run();
        } catch (Exception e) {
            try {
                channel.sendResponse(new BytesRestResponse(channel, e));
            } catch (IOException nested) {
                logger.error("failed to report an index administration failure", nested);
            }
        }
    }

    private <T> T gated(
        String action,
        org.opensearch.action.ActionRequest request,
        org.opensearch.common.CheckedSupplier<T, Exception> work
    ) throws Exception {
        final var serving = node.get();
        return serving == null ? work.get() : serving.actionGate().run(action, request, work);
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
                // Off the transport thread. Creating an index writes to the object store, so this was
                // already blocking the thread that should be reading the next request -- invisibly, because
                // blob IO does not assert about it the way a future does. Running the action filters here
                // made it visible, and the fix is the one every other handler already had.
                return channel -> dispatch(channel, () -> {
                    try {
                        gated(
                            org.opensearch.action.admin.indices.create.CreateIndexAction.NAME,
                            new org.opensearch.action.admin.indices.create.CreateIndexRequest(index),
                            () -> {
                                metadata.createIndex(new IndexDescriptor(index, UUID.randomUUID().toString(), shards, mapping, null));
                                return null;
                            }
                        );
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
                    } catch (Exception e) {
                        channel.sendResponse(new BytesRestResponse(channel, e));
                    }
                });
            }
            case GET: {
                return channel -> dispatch(channel, () -> {
                    final Optional<IndexDescriptor> descriptor = gated(
                        org.opensearch.action.admin.indices.get.GetIndexAction.NAME,
                        new org.opensearch.action.admin.indices.get.GetIndexRequest().indices(index),
                        () -> metadata.describe(index)
                    );
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
                });
            }
            case DELETE: {
                // Inside the consumer, not before it: the delete has to happen after the filters have had
                // their say, and doing it while preparing the request would delete the index and then ask.
                return channel -> dispatch(channel, () -> {
                    final boolean existed;
                    try {
                        existed = gated(
                            org.opensearch.action.admin.indices.delete.DeleteIndexAction.NAME,
                            new org.opensearch.action.admin.indices.delete.DeleteIndexRequest(index),
                            () -> metadata.deleteIndex(index)
                        );
                    } catch (Exception e) {
                        channel.sendResponse(new BytesRestResponse(channel, e));
                        return;
                    }
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
                });
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
