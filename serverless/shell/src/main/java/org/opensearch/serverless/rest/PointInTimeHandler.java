/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.common.UUIDs;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.PointInTime;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@code POST /{index}/_search/point_in_time} and {@code DELETE /_search/point_in_time/{id}} — freezing a
 * view of an index. {@code /{index}/_pit} and {@code /_pit/{id}} are accepted too; see {@link #routes()} for
 * why both spellings exist and which one is OpenSearch's.
 *
 * <p><b>What it is for.</b> {@code search_after} pages through a result set, and between two pages the
 * index moves: documents are written, merged and deleted. A frozen view makes both pages read the same
 * commits, which is the difference between exporting a result set and exporting whatever happened to be
 * there each time you asked.
 *
 * <p><b>It freezes what has been published, not what has been written.</b> A writer acknowledges a write
 * once it is in the log, and publishes some time later; a view taken now contains the last publish and not
 * the acknowledged writes after it. That is the same thing a reader sees at any time and the same thing a
 * classic point in time does — a pinned reader is a pinned reader — but it is worth saying, because a
 * caller who writes a document and immediately freezes will not find it.
 *
 * <p><b>The keep-alive is absolute and searching does not extend it.</b> A sliding one would let a caller
 * paging slowly hold a commit indefinitely without ever having said they meant to, and the files a view
 * holds are files the collector cannot reclaim. A caller who needs longer asks for longer.
 */
public final class PointInTimeHandler extends BaseRestHandler {

    /** How long a view is held when the caller does not say. */
    static final long DEFAULT_KEEP_ALIVE_MILLIS = 300_000L;

    /** The longest a view may be held, so one request cannot pin a commit for a week. */
    static final long MAX_KEEP_ALIVE_MILLIS = 3_600_000L;

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;

    /**
     * Creates the handler.
     *
     * @param node supplies the node
     * @param plane supplies the metadata plane
     */
    public PointInTimeHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
    }

    @Override
    public String getName() {
        return "serverless_pit_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            // OpenSearch's own spelling, which is what an OpenSearch client library calls and what AWS's
            // serverless offering lists as supported. This handler shipped with Elasticsearch's spelling
            // instead -- on a fork of OpenSearch -- so a correctly-written caller got "no handler found".
            new Route(RestRequest.Method.POST, "/{index}/_search/point_in_time"),
            new Route(RestRequest.Method.DELETE, "/_search/point_in_time/{id}"),
            // Kept, because they are what this shell's own callers already use and removing them would break
            // them to fix a compatibility bug, which is a strange trade.
            new Route(RestRequest.Method.POST, "/{index}/_pit"),
            new Route(RestRequest.Method.DELETE, "/_pit/{id}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String index = request.param("index");
        final String id = request.param("id");
        final long keepAlive = Math.min(
            request.paramAsTime("keep_alive", org.opensearch.common.unit.TimeValue.timeValueMillis(DEFAULT_KEEP_ALIVE_MILLIS)).millis(),
            MAX_KEEP_ALIVE_MILLIS
        );

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        final ServerlessNode serving = node.get();

        if (request.method() == RestRequest.Method.DELETE) {
            return channel -> serving.threadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
                try {
                    release(channel, serving, metadata, id);
                } catch (Exception e) {
                    report(channel, e);
                }
            });
        }
        return channel -> serving.threadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                freeze(channel, metadata, index, keepAlive);
            } catch (Exception e) {
                report(channel, e);
            }
        });
    }

    private void freeze(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String index, long keepAlive) throws IOException {
        final Optional<IndexDescriptor> descriptor = metadata.describe(index);
        if (descriptor.isEmpty()) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index));
            return;
        }

        final Map<Integer, CommitManifest> shards = new LinkedHashMap<>();
        for (int shard = 0; shard < descriptor.get().numberOfShards(); shard++) {
            final var manifest = metadata.segmentPublisher(index, descriptor.get().uuid(), shard).readManifest();
            if (manifest.isEmpty()) {
                // A shard with nothing published cannot be frozen, and freezing the rest would give a view
                // that silently covers part of the index -- which is exactly what the coverage numbers on a
                // search exist to make impossible.
                channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.CONFLICT,
                        "nothing_published",
                        "shard " + shard + " of " + index + " has published nothing yet, so there is no commit to freeze"
                    )
                );
                return;
            }
            shards.put(shard, manifest.get());
        }

        final PointInTime pit = new PointInTime(UUIDs.randomBase64UUID(), index, metadata.clock().getAsLong() + keepAlive, shards);
        // Written before it is answered with, because the collector reads these and a view nobody had
        // recorded would be a promise the sweep never heard.
        metadata.createPointInTime(pit);

        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("pit_id", pit.id());
            builder.field("index", index);
            builder.field("shards", shards.size());
            builder.field("keep_alive_millis", keepAlive);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private void release(org.opensearch.rest.RestChannel channel, ServerlessNode serving, MetadataPlane metadata, String id)
        throws IOException {
        // Local shards first, then the record. The other order would leave this node holding open shards
        // for a view that no longer exists, which nothing would ever come back to close.
        final int closed = serving.reconciler().closeFrozenReader(id);
        final boolean existed = metadata.releasePointInTime(id);
        if (existed == false && closed == 0) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "pit_not_found", "no such point in time: " + id));
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("released", true);
            builder.field("pit_id", id);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private void report(org.opensearch.rest.RestChannel channel, Exception e) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, e));
        } catch (IOException nested) {
            logger.error("failed to report a point-in-time failure", nested);
        }
    }
}
