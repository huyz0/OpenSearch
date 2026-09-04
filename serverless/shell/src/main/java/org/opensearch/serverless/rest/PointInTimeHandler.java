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
            // OpenSearch's own spellings, which are what an OpenSearch client library calls and what AWS's
            // serverless offering lists as supported. Deleting is a body naming one or more ids, or _all;
            // listing is _all. This handler shipped with an id-in-the-path delete of its own invention, so
            // a correctly-written client could take a view and never release it: the body form was "no
            // handler found", and _all matched the placeholder and answered 404 having deleted nothing.
            new Route(RestRequest.Method.POST, "/{index}/_search/point_in_time"),
            new Route(RestRequest.Method.DELETE, "/_search/point_in_time"),
            new Route(RestRequest.Method.DELETE, "/_search/point_in_time/_all"),
            new Route(RestRequest.Method.GET, "/_search/point_in_time/_all"),
            // Kept, because they are what this shell's own callers already use and removing them would break
            // them to fix a compatibility bug, which is a strange trade.
            new Route(RestRequest.Method.DELETE, "/_search/point_in_time/{id}"),
            new Route(RestRequest.Method.POST, "/{index}/_pit"),
            new Route(RestRequest.Method.DELETE, "/_pit/{id}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String index = request.param("index");
        final String id = request.param("id");
        final boolean all = request.path().endsWith("/_all");
        final long keepAlive = request.paramAsTime(
            "keep_alive",
            org.opensearch.common.unit.TimeValue.timeValueMillis(DEFAULT_KEEP_ALIVE_MILLIS)
        ).millis();
        // Hints core's create action takes and this one has nothing to do with: there is one copy of each
        // shard, placed by id, and a view either freezes every shard's commit or is refused.
        request.param("preference");
        request.param("routing");
        request.param("allow_partial_pit_creation");
        request.param("expand_wildcards");

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        final ServerlessNode serving = node.get();

        if (request.method() == RestRequest.Method.GET) {
            return channel -> serving.threadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
                try {
                    IndexAdminHandler.gate(
                        serving,
                        org.opensearch.action.search.GetAllPitsAction.NAME,
                        new org.opensearch.action.search.GetAllPitNodesRequest(new org.opensearch.cluster.node.DiscoveryNode[0]),
                        () -> {
                            list(channel, metadata);
                            return null;
                        }
                    );
                } catch (Exception e) {
                    report(channel, e);
                }
            });
        }
        if (request.method() == RestRequest.Method.DELETE) {
            final List<String> ids = new java.util.ArrayList<>();
            if (id != null) {
                ids.add(id);
            } else if (all == false) {
                // Core's shape: {"pit_id": ["...", "..."]}.
                if (request.hasContent() == false) {
                    return channel -> channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.BAD_REQUEST,
                            "missing_body",
                            "name the views to release in a pit_id array, or delete _all"
                        )
                    );
                }
                try (var parser = request.contentParser()) {
                    final Object named = parser.map().get("pit_id");
                    if (named instanceof List<?> many) {
                        for (Object each : many) {
                            ids.add(String.valueOf(each));
                        }
                    } else if (named instanceof String one) {
                        ids.add(one);
                    }
                }
                if (ids.isEmpty()) {
                    return channel -> channel.sendResponse(
                        IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "pit_id must name at least one view")
                    );
                }
            }
            return channel -> serving.threadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
                try {
                    IndexAdminHandler.gate(
                        serving,
                        org.opensearch.action.search.DeletePitAction.NAME,
                        new org.opensearch.action.search.DeletePitRequest(all ? List.of("_all") : ids),
                        () -> {
                            release(channel, serving, metadata, all ? null : ids);
                            return null;
                        }
                    );
                } catch (Exception e) {
                    report(channel, e);
                }
            });
        }
        if (keepAlive > MAX_KEEP_ALIVE_MILLIS) {
            // Refused rather than clamped. A caller asking for a day and being handed an hour, with a 200,
            // finds out when their paging fails at minute sixty-one -- which is exactly the kind of quiet
            // substitution a keep-alive exists to make unnecessary.
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "keep_alive_too_long",
                    "keep_alive may be at most "
                        + org.opensearch.common.unit.TimeValue.timeValueMillis(MAX_KEEP_ALIVE_MILLIS)
                        + "; a view pins a commit's files for as long as it lives"
                )
            );
        }
        return channel -> serving.threadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                IndexAdminHandler.gate(
                    serving,
                    org.opensearch.action.search.CreatePitAction.NAME,
                    new org.opensearch.action.search.CreatePitRequest(
                        org.opensearch.common.unit.TimeValue.timeValueMillis(keepAlive),
                        false,
                        index
                    ),
                    () -> {
                        freeze(channel, metadata, index, keepAlive, request);
                        return null;
                    }
                );
            } catch (Exception e) {
                report(channel, e);
            }
        });
    }

    private void freeze(
        org.opensearch.rest.RestChannel channel,
        MetadataPlane metadata,
        String index,
        long keepAlive,
        org.opensearch.core.xcontent.ToXContent.Params params
    ) throws IOException {
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

        final long now = metadata.clock().getAsLong();
        final PointInTime pit = new PointInTime(UUIDs.randomBase64UUID(), index, now + keepAlive, shards);
        // Written before it is answered with, because the collector reads these and a view nobody had
        // recorded would be a promise the sweep never heard.
        metadata.createPointInTime(pit);

        // Core's CreatePitResponse shape -- pit_id, a broadcast _shards header, creation_time -- with the
        // index and the deadline this shell's own callers read after it. Every shard is in the view or the
        // request was refused above, so the header's counts are the shard count twice and no failures.
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("pit_id", pit.id());
            org.opensearch.rest.action.RestActions.buildBroadcastShardsHeader(
                builder,
                params,
                shards.size(),
                shards.size(),
                0,
                0,
                org.opensearch.action.search.ShardSearchFailure.EMPTY_ARRAY
            );
            builder.field("creation_time", now);
            builder.field("index", index);
            builder.field("keep_alive_millis", keepAlive);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /**
     * Releases views, and says which.
     *
     * @param ids the views to release, or null for every live one
     */
    private void release(org.opensearch.rest.RestChannel channel, ServerlessNode serving, MetadataPlane metadata, List<String> ids)
        throws IOException {
        final List<String> named = ids;
        final List<String> targets;
        if (named == null) {
            // _all is a listing, and the reaper already pays this one every tenth pass; paying it once
            // more on request is what the endpoint is for.
            targets = new java.util.ArrayList<>();
            for (PointInTime live : metadata.livePointsInTime(metadata.clock().getAsLong())) {
                targets.add(live.id());
            }
        } else {
            targets = named;
        }
        final Map<String, Boolean> released = new LinkedHashMap<>();
        for (String id : targets) {
            // Local shards first, then the record. The other order would leave this node holding open
            // shards for a view that no longer exists, which nothing would ever come back to close.
            final int closed = serving.reconciler().closeFrozenReader(id);
            final boolean existed = metadata.releasePointInTime(id);
            released.put(id, existed || closed > 0);
        }
        if (named != null && released.values().stream().noneMatch(Boolean::booleanValue)) {
            channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_FOUND,
                    "pit_not_found",
                    "no such point in time: " + String.join(", ", named)
                )
            );
            return;
        }
        // Core's DeletePitResponse shape: one entry per id, saying whether it was there to release.
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startArray("pits");
            for (Map.Entry<String, Boolean> each : released.entrySet()) {
                builder.startObject();
                builder.field("successful", each.getValue());
                builder.field("pit_id", each.getKey());
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /** Lists every live view, in core's GET _all shape: {@code pits[]} of id and remaining keep-alive. */
    private void list(org.opensearch.rest.RestChannel channel, MetadataPlane metadata) throws IOException {
        final long now = metadata.clock().getAsLong();
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startArray("pits");
            for (PointInTime live : metadata.livePointsInTime(now)) {
                builder.startObject();
                builder.field("pit_id", live.id());
                // What is left, not what was asked: this record carries a deadline rather than a creation
                // time, and the remaining life is the number a caller deciding whether to extend needs.
                builder.field("keep_alive", Math.max(0L, live.expiresAtMillis() - now));
                builder.field("index", live.index());
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private void report(org.opensearch.rest.RestChannel channel, Exception e) {
        try {
            channel.sendResponse(IndexAdminHandler.failure(channel, e));
        } catch (IOException nested) {
            logger.error("failed to report a point-in-time failure", nested);
        }
    }
}
