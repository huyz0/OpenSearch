/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.GarbageCollector;
import org.opensearch.serverless.reconcile.NodePackCollector;
import org.opensearch.serverless.store.NodePackLedger;
import org.opensearch.serverless.store.NodePackStore;
import org.opensearch.serverless.store.NodePackWriter;
import org.opensearch.serverless.store.SegmentPublisher;
import org.opensearch.threadpool.ThreadPool;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.function.Function;

/**
 * Builds the segment publishers a node's shards use, with node packs when the node has them on.
 *
 * <p>Node packs are opt-in through {@code serverless.segments.node_pack.enabled}, off by default: with them off every
 * shard keeps one blob per segment under its own prefix. With them on, a publish's segment blobs of up to
 * {@code serverless.segments.node_pack.max_blob_bytes} go into the node's next pack, shared with whatever other shards
 * of any index publish within {@code serverless.segments.node_pack.linger_millis}; larger blobs keep their own PUT.
 * Packs build on per-segment blobs, so they apply only while {@code serverless.segments.pack.enabled} is on too.
 *
 * <p>One writer serves every shard of a node for one plane, so concurrent publishes share packs. Its epoch is the
 * wall-clock millisecond it was made at, which a restarted node has always moved past, and is recorded in the node's
 * {@link NodePackLedger} just before its first pack is written. A {@link NodePackCollector} beside it deletes the node's
 * packs, of this epoch and every earlier one, once nothing names them, every
 * {@code serverless.segments.node_pack.collect_interval_millis}.
 */
final class NodePackWiring {

    static final String ENABLED = "serverless.segments.node_pack.enabled";
    static final String MAX_BLOB_BYTES = "serverless.segments.node_pack.max_blob_bytes";
    static final String LINGER_MILLIS = "serverless.segments.node_pack.linger_millis";
    static final String COLLECT_INTERVAL_MILLIS = "serverless.segments.node_pack.collect_interval_millis";
    static final long DEFAULT_COLLECT_INTERVAL_MILLIS = 60_000L;
    static final long DEFAULT_MAX_BLOB_BYTES = 8L * 1024 * 1024;
    static final long DEFAULT_LINGER_MILLIS = 50L;
    /** A pack this full is written without waiting out the linger. */
    static final long MAX_PACK_BYTES = 64L * 1024 * 1024;

    private static final Map<ServerlessNode, Map<MetadataPlane, NodePackWriter>> WRITERS = new WeakHashMap<>();
    private static long lastEpoch = -1L;

    private NodePackWiring() {}

    /**
     * Returns how a node opens each shard's publisher on a plane.
     *
     * @param node the node
     * @param plane the plane its shards publish to
     * @return the publisher for a shard
     */
    static Function<ShardId, SegmentPublisher> publishers(ServerlessNode node, MetadataPlane plane) {
        final boolean enabled = node.packSegments && node.settings.getAsBoolean(ENABLED, false);
        if (enabled == false) {
            return id -> plane.segmentPublisher(id.getIndexName(), id.getIndex().getUUID(), id.id()).packing(node.packSegments);
        }
        final long maxBlobBytes = maxBlobBytes(node.settings);
        return id -> plane.segmentPublisher(id.getIndexName(), id.getIndex().getUUID(), id.id())
            .packing(true)
            .nodePacks(writer(node, plane), id.getIndexName(), id.getIndex().getUUID(), id.id(), maxBlobBytes);
    }

    static long maxBlobBytes(Settings settings) {
        return settings.getAsBytesSize(MAX_BLOB_BYTES, new org.opensearch.core.common.unit.ByteSizeValue(DEFAULT_MAX_BLOB_BYTES))
            .getBytes();
    }

    private static NodePackWriter writer(ServerlessNode node, MetadataPlane plane) {
        synchronized (WRITERS) {
            final Map<MetadataPlane, NodePackWriter> byPlane = WRITERS.computeIfAbsent(node, n -> new IdentityHashMap<>());
            final NodePackWriter existing = byPlane.get(plane);
            if (existing != null) {
                return existing;
            }
            final String nodeId = node.localNode().getId();
            final NodePackStore store = new NodePackStore(plane.blobStore(), plane.basePath(), nodeId);
            final NodePackLedger ledger = new NodePackLedger(plane.blobStore(), plane.basePath(), nodeId);
            final long epoch = nextEpoch();
            final long linger = node.settings.getAsLong(LINGER_MILLIS, DEFAULT_LINGER_MILLIS);
            final NodePackWriter writer = new NodePackWriter(store, epoch, linger, MAX_PACK_BYTES);
            writer.recordIn(ledger);
            final NodePackCollector collector = new NodePackCollector(
                store,
                ledger,
                plane,
                plane.clock(),
                GarbageCollector.DEFAULT_MINIMUM_UNREFERENCED_MILLIS
            );
            collector.follow(writer);
            final long interval = node.settings.getAsLong(COLLECT_INTERVAL_MILLIS, DEFAULT_COLLECT_INTERVAL_MILLIS);
            if (interval > 0L) {
                node.threadPool()
                    .scheduleWithFixedDelay(collector::runQuietly, TimeValue.timeValueMillis(interval), ThreadPool.Names.GENERIC);
            }
            byPlane.put(plane, writer);
            return writer;
        }
    }

    // Two writers made in one process within the same millisecond, as tests do, still get distinct epochs.
    private static long nextEpoch() {
        lastEpoch = Math.max(lastEpoch + 1L, System.currentTimeMillis());
        return lastEpoch;
    }
}
