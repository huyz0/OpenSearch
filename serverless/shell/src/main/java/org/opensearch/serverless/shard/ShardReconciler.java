/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.MergedSegmentWarmerFactory;
import org.opensearch.index.seqno.RetentionLeaseSyncer;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.serverless.cluster.ShardAssignment;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Opens and closes shards on this node.
 *
 * <p><b>The invariant this class exists to hold</b>, established by measurement in
 * {@code s1-findings.md} rather than by argument:
 *
 * <blockquote>Absence from a projected view is never a removal signal. A shard closes when the
 * shard-head says this node no longer owns it, and never because a locally-computed view failed to
 * mention it.</blockquote>
 *
 * <p>S1 measured both halves of why that matters. Applying a view naming no indices at all left a shard
 * STARTED and still serving, so projection is harmless on its own. But the close-set a
 * <em>diff-based</em> reconciler would have computed was exactly the set of locally-open shards. The
 * hazard is entirely a property of the reconciler, which is this class.
 *
 * <p>So the invariant is structural, not merely tested. {@link #ensureOpen} contains no code path that
 * closes anything, and {@link #releaseShard} takes a shard id the caller must have obtained from truth.
 * There is deliberately no method that accepts a view and closes what is missing from it, which is the
 * shape {@code IndicesClusterStateService} uses and the reason it is replaced rather than reused.
 */
public final class ShardReconciler {

    private final IndicesService indicesService;
    private final DiscoveryNode localNode;
    private final Map<ShardId, IndexShard> open = new ConcurrentHashMap<>();

    /**
     * Creates a reconciler for one node.
     *
     * @param indicesService the node's indices service
     * @param localNode this node's identity
     */
    public ShardReconciler(IndicesService indicesService, DiscoveryNode localNode) {
        this.indicesService = indicesService;
        this.localNode = localNode;
    }

    /**
     * Opens and starts every assigned shard that is not already open. Idempotent, and never closes
     * anything.
     *
     * @param view the node-local view, used for index metadata and node identity
     * @param owned the shards this node owns according to shard-heads
     * @return the shards opened by this call, excluding those already open
     * @throws IOException if a shard cannot be created or recovered
     */
    public Set<ShardId> ensureOpen(ClusterState view, Collection<ShardAssignment> owned) throws IOException {
        final Set<ShardId> opened = new LinkedHashSet<>();
        for (ShardAssignment assignment : owned) {
            final IndexMetadata indexMetadata = view.metadata().index(assignment.indexName());
            if (indexMetadata == null) {
                // Truth says this node owns the shard, but the view does not describe the index. That is
                // a projector bug, not a removal: refusing loudly is the only safe response, because
                // continuing would silently serve nothing.
                throw new IllegalStateException(
                    "assigned shard "
                        + assignment
                        + " has no descriptor in the projected view; the projector must describe every"
                        + " index this node owns a shard of"
                );
            }
            final ShardId shardId = new ShardId(indexMetadata.getIndex(), assignment.shardId());
            if (open.containsKey(shardId)) {
                continue;
            }
            open.put(shardId, openAndStart(indexMetadata, shardId, assignment.term(), view.nodes()));
            opened.add(shardId);
        }
        return opened;
    }

    /**
     * The four calls S0 measured, in order. Step two is the one that is easy to omit: createIndex does
     * not apply the mapping, and without it the first write returns MAPPING_UPDATE_REQUIRED as a
     * <em>result value</em> rather than throwing (see {@code s0-findings.md} F5).
     */
    private IndexShard openAndStart(IndexMetadata indexMetadata, ShardId shardId, long shardHeadTerm, DiscoveryNodes nodes)
        throws IOException {
        final Index index = indexMetadata.getIndex();
        IndexService indexService = indicesService.indexService(index);
        if (indexService == null) {
            indexService = indicesService.createIndex(indexMetadata, Collections.emptyList(), false);
            indexService.updateMapping(null, indexMetadata);
        }

        final ShardRouting initializing = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "serverless activation")
        ).initialize(localNode.getId(), null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);

        final IndexShard shard = indexService.createShard(
            initializing,
            ignored -> {},
            RetentionLeaseSyncer.EMPTY,
            null,
            null,
            null,
            localNode,
            null,
            nodes,
            new MergedSegmentWarmerFactory(null, null, null),
            null,
            null
        );

        shard.markAsRecovering("serverless-store", new RecoveryState(initializing, localNode, null));
        final PlainActionFuture<Boolean> recovered = PlainActionFuture.newFuture();
        shard.recoverFromStore(recovered);
        if (Boolean.TRUE.equals(recovered.actionGet()) == false) {
            throw new IOException("recovery from store reported failure for " + shardId);
        }

        final ShardRouting started = initializing.moveToStarted();
        // The applying version is the SHARD-HEAD TERM, not the projected view's version. S1 reproduced
        // what happens when a per-node counter is used here: the update is silently ignored, because
        // ReplicationTracker gates on > against a value that may have crossed a node boundary.
        shard.updateShardState(
            started,
            shardHeadTerm,
            null,
            shardHeadTerm,
            Set.of(started.allocationId().getId()),
            new IndexShardRoutingTable.Builder(shardId).addShard(started).build(),
            nodes
        );
        return shard;
    }

    /**
     * Closes one shard. This is the <em>only</em> way a shard closes, and the caller must have learned
     * from a shard-head that this node no longer owns it.
     *
     * @param shardId the shard to release
     * @param reason why, for the log
     */
    public void releaseShard(ShardId shardId, String reason) {
        final IndexShard shard = open.remove(shardId);
        if (shard == null) {
            return;
        }
        final Index index = shardId.getIndex();
        final IndexService indexService = indicesService.indexService(index);
        if (indexService != null && indexService.hasShard(shardId.id())) {
            indexService.removeShard(shardId.id(), reason);
        }
        if (open.keySet().stream().noneMatch(id -> id.getIndex().equals(index))) {
            indicesService.removeIndex(index, IndexRemovalReason.NO_LONGER_ASSIGNED, reason);
        }
    }

    /**
     * Returns the shard for an id, or null if this node does not hold it.
     *
     * @param shardId the shard
     * @return the shard, or null
     */
    public IndexShard shard(ShardId shardId) {
        return open.get(shardId);
    }

    /**
     * Returns the shards currently open on this node.
     *
     * @return the open shard ids
     */
    public Set<ShardId> openShards() {
        return Set.copyOf(open.keySet());
    }
}
