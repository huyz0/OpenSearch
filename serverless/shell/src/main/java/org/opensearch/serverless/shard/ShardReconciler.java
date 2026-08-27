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
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.SegmentPublisher;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

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
    private volatile BiFunction<String, Integer, SegmentPublisher> publishers;

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
     * Supplies the object-store publisher for each shard. Without one, shards open from local disk only,
     * which is the phase 2 behaviour and is what the projection tests still exercise.
     *
     * @param publishers index name and shard number to publisher
     */
    public void setSegmentPublishers(BiFunction<String, Integer, SegmentPublisher> publishers) {
        this.publishers = publishers;
    }

    /**
     * Publishes a shard's current commit to the object store.
     *
     * @param shardId the shard
     * @param term the owning writer's term, which fences a zombie
     * @return the manifest published
     * @throws IOException if the shard is not held here, or publication fails
     */
    public CommitManifest publish(ShardId shardId, long term) throws IOException {
        final IndexShard shard = open.get(shardId);
        if (shard == null) {
            throw new IOException("cannot publish " + shardId + ": not open on " + localNode.getId());
        }
        if (publishers == null) {
            throw new IOException("cannot publish " + shardId + ": no object store configured");
        }
        // A commit must exist before there is anything to publish; an unflushed shard has its data only
        // in the translog, which this does not upload.
        shard.flush(new org.opensearch.action.admin.indices.flush.FlushRequest().force(true).waitIfOngoing(true));
        return publishers.apply(shardId.getIndexName(), shardId.id()).publish(shard.store(), term);
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

        // Whether anything was published decides the recovery source, and the decision must be made
        // before the shard is created because the source is baked into its routing entry.
        final SegmentPublisher publisher = publishers == null ? null : publishers.apply(shardId.getIndexName(), shardId.id());
        final Optional<CommitManifest> published = publisher == null ? Optional.empty() : publisher.readManifest();
        final boolean restoring = published.isPresent() && published.get().files().isEmpty() == false;

        final ShardRouting initializing = ShardRouting.newUnassigned(
            shardId,
            true,
            restoring ? RecoverySource.ExistingStoreRecoverySource.INSTANCE : RecoverySource.EmptyStoreRecoverySource.INSTANCE,
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

        if (restoring) {
            // Between createShard and recovery: the store exists but nothing has opened it yet. Using
            // EMPTY_STORE here instead would call Store#createEmpty and delete exactly what this writes.
            publisher.restoreInto(shard.store().directory(), shardId);
            bootstrapTranslogFor(shard);
        }

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
     * Gives a restored commit a fresh, empty translog.
     *
     * <p>Restoring segments is not enough on its own: recovery reads a translog whose UUID matches the
     * commit, and a node taking a shard over has neither. Without this the shard fails recovery with a
     * corrupt-translog error naming a file that was never written.
     *
     * <p>The sequence is the one {@code StoreRecovery} uses for snapshot restore, for the same reason —
     * segments arrived from somewhere other than a peer, so the history has to be re-established rather
     * than continued. Operations the previous writer had accepted but not committed are lost here; that
     * is what a write-ahead log is for, and this phase does not have one (see phase 4's notes).
     */
    private void bootstrapTranslogFor(IndexShard shard) throws IOException {
        shard.store().bootstrapNewHistory();
        final org.apache.lucene.index.SegmentInfos segmentInfos = shard.store().readLastCommittedSegmentsInfo();
        final long localCheckpoint = Long.parseLong(
            segmentInfos.userData.get(org.opensearch.index.seqno.SequenceNumbers.LOCAL_CHECKPOINT_KEY)
        );
        final String translogUUID = segmentInfos.getUserData().get(org.opensearch.index.translog.Translog.TRANSLOG_UUID_KEY);
        org.opensearch.index.translog.Translog.createEmptyTranslog(
            shard.shardPath().resolveTranslog(),
            shard.shardId(),
            localCheckpoint,
            shard.getPendingPrimaryTerm(),
            translogUUID,
            java.nio.channels.FileChannel::open
        );
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
