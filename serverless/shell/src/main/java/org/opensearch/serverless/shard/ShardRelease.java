/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shard;

import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;

/**
 * Closes shards for {@link ShardReconciler}: a shard let go, a reader gone stale or an engine that failed, and the
 * index service once its last shard is gone.
 */
final class ShardRelease {

    private final ShardReconciler reconciler;

    ShardRelease(ShardReconciler reconciler) {
        this.reconciler = reconciler;
    }

    /**
     * Removes only the engine that failed. Its admitted queries cannot keep an unusable reader cached,
     * and an asynchronously delivered failure cannot remove a healthy replacement.
     *
     * @param failed the actual failed engine
     * @param reason why it is being removed
     * @return the removed engine's role, or {@link ShardReconciler.FailedShardRelease#NOT_CURRENT}
     */
    public ShardReconciler.FailedShardRelease releaseFailedShard(IndexShard failed, String reason) {
        final ShardId shardId = failed.shardId();
        synchronized (reconciler.lifecycle(shardId)) {
            if (reconciler.open.get(shardId) != failed) {
                return ShardReconciler.FailedShardRelease.NOT_CURRENT;
            }
            final boolean reader = reconciler.readers.contains(shardId);
            releaseShardLocked(shardId, reason, false, true);
            return reader ? ShardReconciler.FailedShardRelease.READER : ShardReconciler.FailedShardRelease.WRITER;
        }
    }

    boolean releaseShardLocked(ShardId shardId, String reason, boolean deleteStore) {
        return releaseShardLocked(shardId, reason, deleteStore, false);
    }

    boolean releaseShardLocked(ShardId shardId, String reason, boolean deleteStore, boolean failed) {
        reconciler.publishedThroughAtOpen.remove(shardId);
        if (reconciler.readers.contains(shardId) == false && reconciler.open.containsKey(shardId)) {
            // Writers only: every way a writer stops, for reconstructing a handover.
            final IndexShard writer = reconciler.open.get(shardId);
            long maxSeqNo = -1L;
            long term = -1L;
            try {
                maxSeqNo = writer.seqNoStats().getMaxSeqNo();
                term = writer.getOperationPrimaryTerm();
            } catch (Exception e) {
                // closing under us
            }
            ShardReconciler.logger.info("releasing writer {} at term {} with max seqNo {}: {}", shardId, term, maxSeqNo, reason);
        }
        final int busy = reconciler.inFlight(shardId);
        // Readers only. A writer being released has lost its head, or is about to give it up, and a
        // writer that stays open past that point would go on acknowledging writes nobody will replay; a
        // query against it failing on a closed reader is the lesser harm, and the one this always risked.
        if (failed == false && busy > 0 && reconciler.readers.contains(shardId)) {
            ShardReconciler.logger.debug(
                "not releasing {} ({}): {} quer{} still running against it",
                shardId,
                reason,
                busy,
                busy == 1 ? "y is" : "ies are"
            );
            return false;
        }
        // Out of the open set before it stops being a reader: a shard open and not a reader is this node's writer to
        // the write path, and a reader is a writable engine on an ingest node.
        final IndexShard shard = reconciler.open.remove(shardId);
        reconciler.readers.remove(shardId);
        reconciler.readerCommits.remove(shardId);
        reconciler.walCache.remove(shardId);
        reconciler.publisherCache.remove(shardId);
        reconciler.lastUsed.remove(shardId);
        reconciler.writeGuards.remove(shardId);
        final Index index = shardId.getIndex();
        final IndexService indexService = reconciler.indicesService.indexService(index);
        boolean removed = false;
        if (indexService != null && indexService.hasShard(shardId.id())) {
            indexService.removeShard(shardId.id(), reason);
            removed = true;
        }
        if (shard == null && removed == false) {
            return true;
        }
        closeIndexIfUnused(index, reason, deleteStore);
        return true;
    }

    /** Closes the index's service once nothing of ours is open or opening under it, and evicts its monitor. */
    void closeIndexIfUnused(Index index, String reason, boolean deleteStore) {
        while (true) {
            final Object lock = reconciler.indexLocks.computeIfAbsent(index, ignored -> new Object());
            synchronized (lock) {
                if (reconciler.indexLocks.get(index) != lock) {
                    continue;
                }
                final IndexService service = reconciler.indicesService.indexService(index);
                // A release must depend only on this index. Scanning every held shard for every close
                // made releasing N single-shard indices quadratic, including frozen-view churn.
                final boolean inUse = (service != null && service.numberOfShards() > 0) || reconciler.opening.containsKey(index);
                if (inUse == false) {
                    // DELETED wipes the directory; NO_LONGER_ASSIGNED closes and keeps it. A view's
                    // directory is never opened again, a live shard's is a cache the next open uses.
                    reconciler.indicesService.removeIndex(
                        index,
                        deleteStore ? IndexRemovalReason.DELETED : IndexRemovalReason.NO_LONGER_ASSIGNED,
                        reason
                    );
                    reconciler.indexLocks.remove(index, lock);
                }
                return;
            }
        }
    }
}
