/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.reconcile;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.shell.WriterHibernation;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Hibernates {@link BackgroundReconciler}'s idle writers: publishes what they hold, closes their engines and keeps
 * their heads, so the next request wakes them at the same term instead of taking them over. See
 * {@link WriterHibernation} for the state and why waking is safe.
 *
 * <p><b>Two thresholds, not one.</b> A writer unused for {@link BackgroundReconciler#setHibernateAfterMillis} is
 * hibernated; one unused for {@link BackgroundReconciler#setIdleAfterMillis} has its head given back, as idle release
 * always did. Hibernating costs what a publish costs, and waking about a head read and an open, so it can come much
 * sooner than giving the head back, which makes the next request pay a whole takeover. The head is still given back
 * in the end: a node holding the heads of every shard it ever wrote would take all of them with it when it dies, and
 * would keep every tenant it ever served from being placed anywhere else.
 */
final class ShardHibernation {

    private final BackgroundReconciler reconciler;

    ShardHibernation(BackgroundReconciler reconciler) {
        this.reconciler = reconciler;
    }

    /**
     * Hibernates the writers unused for the hibernation threshold, and gives back the heads of hibernated writers
     * unused for the idle threshold.
     *
     * @param nowMillis the observer's clock
     * @return the shards whose heads were given back
     */
    Set<ShardId> pass(long nowMillis) {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return Set.of();
            }
            final Set<ShardId> released = new java.util.LinkedHashSet<>();
            if (reconciler.hibernateAfterMillis > 0 && reconciler.demandDriven) {
                for (ShardId shardId : reconciler.node.reconciler().openShards()) {
                    if (reconciler.pinnedIndices.contains(shardId.getIndexName()) || reconciler.node.reconciler().isReader(shardId)) {
                        continue;
                    }
                    final long lastUsed = lastUsed(shardId, nowMillis);
                    if (nowMillis - lastUsed < reconciler.hibernateAfterMillis) {
                        continue;
                    }
                    if (reconciler.idleAfterMillis > 0 && nowMillis - lastUsed >= reconciler.idleAfterMillis) {
                        // Past the idle threshold already: idle release gives it back, which hibernating first would only delay.
                        continue;
                    }
                    hibernate(shardId, lastUsed, nowMillis);
                }
            }
            if (reconciler.idleAfterMillis > 0) {
                for (WriterHibernation.Hibernated slept : reconciler.node.hibernation().all()) {
                    if (nowMillis - slept.lastUsedMillis() >= reconciler.idleAfterMillis && giveBack(slept)) {
                        released.add(slept.shardId());
                    }
                }
            }
            return released;
        }
    }

    private long lastUsed(ShardId shardId, long nowMillis) {
        return reconciler.node.reconciler().lastUsed(shardId).orElse(reconciler.openedAt.getOrDefault(shardId, nowMillis));
    }

    /**
     * Hibernates one writer, under its fence.
     *
     * @return true if it hibernated
     */
    boolean hibernate(ShardId shardId, long lastUsed, long nowMillis) {
        try {
            return reconciler.node.fences().underShardFence(shardId, () -> hibernateFenced(shardId, lastUsed, nowMillis));
        } catch (Exception e) {
            // Hibernating is an optimisation: a writer that could not hibernate stays open, and is tried again next pass.
            BackgroundReconciler.logger.warn("could not hibernate " + shardId, e);
            return false;
        }
    }

    private boolean hibernateFenced(ShardId shardId, long lastUsed, long nowMillis) throws Exception {
        final var shard = reconciler.node.reconciler().shard(shardId);
        if (shard == null || reconciler.node.reconciler().isReader(shardId) || reconciler.node.fences().isWriteFenced(shardId)) {
            // A write-fenced writer holds an operation its log does not: it is reopened from the log, never put to sleep.
            return false;
        }
        if (reconciler.node.reconciler().lastUsed(shardId).orElse(lastUsed) != lastUsed) {
            // Used while this waited for the fence.
            return false;
        }
        final long term = shard.getOperationPrimaryTerm();
        final var head = reconciler.plane.heads().read(shardId.getIndexName(), shardId.id());
        if (head.isEmpty()
            || reconciler.node.localNode().getId().equals(head.get().ownerNodeId()) == false
            || head.get().term() != term
            || (head.get().ownerEphemeralId() != null
                && reconciler.node.localNode().getEphemeralId().equals(head.get().ownerEphemeralId()) == false)
            || (head.get().indexUuid() != null && shardId.getIndex().getUUID().equals(head.get().indexUuid()) == false)) {
            // Not this process's head at this term: the head check that runs on every open writer will release it.
            return false;
        }
        // Everything acknowledged goes into a published commit first: the engine is about to close, and waking
        // opens from that commit at the same term without replaying anything.
        final long maxSeqNo = shard.seqNoStats().getMaxSeqNo();
        final boolean unchanged = maxSeqNo < 0 || maxSeqNo <= reconciler.lastPublishedMaxSeqNo.getOrDefault(shardId, -1L);
        org.opensearch.serverless.store.CommitManifest published = reconciler.lastManifests.get(shardId);
        synchronized (reconciler.publishLock(shardId)) {
            if (unchanged == false) {
                published = reconciler.node.shards().publishShard(shardId, term);
                reconciler.lastManifests.put(shardId, published);
            } else {
                // As idle release does: a commit staged and not yet in the register is checkpointed, so an open from the
                // register -- this node's on waking, or a successor's -- restores it rather than replaying the log.
                final var staged = reconciler.node.reconciler().checkpoint(shardId, false);
                if (staged != null) {
                    published = staged;
                    reconciler.lastManifests.put(shardId, staged);
                }
            }
        }
        // The commit's small files into the block cache, from this node's own copies, so waking reads them from memory.
        warm(shard, published);
        final WriterHibernation.Hibernated slept = new WriterHibernation.Hibernated(shardId, term, lastUsed, nowMillis);
        // Recorded before the close: a request that arrives while the engine closes finds the shard hibernated and
        // wakes it, rather than taking over a head this node still holds.
        reconciler.node.hibernation().put(slept);
        try {
            reconciler.node.shards().releaseShard(shardId, "hibernating after " + (nowMillis - lastUsed) + "ms unused");
        } catch (RuntimeException e) {
            reconciler.node.hibernation().undo(slept);
            throw e;
        }
        reconciler.openedAt.remove(shardId);
        reconciler.lastPublishedMaxSeqNo.remove(shardId);
        reconciler.lastPublishedAtMillis.remove(shardId);
        // A demand-driven node wakes it on the next request for it; wanting it would wake it on the next pass.
        reconciler.wanted.remove(Map.entry(shardId.getIndexName(), shardId.id()));
        // Unmarked, as a released writer is: nothing it holds is outside the published commit, so a search may rule the
        // shard out from its digest, and a reader has no tail to ask a closed writer for. Waking marks it again.
        try {
            final Optional<org.opensearch.serverless.store.CommitManifest> commit = published != null && published.term() == term
                ? Optional.of(published)
                : reconciler.plane.segmentPublisher(shardId.getIndexName(), shardId.getIndex().getUUID(), shardId.id()).readManifest();
            reconciler.plane.rollups()
                .clearOwned(
                    shardId.getIndexName(),
                    shardId.getIndex().getUUID(),
                    shardId.id(),
                    term,
                    commit.map(org.opensearch.serverless.store.CommitManifest::digest).orElse(null)
                );
        } catch (Exception e) {
            BackgroundReconciler.logger.debug("could not clear the writer mark of hibernated " + shardId, e);
        }
        BackgroundReconciler.logger.debug("hibernated {} at term {}, unused for {}ms", shardId, term, nowMillis - lastUsed);
        return true;
    }

    /** What a writer going to sleep puts into the block cache at most: an open's metadata, not its postings. */
    static final long WARM_BYTES = 1L << 20;

    /**
     * Puts the small files of a writer's published commit into the block cache before it closes. Best effort: a miss
     * later costs a read from the store, which is what it cost before.
     */
    static void warm(org.opensearch.index.shard.IndexShard shard, org.opensearch.serverless.store.CommitManifest published) {
        if (published == null) {
            return;
        }
        try {
            final var directory = org.opensearch.serverless.store.BlockCacheDirectory.unwrap(shard.store().directory());
            if (directory != null) {
                directory.warmFromLocal(published, WARM_BYTES);
            }
        } catch (Exception e) {
            BackgroundReconciler.logger.debug("could not warm the block cache with " + shard.shardId() + "'s published commit", e);
        }
    }

    /**
     * Gives back the head of a hibernated writer, as idle release gives back an open one's.
     *
     * @return true if the head was given back here
     */
    boolean giveBack(WriterHibernation.Hibernated slept) {
        final ShardId shardId = slept.shardId();
        if (reconciler.node.hibernation().released(slept) == false) {
            // Woken meanwhile.
            return false;
        }
        try {
            final boolean given = reconciler.plane.heads()
                .release(
                    shardId.getIndexName(),
                    shardId.id(),
                    reconciler.node.localNode().getId(),
                    slept.term(),
                    shardId.getIndex().getUUID(),
                    reconciler.node.localNode().getEphemeralId()
                );
            reconciler.plane.forgetAssignment(reconciler.node.localNode().getId(), shardId.getIndexName(), shardId.id());
            return given;
        } catch (Exception e) {
            // The head goes on naming this node with nothing open: the next request for it takes it the ordinary way, at
            // a higher term, as a writer closed after a failed engine is.
            BackgroundReconciler.logger.warn("could not give back the head of hibernated " + shardId, e);
            return false;
        }
    }

    /**
     * Gives back the head of every hibernated writer, for a clean shutdown.
     *
     * @return how many were given back
     */
    int giveBackAll() {
        int given = 0;
        for (WriterHibernation.Hibernated slept : reconciler.node.hibernation().all()) {
            if (giveBack(slept)) {
                given++;
            }
        }
        return given;
    }
}
