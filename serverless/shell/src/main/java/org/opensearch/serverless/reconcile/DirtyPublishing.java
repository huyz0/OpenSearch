/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.reconcile;

import org.opensearch.core.index.shard.ShardId;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Publishes {@link BackgroundReconciler}'s dirty writers: eagerly, or lazily by age and operation count.
 */
final class DirtyPublishing {

    private final BackgroundReconciler reconciler;

    DirtyPublishing(BackgroundReconciler reconciler) {
        this.reconciler = reconciler;
    }

    /**
     * Publishes every open shard that is due by {@link BackgroundReconciler#setLazyPublish}.
     *
     * @param nowMillis the plane's clock, now
     * @return the shards published
     * @throws Exception if the metadata plane cannot be reached
     */
    public Set<ShardId> publishAllDue(long nowMillis) throws Exception {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return Set.of();
            }
            final Set<ShardId> due = new LinkedHashSet<>();
            for (ShardId shardId : reconciler.node.reconciler().openShards()) {
                if (publishDue(shardId, nowMillis, 0L, 0L)) {
                    due.add(shardId);
                }
            }
            return publish(due);
        }
    }

    /**
     * Publishes every open shard whose commit has moved on.
     *
     * <p>The backstop's publish: it looks at everything rather than trusting that a write edge fired.
     *
     * @return the shards published
     * @throws Exception if the metadata plane cannot be reached
     */
    public Set<ShardId> publishAll() throws Exception {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return Set.of();
            }
            return publish(reconciler.node.reconciler().openShards());
        }
    }

    /**
     * Publishes only the shards a write has been reported on since the last publish.
     *
     * <p>The edge's publish. Draining the marks before doing the work rather than after is deliberate:
     * a write that lands mid-publish re-marks the shard and gets its own pass, where clearing afterwards
     * would swallow it and leave the last writes of a burst unpublished until the backstop.
     *
     * @return the shards published
     * @throws Exception if the metadata plane cannot be reached
     */
    public Set<ShardId> publishDirty() throws Exception {
        return publishDirty(0L, 0L, 0L);
    }

    /**
     * Publishes the shards written to since their last publish that are due for another one.
     *
     * <p>A publish is a flush plus one upload per new segment file plus a manifest swap: five or more PUTs for
     * a commit that adds one small segment. On a shard taking a write every second or so, publishing on every
     * debounce window costs those five PUTs per write, several times the one log append the write itself paid.
     * Durability does not need it, because an acknowledged write is already in the log. What a publish buys is
     * reader freshness and a shorter log to replay on takeover, and both are well served by publishing a busy
     * shard every few seconds rather than every few hundred milliseconds.
     *
     * <p>So a shard is due when it has never been published by this node, when {@code minIntervalMillis} has
     * passed since its last publish, or when it holds at least {@code maxUnpublishedOperations} operations its
     * last publish did not cover, which bounds the replay a successor inherits whatever the write rate. A shard
     * that is not yet due keeps its mark and is looked at again on the next pass.
     *
     * @param nowMillis the plane's clock, now
     * @param minIntervalMillis the least time between two publishes of one shard; zero or less publishes every
     *     marked shard, as {@link #publishDirty()} does
     * @param maxUnpublishedOperations how many unpublished operations make a shard due early; zero or less to
     *     never publish early
     * @return the shards published
     * @throws Exception if the metadata plane cannot be reached
     */
    public Set<ShardId> publishDirty(long nowMillis, long minIntervalMillis, long maxUnpublishedOperations) throws Exception {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return Set.of();
            }
            final Set<ShardId> due = new LinkedHashSet<>();
            for (ShardId shardId : reconciler.dirty) {
                // Unmarked one at a time, as each is found due: a write that re-marks a shard after this point keeps
                // its mark and gets its own pass, where removing them all at the end would erase it.
                if (publishDue(shardId, nowMillis, minIntervalMillis, maxUnpublishedOperations) && reconciler.dirty.remove(shardId)) {
                    due.add(shardId);
                }
            }
            return publish(due);
        }
    }

    /**
     * Whether any marked shard is due for a publish now, by the rule {@link #publishDirty(long, long, long)}
     * applies.
     *
     * @param nowMillis the plane's clock, now
     * @param minIntervalMillis the least time between two publishes of one shard
     * @param maxUnpublishedOperations how many unpublished operations make a shard due early
     * @return true if a pass now would publish something
     */
    public boolean hasDirtyDue(long nowMillis, long minIntervalMillis, long maxUnpublishedOperations) {
        for (ShardId shardId : reconciler.dirty) {
            if (publishDue(shardId, nowMillis, minIntervalMillis, maxUnpublishedOperations)) {
                return true;
            }
        }
        return false;
    }

    boolean publishDue(ShardId shardId, long nowMillis, long minIntervalMillis, long maxUnpublishedOperations) {
        final long maxAge = reconciler.lazyPublishMaxAgeMillis;
        if (maxAge > 0) {
            return lazyPublishDue(shardId, nowMillis, maxAge);
        }
        if (minIntervalMillis <= 0) {
            return true;
        }
        final Long lastAt = reconciler.lastPublishedAtMillis.get(shardId);
        if (lastAt == null || nowMillis - lastAt >= minIntervalMillis) {
            return true;
        }
        if (maxUnpublishedOperations <= 0) {
            return false;
        }
        final var shard = reconciler.node.reconciler().shard(shardId);
        if (shard == null) {
            // Closed since it was marked; publishOne drops it without a store request.
            return true;
        }
        final long maxSeqNo;
        try {
            maxSeqNo = shard.seqNoStats().getMaxSeqNo();
        } catch (RuntimeException e) {
            // An engine closing or resetting. Due, so the publish finds out and re-marks it on its own, rather than
            // this one shard failing the whole pass for every other.
            return true;
        }
        return maxSeqNo - reconciler.lastPublishedMaxSeqNo.getOrDefault(shardId, -1L) >= maxUnpublishedOperations;
    }

    boolean lazyPublishDue(ShardId shardId, long nowMillis, long maxAge) {
        final var shard = reconciler.node.reconciler().shard(shardId);
        if (shard == null) {
            // Closed since it was marked; publishOne drops it without a store request.
            return true;
        }
        final long maxSeqNo;
        try {
            maxSeqNo = shard.seqNoStats().getMaxSeqNo();
        } catch (RuntimeException e) {
            return true;
        }
        final Long lastThrough = reconciler.lastPublishedMaxSeqNo.get(shardId);
        if (maxSeqNo < 0 || (lastThrough != null && maxSeqNo <= lastThrough)) {
            // Nothing unpublished.
            reconciler.unpublishedSinceMillis.remove(shardId);
            return false;
        }
        if (lastThrough == null) {
            // Never published by this node: once, so a new index is searchable at once and the commit records where
            // this node's writes begin.
            return true;
        }
        // A write reported while a publish was in flight may have lost its stamp; it is aged from now.
        final long since = reconciler.unpublishedSinceMillis.computeIfAbsent(shardId, ignored -> nowMillis);
        final long age = publishAgeOf(shard, maxAge);
        if (age >= 0) {
            final long tick = reconciler.lazyPublishTickMillis;
            final long dueAt = since + age;
            final long alignedDueAt = dueAt % tick == 0 ? dueAt : (dueAt / tick + 1) * tick;
            if (nowMillis >= alignedDueAt) {
                return true;
            }
        }
        final long maxOperations = reconciler.lazyPublishMaxOperations;
        return maxOperations > 0 && maxSeqNo - lastThrough >= maxOperations;
    }

    /** How old a shard's oldest unpublished write may get: its index's refresh interval, or the node's default. */
    static long publishAgeOf(org.opensearch.index.shard.IndexShard shard, long defaultMillis) {
        final org.opensearch.index.IndexSettings settings = shard.indexSettings();
        if (org.opensearch.index.IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.exists(settings.getSettings()) == false) {
            return defaultMillis;
        }
        final long refresh = settings.getRefreshInterval().millis();
        // Refresh turned off: published by size or on release only.
        return refresh < 0 ? -1L : refresh;
    }

    /**
     * Reports that a shard has been written to and should be published soon.
     *
     * @param shardId the shard
     */
    public void markDirty(ShardId shardId) {
        if (reconciler.activity.isClosed()) {
            return;
        }
        reconciler.dirty.add(shardId);
        if (reconciler.lazyPublishMaxAgeMillis > 0) {
            reconciler.unpublishedSinceMillis.putIfAbsent(shardId, reconciler.plane.clock().getAsLong());
        }
    }

    Set<ShardId> publish(Collection<ShardId> candidates) throws Exception {
        final Set<ShardId> published = java.util.concurrent.ConcurrentHashMap.newKeySet();
        final java.util.concurrent.ExecutorService pool = publishPool();
        if (pool == null || candidates.size() <= 1) {
            for (ShardId shardId : candidates) {
                if (publishOne(shardId)) {
                    published.add(shardId);
                }
            }
            return published;
        }
        // Several at once, on a pool of their own. One after another, a node holding a thousand shards took minutes to get
        // round to the last dirty one -- its readers that far behind its writes -- and the passes started meanwhile piled
        // up on GENERIC, blocked on the same shards, until the writes waiting on GENERIC for an activation or a forward
        // starved and the write limiter refused them.
        final List<java.util.concurrent.Future<?>> running = new java.util.ArrayList<>();
        for (ShardId shardId : candidates) {
            running.add(pool.submit(() -> {
                if (publishOne(shardId)) {
                    published.add(shardId);
                }
            }));
        }
        for (java.util.concurrent.Future<?> each : running) {
            try {
                each.get();
            } catch (java.util.concurrent.ExecutionException e) {
                BackgroundReconciler.logger.warn("a publish failed outside its own handling", e.getCause());
            }
        }
        return published;
    }

    java.util.concurrent.ExecutorService publishPool() {
        try {
            return reconciler.node.threadPool().executor(org.opensearch.serverless.shell.ServerlessNode.PUBLISH_POOL);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** Whether any shard has been written to since it was last published. */
    public boolean hasDirty() {
        return reconciler.dirty.isEmpty() == false;
    }

    /** Publishes one shard if it has anything new; true if it did. */
    boolean publishOne(ShardId shardId) {
        if (reconciler.node.reconciler().isReader(shardId)) {
            return false;
        }
        final var shard = reconciler.node.reconciler().shard(shardId);
        if (shard == null) {
            return false;
        }
        final long maxSeqNo = shard.seqNoStats().getMaxSeqNo();
        // What the commit about to be flushed is certain to hold: every operation at or below the processed
        // checkpoint, read before the flush. Not the highest sequence number assigned -- an operation can be
        // assigned one and still be on its way into the index when the flush commits, and recording its
        // number as published let a later release skip the publish that would have carried it, leaving an
        // acknowledged write readable only after someone replayed the log.
        final long covered = shard.seqNoStats().getLocalCheckpoint();
        if (maxSeqNo < 0 || maxSeqNo <= reconciler.lastPublishedMaxSeqNo.getOrDefault(shardId, -1L)) {
            // Nothing new. Publishing anyway would flush a fresh commit and upload it every tick,
            // forever, on an idle shard -- an object-store bill for saying nothing happened. This
            // guard stays on the edge path too: a spurious mark must not become an upload.
            return false;
        }
        if (reconciler.node.isWriteFenced(shardId)) {
            // Its engine holds an operation its log does not. Publishing that commit would make the
            // failure the caller was told a lie; the shard is reopened from its log by the next
            // heartbeat that reaches the store, and published after.
            return false;
        }
        try {
            // The heartbeat read this head a moment ago; the manifest's own compare-and-swap is the
            // fence that matters, so a head up to one renewal old is as good here as a fresh read.
            final var head = reconciler.leases.recentOrReadHead(shardId);
            if (head.isEmpty() || reconciler.node.localNode().getId().equals(head.get().ownerNodeId()) == false) {
                return false;
            }
            synchronized (reconciler.publishLock(shardId)) {
                reconciler.lastManifests.put(shardId, reconciler.node.publishShard(shardId, head.get().term()));
            }
        } catch (org.opensearch.serverless.store.StaleWriterException e) {
            // A newer term has already published. This node is a zombie for this shard: it read a
            // head that named it, and by the time it wrote, it did not. Stop serving it now rather
            // than at the next renewal, and go look at ownership -- this is exactly the evidence
            // ownershipDoubted exists for. Under the fence, so a write in flight is not acknowledged
            // against a shard that is being closed as fenced.
            try {
                reconciler.node.underShardFence(shardId, () -> {
                    reconciler.node.reconciler().releaseShard(shardId, "fenced while publishing: " + e.getMessage());
                    return null;
                });
            } catch (Exception releaseFailure) {
                BackgroundReconciler.logger.warn("could not release " + shardId + " after a fenced publish", releaseFailure);
            }
            reconciler.lastPublishedMaxSeqNo.remove(shardId);
            reconciler.lastPublishedAtMillis.remove(shardId);
            reconciler.fenced.add(shardId);
            return false;
        } catch (Exception e) {
            // One shard's upload failing must not defer every other dirty shard to the backstop:
            // the marks were drained before this loop began, so an exception out of it silently left
            // the rest unpublished for up to a backstop interval. This shard is re-marked and the
            // loop goes on.
            BackgroundReconciler.logger.warn("could not publish " + shardId + "; it is re-marked and will be retried", e);
            reconciler.dirty.add(shardId);
            return false;
        }
        reconciler.lastPublishedMaxSeqNo.put(shardId, covered);
        reconciler.lastPublishedAtMillis.put(shardId, reconciler.plane.clock().getAsLong());
        // A write reported since the flush read its checkpoint is stamped again by the next look.
        reconciler.unpublishedSinceMillis.remove(shardId);
        return true;
    }

    /**
     * Returns and clears the shards this reconciler was fenced out of while publishing.
     *
     * <p>Drained rather than read so a scheduler acting on it cannot act on the same fencing twice.
     *
     * @return the shards fenced since the last call
     */
    public Set<ShardId> drainFenced() {
        final Set<ShardId> drained = Set.copyOf(reconciler.fenced);
        reconciler.fenced.removeAll(drained);
        return drained;
    }
}
