/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.reconcile;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.shard.ShardReconciler;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Sweeps for {@link BackgroundReconciler}: stale readers, published files nothing references, expired views,
 * tombstones, orphan slices and reclaimed indices.
 */
final class PublishedSweeps {

    private final BackgroundReconciler reconciler;

    PublishedSweeps(BackgroundReconciler reconciler) {
        this.reconciler = reconciler;
    }

    /**
     * Lets go of readers whose commit has moved on, so nothing serves a stale one forever.
     *
     * <p><b>A reader is a cache of one commit, and until this existed nothing invalidated it.</b> A search
     * node that opened shard 0 went on answering from the commit it opened for as long as it held the
     * shard, however much the writer published afterwards — no error, no flag, no gap in the coverage it
     * reported. That is the confident wrong answer this design refuses everywhere else, and it was the one
     * place it was being given.
     *
     * <p><b>Release, not reopen.</b> Moving an open reader onto a newer commit would mean handing a live
     * {@code ReadOnlyEngine} a different commit, which core does not offer and should not. Releasing costs
     * the next search an open — and the next search is the only thing that proves the shard is still
     * wanted at all, so a reader nobody comes back for costs one release and then nothing.
     *
     * <p><b>The cost is one register read per reader per pass</b>, which is the price the design already
     * pays to hold a shard at all, and it acts only when the commit has actually changed: an idle index
     * publishes nothing, so nothing is given up and nothing is reopened.
     *
     * <p>Frozen views are exempt, and that is not an optimisation. A view exists precisely to go on serving
     * a commit the writer has moved past; refreshing one would be the feature deleting itself.
     *
     * @return the readers released
     */
    public Set<ShardId> refreshReaders() {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return Set.of();
            }
            final Set<ShardId> stale = new LinkedHashSet<>();
            final Set<ShardId> views = reconciler.node.reconciler().frozenShards();
            for (ShardId shardId : reconciler.node.reconciler().readerShards()) {
                if (views.contains(shardId)) {
                    continue;
                }
                final var opened = reconciler.node.reconciler().readerCommit(shardId);
                if (opened.isEmpty()) {
                    continue;
                }
                if (reconciler.node.reconciler().inFlight(shardId) > 0) {
                    // A query is running against it. Closing the reader under a running aggregation failed
                    // the query with a closed reader, which looks like corruption and is a scheduling choice;
                    // the next pass finds the commit still superseded and tries again.
                    continue;
                }
                try {
                    final var current = reconciler.plane.segmentPublisher(
                        shardId.getIndexName(),
                        shardId.getIndex().getUUID(),
                        shardId.id()
                    ).readManifest();
                    if (current.isEmpty()) {
                        // The commit this reader is serving is no longer published at all. A deleted index,
                        // most likely -- but a manifest read that came back empty is not proof of that on its
                        // own, so the descriptor decides: gone too, and the reader is let go with the records
                        // this node kept of it; still there, and the next search will say what is true.
                        if (reconciler.plane.describe(shardId.getIndexName()).isEmpty()
                            && reconciler.node.reconciler()
                                .releaseReader(
                                    shardId,
                                    opened.get(),
                                    "its index has been deleted"
                                ) == ShardReconciler.ReaderRelease.RELEASED) {
                            reconciler.node.readers().forgetReader(shardId);
                            stale.add(shardId);
                        }
                        continue;
                    }
                    if (sameCommit(opened.get(), current.get())) {
                        continue;
                    }
                    // Only if it is still the reader judged above. The manifest read is a round trip, and a write during
                    // it can make this node the shard's writer under the same id: closing that would leave its head
                    // naming a node with nothing open, answering "retry" for as long as the node lives.
                    if (reconciler.node.reconciler()
                        .releaseReader(
                            shardId,
                            opened.get(),
                            "the commit it was serving has been superseded"
                        ) != ShardReconciler.ReaderRelease.RELEASED) {
                        continue;
                    }
                    // And the node's own record of serving it, or a search node that cycles through readers
                    // carries every one it ever opened into every reader open for the rest of its life.
                    reconciler.node.readers().forgetReader(shardId);
                    stale.add(shardId);
                } catch (Exception e) {
                    // Refreshing is an optimisation over being wrong, not a correctness step of its own: a
                    // failure here leaves the reader exactly where it was, which is where it would have been
                    // if this method did not exist.
                    BackgroundReconciler.logger.warn("could not check whether the reader for " + shardId + " is stale; keeping it", e);
                }
            }
            return stale;
        }
    }

    /**
     * Decides whether a sweep of a shard can find anything a previous sweep did not.
     *
     * <p>A file becomes unreferenced when a manifest stops naming it -- a merge or a delete -- and at no
     * other moment this node can see. So a shard is swept when its newest manifest lacks a file the
     * last sweep's manifest had, when it has candidates on watch for the grace period, when a reaped
     * view may have freed something, or when this node has never swept it since opening it. A publish
     * that only added segments, which is most of them, costs no listing.
     */
    boolean sweepCanFindSomething(ShardId shardId) {
        if (reconciler.forceSweep.remove(shardId) || reconciler.unreferencedFor.containsKey(shardId)) {
            return true;
        }
        final Set<String> previous = reconciler.sweptAgainst.get(shardId);
        final org.opensearch.serverless.store.CommitManifest current = reconciler.lastManifests.get(shardId);
        if (previous == null || current == null) {
            return true;
        }
        for (String file : previous) {
            // Every commit replaces its segments_N file, so that one loss says nothing about a merge;
            // the tiny orphan it leaves is collected with the next real one.
            if (file.startsWith("segments_") == false && current.files().containsKey(file) == false) {
                return true;
            }
        }
        return false;
    }

    /**
     * Collects unreferenced segment blobs for the shards this node has just published.
     *
     * <p><b>Until this existed, nothing in a running deployment ever ran the collector.</b> It was reachable
     * from tests and from an operator, and the garbage a failover leaves — a zombie's unpublishable files,
     * segments a merge superseded — accumulated for the life of the deployment. Storage that only grows is
     * not a bug anybody is paged for, which is why it lasted this long.
     *
     * <p><b>Driven by publishing rather than by holding.</b> A shard that nobody is writing to produces no
     * garbage, so sweeping every held shard on a schedule would pay a listing per shard per pass to be told
     * nothing had changed. This sweeps what was just published, and what a previous sweep is still watching
     * — so the cost follows write activity, and an idle deployment pays nothing.
     *
     * <p><b>And only shards this node owns.</b> Two nodes sweeping one shard is not unsafe — the rule is
     * the same for both — but the owner is the only node that knows the shard is not mid-publish somewhere,
     * and the owner is the node whose publishing created the garbage.
     *
     * @param published the shards published by this pass
     * @return the blobs deleted, qualified by term container
     */
    public Set<String> sweepPublished(Collection<ShardId> published) {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return Set.of();
            }
            final Set<String> deleted = new LinkedHashSet<>();
            if (reconciler.plane.blobStore() == null) {
                return deleted;
            }
            final Set<ShardId> toSweep = new LinkedHashSet<>();
            for (ShardId shardId : published) {
                if (sweepCanFindSomething(shardId)) {
                    toSweep.add(shardId);
                }
            }
            toSweep.addAll(reconciler.unreferencedFor.keySet());
            if (toSweep.isEmpty()) {
                // Nothing published that lost a file, and nothing on watch: this pass costs no listing at all.
                return deleted;
            }
            // The views and snapshots every shard's sweep checks against, read once per pass rather than once
            // per shard: at a hundred published shards and a thousand views that was a hundred thousand reads
            // a pass.
            final java.util.List<org.opensearch.serverless.metadata.PointInTime> views;
            final java.util.List<org.opensearch.serverless.metadata.SnapshotRecord> snapshots;
            try {
                views = reconciler.plane.livePointsInTime(reconciler.plane.clock().getAsLong());
                snapshots = reconciler.plane.liveSnapshots();
            } catch (Exception e) {
                BackgroundReconciler.logger.warn(
                    "could not read the views and snapshots a sweep must respect; sweeping nothing this pass",
                    e
                );
                return deleted;
            }
            for (ShardId shardId : toSweep) {
                if (reconciler.node.reconciler().openShards().contains(shardId) == false
                    || reconciler.node.reconciler().isReader(shardId)) {
                    // Not ours to sweep any more. Forgetting what we had seen is the safe direction: the next
                    // owner starts again from nothing and simply deletes later than it could have.
                    reconciler.unreferencedFor.remove(shardId);
                    reconciler.sweptAgainst.remove(shardId);
                    reconciler.lastManifests.remove(shardId);
                    continue;
                }
                final Map<String, Integer> seen = reconciler.unreferencedFor.getOrDefault(shardId, Map.of());
                // Null means "everything unreferenced, now". Building the eligible set from what a previous
                // sweep saw would make a grace of zero delete nothing on the first sweep and everything on the
                // second -- a knob that looks like it is off and is really set to one.
                Set<String> eligible = null;
                if (reconciler.sweepGracePasses > 0) {
                    eligible = new java.util.HashSet<>();
                    for (Map.Entry<String, Integer> entry : seen.entrySet()) {
                        if (entry.getValue() >= reconciler.sweepGracePasses) {
                            eligible.add(entry.getKey());
                        }
                    }
                }
                try {
                    final var swept = reconciler.collector.sweepShard(
                        reconciler.plane,
                        shardId.getIndexName(),
                        shardId.id(),
                        eligible,
                        views,
                        snapshots
                    );
                    final var manifest = reconciler.lastManifests.get(shardId);
                    if (manifest != null) {
                        reconciler.sweptAgainst.put(shardId, Set.copyOf(manifest.files().keySet()));
                    }
                    deleted.addAll(swept.deleted());
                    final Map<String, Integer> next = new java.util.HashMap<>();
                    for (String candidate : swept.candidates()) {
                        next.put(candidate, seen.getOrDefault(candidate, 0) + 1);
                    }
                    if (next.isEmpty()) {
                        reconciler.unreferencedFor.remove(shardId);
                    } else {
                        reconciler.unreferencedFor.put(shardId, next);
                    }
                } catch (Exception e) {
                    // Reclaiming space is the one job here whose failure costs only money, so it never
                    // interrupts a pass and never escalates.
                    BackgroundReconciler.logger.warn("could not sweep " + shardId + "; the next pass will try again", e);
                }
            }
            return deleted;
        }
    }

    /**
     * Removes frozen views that have expired, and closes what this node was holding for them.
     *
     * <p><b>Expiry existed as an answer to a request and not as a thing that happened.</b> A search
     * quoting a view past its keep-alive was refused, which is the visible half; the record stayed in the
     * object store pinning its blobs against the sweep, and the shards a node had opened for it stayed
     * open — counted against the node's bound, never a candidate for eviction, serving a commit nobody
     * could still ask for. A keep-alive that only stops answering is not a keep-alive.
     *
     * <p>Every node runs this, and that is safe because it is idempotent: deleting a record twice is
     * deleting it once, and the second node's list simply comes back shorter.
     *
     * <p>Closing local shards is done by asking what this node holds rather than by acting on what this
     * pass deleted — another node may have reaped the record, and this node's shards would then be held
     * against a view that no longer exists anywhere.
     *
     * @return how many records were removed by this pass
     */
    public int reapExpiredViews() {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return 0;
            }
            int reaped = 0;
            try {
                reaped = reconciler.plane.reapPointsInTime(reconciler.plane.clock().getAsLong());
            } catch (Exception e) {
                BackgroundReconciler.logger.warn("could not reap expired points in time; the next pass will retry", e);
            }
            closeReleasedViews();
            return reaped;
        }
    }

    /**
     * Starts a tombstone sweep on the generic pool, unless one is still running.
     *
     * <p>Off the pass because its length is set by how much was deleted, and the pass is on a fixed delay:
     * run inline, a node draining a backlog of deletions held up its own idle release, backstop publish,
     * view reaping and resync for as long as the drain took. Single-flight, so a drain that outlasts the
     * interval is not joined by a second one over the same buckets.
     */
    void sweepTombstonesOffThePass(String selfId) {
        if (reconciler.sweepingTombstones.compareAndSet(false, true) == false) {
            return;
        }
        try {
            reconciler.tombstoneSweep = reconciler.node.threadPool().generic().submit(() -> {
                try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
                    if (work == null) {
                        return;
                    }
                    reconciler.collector.collectTombstones(reconciler.plane, reconciler.plane.clock().getAsLong(), selfId);
                } catch (Exception e) {
                    BackgroundReconciler.logger.warn("could not sweep descriptor tombstones; the next sweep will retry", e);
                } finally {
                    reconciler.sweepingTombstones.set(false);
                }
            });
        } catch (RuntimeException e) {
            reconciler.sweepingTombstones.set(false);
            BackgroundReconciler.logger.warn("could not start a tombstone sweep; the next pass that is due will try again", e);
        }
    }

    void sliceOrphansOffThePass() {
        if (reconciler.slicingOrphans.compareAndSet(false, true) == false) {
            return;
        }
        try {
            reconciler.orphanSlice = reconciler.node.threadPool().generic().submit(() -> {
                try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
                    if (work == null) {
                        return;
                    }
                    final GarbageCollector.OrphanSlice slice = reconciler.collector.collectOrphanedShards(
                        reconciler.plane,
                        reconciler.orphanSliceBudget
                    );
                    if (slice.deleted().isEmpty() == false) {
                        BackgroundReconciler.logger.info("the orphan sweep collected {} shard containers", slice.deleted().size());
                    }
                } catch (Exception e) {
                    BackgroundReconciler.logger.warn(
                        "could not sweep a slice of orphaned shards; the cursor stays and the next slice retries",
                        e
                    );
                } finally {
                    reconciler.slicingOrphans.set(false);
                }
            });
        } catch (RuntimeException e) {
            reconciler.slicingOrphans.set(false);
            BackgroundReconciler.logger.warn("could not start an orphan slice; the next one that is due will try again", e);
        }
    }

    /** Drains the due reclaim intents this node owns on the generic pool, unless a drain is still running. */
    void reclaimOffThePass(String selfId) {
        if (reconciler.reclaiming.compareAndSet(false, true) == false) {
            return;
        }
        try {
            reconciler.reclaimSweep = reconciler.node.threadPool().generic().submit(() -> {
                try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
                    if (work == null) {
                        return;
                    }
                    reconciler.collector.collectReclaims(reconciler.plane, reconciler.plane.clock().getAsLong(), selfId);
                } catch (Exception e) {
                    BackgroundReconciler.logger.warn("could not drain the reclaim queue; the next reap will retry", e);
                } finally {
                    reconciler.reclaiming.set(false);
                }
            });
        } catch (RuntimeException e) {
            reconciler.reclaiming.set(false);
            BackgroundReconciler.logger.warn("could not start draining the reclaim queue; the next reap will try again", e);
        }
    }

    /**
     * Closes the frozen views this node is holding whose record has gone.
     *
     * <p>Separate from the reap because it is the half with no listing in it: it asks only about views this
     * node has open, so a node holding none does nothing and pays nothing.
     *
     * @return how many views were closed
     */
    public int closeReleasedViews() {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return 0;
            }
            int closed = 0;
            // One record read per view, not per frozen shard: a hundred-shard view was a hundred identical
            // reads every pass on every node holding it.
            final Set<String> viewIds = new LinkedHashSet<>();
            for (ShardId view : reconciler.node.reconciler().frozenShards()) {
                viewIds.add(view.getIndex().getUUID());
            }
            for (String viewId : viewIds) {
                try {
                    final var record = reconciler.plane.pointInTime(viewId);
                    if (record.isEmpty() || record.get().expiredAt(reconciler.plane.clock().getAsLong())) {
                        closed += reconciler.node.reconciler().closeFrozenReader(viewId);
                    }
                } catch (Exception e) {
                    BackgroundReconciler.logger.warn(
                        "could not check whether the view " + viewId + " is still held; keeping its shards",
                        e
                    );
                }
            }
            return closed;
        }
    }

    /**
     * Compares two commits by what they name rather than by identity.
     *
     * <p>The term alone is not enough: a writer publishes many commits at one term, which is the ordinary
     * case rather than an edge one. The file set alone is not enough either — a failover can produce the
     * same file names in a different term container, and those are different bytes.
     */
    static boolean sameCommit(org.opensearch.serverless.store.CommitManifest a, org.opensearch.serverless.store.CommitManifest b) {
        return a.term() == b.term() && a.files().equals(b.files());
    }
}
