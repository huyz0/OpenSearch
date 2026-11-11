/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.reconcile;

import org.opensearch.core.index.shard.ShardId;

import java.util.Set;

/**
 * Runs one of {@link BackgroundReconciler}'s passes: renews, sweeps, releases, activates and publishes, each on its
 * own cadence.
 */
final class ReconcilePass {

    private final BackgroundReconciler reconciler;

    ReconcilePass(BackgroundReconciler reconciler) {
        this.reconciler = reconciler;
    }

    /**
     * Runs one full reconciliation pass: the backstop.
     *
     * <p>This is what makes the edges in {@link ReconcileSignals} safe to be best-effort. Every job runs
     * here regardless of whether its signal ever fired, so a lost, dropped or never-sent signal costs
     * latency and never correctness.
     *
     * @param nowMillis the observer's clock, for hint staleness
     * @return what the pass did
     * @throws Exception if the metadata plane cannot be reached
     */
    public TickResult tick(long nowMillis) throws Exception {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return new TickResult(Set.of(), Set.of(), Set.of(), 0);
            }
            final Set<ShardId> released = new java.util.LinkedHashSet<>(reconciler.renewLeases());
            final Set<ShardId> activated = new java.util.LinkedHashSet<>(reconciler.activateWanted());
            final Set<ShardId> published = reconciler.lazyPublishMaxAgeMillis > 0
                ? reconciler.publishing.publishAllDue(nowMillis)
                : reconciler.publishAll();
            // After publishing, so a shard released for idleness has just had its chance to flush.
            released.addAll(reconciler.releaseIdle(nowMillis));
            reconciler.trimToCap();
            reconciler.handOffToMemberWithRoom();
            // And after that, so a reader this node has just let go of for idleness is not looked up again.
            released.addAll(reconciler.refreshReaders());
            // Views this node is holding for a record that has gone: in-memory to decide, so it costs nothing
            // on a node holding none, and it must not wait for the reaping cadence below.
            reconciler.sweeps.closeReleasedViews();
            // The deployment-wide reap is a listing, and every node would otherwise pay one every pass to be
            // told that a feature nobody used is still not being used. A keep-alive is minutes; this is
            // seconds times a small number, which is soon enough and is a fraction of the cost.
            if (reconciler.passes++ % BackgroundReconciler.REAP_EVERY_PASSES == 0) {
                if (reconciler.reapExpiredViews() > 0) {
                    // Files a reaped view was holding are deletable now, and nothing else would notice until
                    // the shard next lost a file of its own.
                    reconciler.forceSweepOfOpenWriters();
                }
                reconciler.reaps++;
                // And the pins this node did not touch: two listings, no reads, every few reaps. A view reaped
                // elsewhere or a snapshot deleted anywhere changes the set, and a changed set is the one signal
                // that files a quiet index held under that pin are collectable now rather than at its next
                // merge.
                if (reconciler.reaps % BackgroundReconciler.PINS_EVERY_REAPS == 0) {
                    try {
                        final Set<String> pins = reconciler.collector.pins();
                        if (reconciler.lastPins != null && pins.equals(reconciler.lastPins) == false) {
                            reconciler.forceSweepOfOpenWriters();
                        }
                        reconciler.lastPins = pins;
                    } catch (Exception e) {
                        BackgroundReconciler.logger.warn("could not read the deployment's pins; the sweep gate keeps its last reading", e);
                    }
                }
                // Descriptor tombstones past their quarantine, rarely: a name created and deleted daily used
                // to poison its prefix pattern after a year, because nothing ever removed them. Only the
                // buckets this node owns, so the deployment pays once per deleted name, not once per node.
                final org.opensearch.cluster.node.DiscoveryNode self = reconciler.node.localNode();
                if (reconciler.reaps % BackgroundReconciler.TOMBSTONE_SWEEP_EVERY_REAPS == 0 && self != null) {
                    reconciler.sweeps.sweepTombstonesOffThePass(self.getId());
                }
                // And one slice of the orphaned-shard sweep, as rarely: a backstop for shard data whose index
                // is gone and whose reclaim never ran. The cursor is shared, so every node's slices advance one
                // cycle, and a slice costs its budget whatever the deployment's size.
                if (reconciler.reaps % BackgroundReconciler.ORPHAN_SLICE_EVERY_REAPS == BackgroundReconciler.ORPHAN_SLICE_EVERY_REAPS / 2
                    && reconciler.orphanSliceBudget > 0) {
                    reconciler.sweeps.sliceOrphansOffThePass();
                }
                // And the deletes whose reclaim intents have fallen due, every reap: an intent is due minutes
                // after its delete, not hours, and the queue lists only the buckets that are.
                if (self != null) {
                    reconciler.sweeps.reclaimOffThePass(self.getId());
                }
            }
            // One small register read: the stored scripts are re-read only when their marker has moved, which
            // is how a script put on another node reaches this one without a listing per pass.
            reconciler.node.storedScripts().refresh(false);
            // The full re-read of this node's truth, rarely. Activation no longer goes through it -- taking
            // one shard used to re-read every held head -- so this is what reopens a shard whose head still
            // names this node but which is not open here: one closed after a failed log append, or one whose
            // open failed after the head was won. One listing and a head read per claim, every
            // BackgroundReconciler.RESYNC_EVERY_PASSES backstops, which at the default interval is once in ten minutes.
            if (reconciler.passes % BackgroundReconciler.RESYNC_EVERY_PASSES == BackgroundReconciler.RESYNC_EVERY_PASSES - 1) {
                try {
                    activated.addAll(reconciler.node.syncFrom(reconciler.plane));
                } catch (Exception e) {
                    BackgroundReconciler.logger.warn("periodic resync against the metadata plane failed; the next one will retry", e);
                }
            }
            // Only what was published, so the sweep costs nothing at all on a shard nobody is writing to.
            // Edge-triggered publishes are swept here too: only the backstop's result used to enter the
            // watch set, so a burst-then-idle shard kept its merged-away segments forever.
            published.addAll(reconciler.drainPendingSweeps());
            reconciler.sweepPublished(published);
            return new TickResult(released, activated, published, reconciler.leases.refreshHints(nowMillis));
        }
    }
}
