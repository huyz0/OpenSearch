/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.storage.clone.ShardCloner;
import org.opensearch.serverless.storage.scheduling.JitteredScheduling;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;

/**
 * Schedules {@link PinLedgerSweeper} to run periodically for one index, the same
 * "create and immediately schedule" shape {@link PitrRetentionSchedulerTask} already uses.
 *
 * <h2>Why this is per-index rather than fleet-wide</h2>
 *
 * A ledger lives in its index's shard 0 container, and this task is constructed wherever this
 * node's own knowledge of "shard 0 of this index, plus a resolver for its other shards" already
 * exists -- see {@code ServerlessStoragePlugin#getEngineFactory}, the only place that owns both. It
 * is deliberately not driven from a fleet-wide registry the way {@code GcCandidateTailer} or {@code
 * DescriptorChangeTailer} are: there is no existing fleet-wide index of "which indices have a pin
 * ledger" to drive one from, and building one is out of this item's own scope (round 006's plan
 * calls this out as the reason item 6 was left unbuilt for as long as it was). The coverage this
 * gives is real but bounded: an index whose shard 0 has been opened on this node, at any point since
 * the node started, gets its ledger swept on an interval; an index whose shard 0 has never opened
 * here does not. That is honest, node-local coverage, not fleet-wide sweeping -- stated here rather
 * than implied by the class existing at all.
 *
 * <h2>Why the task keeps running after shard 0 relocates away</h2>
 *
 * Sweeping is pure object-store I/O through {@link ShardCloner.ContainerResolver}, indifferent to
 * whether this node currently holds any shard of the index -- the same property that lets {@link
 * org.opensearch.serverless.storage.retention.action.TransportSnapshotPinAction} run without
 * node-specific routing. So a task started once is left running for the rest of this node's
 * lifetime rather than torn down and rebuilt on every shard relocation, which would cost a real
 * scheduling churn for no correctness benefit. The caller ({@code ServerlessStoragePlugin}) dedupes
 * by index uuid so a shard 0 that opens repeatedly (relocation, restart-in-place) does not start a
 * second task racing the first.
 */
public final class PinLedgerSweepTask implements Closeable {

    private static final Logger logger = LogManager.getLogger(PinLedgerSweepTask.class);

    /**
     * How often a pass runs. Longer than {@link PitrRetentionSchedulerTask#DEFAULT_RECONCILE_INTERVAL}:
     * an abandoned ledger is a bounded storage leak, not a correctness risk on the scale that
     * reconciliation is, so this can afford to be a much less frequent background pass.
     */
    public static final TimeValue DEFAULT_SWEEP_INTERVAL = TimeValue.timeValueMinutes(30);

    /**
     * How old a still-live ledger has to be before a pass logs it as worth an operator's attention.
     * Long enough that an index-wide pin fanning out across a genuinely large shard count is not
     * flagged while it is still legitimately in flight -- {@code
     * TransportIndexSnapshotPinAction#UNCONFIRMED_PIN_TTL_MILLIS} bounds the unconfirmed phase at ten
     * minutes; this is an order of magnitude past that so a slow-but-healthy fan-out is never mistaken
     * for an abandoned one.
     */
    public static final long DEFAULT_ABANDONED_AFTER_MILLIS = 2 * 60 * 60 * 1000L;

    private final PinLedgerSweeper sweeper;
    private final long abandonedAfterMillis;
    private final String indexUuid;
    private final Scheduler.Cancellable task;

    /**
     * Creates and immediately schedules a recurring pin-ledger sweep for one index.
     *
     * @param threadPool the thread pool used to schedule the recurring task, on {@code GENERIC} since
     *                    a pass is real object-store I/O.
     * @param interval how often to run a sweep pass.
     * @param indexUuid the index this task sweeps, for logging only -- {@code ledgerStore} and {@code
     *                  containerResolver} already carry it implicitly.
     * @param ledgerStore the index's shard-0 ledger store.
     * @param containerResolver resolves any other shard of this same index, for the shards a ledger
     *                          names beyond shard 0.
     * @param abandonedAfterMillis how old a still-live ledger has to be before a pass logs it.
     */
    public PinLedgerSweepTask(
        ThreadPool threadPool,
        TimeValue interval,
        String indexUuid,
        BlobContainerPinLedgerStore ledgerStore,
        ShardCloner.ContainerResolver containerResolver,
        long abandonedAfterMillis
    ) {
        this.sweeper = new PinLedgerSweeper(ledgerStore, containerResolver);
        this.abandonedAfterMillis = abandonedAfterMillis;
        this.indexUuid = indexUuid;
        // Jittered, matching GcSchedulerTask/WalGcSchedulerTask -- this task is "GC-adjacent" by its
        // own setting's javadoc, and is constructed once per index in getEngineFactory, so a
        // coordinated restart or object-store recovery event that opens many indices' shard 0 around
        // the same moment would otherwise schedule every one of their sweeps in lockstep, the exact
        // thundering herd JitteredScheduling exists to prevent for its siblings.
        this.task = threadPool.scheduleWithFixedDelay(this::sweepOnce, JitteredScheduling.jitter(interval), ThreadPool.Names.GENERIC);
    }

    private void sweepOnce() {
        try {
            PinLedgerSweeper.SweepResult result = sweeper.sweepOnce(System.currentTimeMillis(), abandonedAfterMillis);
            if (result.ledgersCleared() > 0) {
                logger.info("pin ledger sweep for index [{}] cleared [{}] released ledger(s)", indexUuid, result.ledgersCleared());
            }
            if (result.ledgersWithinFanOutWindow() > 0) {
                // Debug, not warn: this is the sweep working as designed, and the next pass will judge them.
                // Logged at all because "the ledger is still there and the sweep ran" otherwise has no
                // visible explanation.
                logger.debug(
                    "pin ledger sweep for index [{}] left [{}] ledger(s) alone: still inside the fan-out window",
                    indexUuid,
                    result.ledgersWithinFanOutWindow()
                );
            }
            if (result.abandonedPastTtl().isEmpty() == false) {
                logger.warn(
                    "pin ledger sweep for index [{}] found [{}] ledger(s) still live past their abandoned-after window: {}",
                    indexUuid,
                    result.abandonedPastTtl().size(),
                    result.abandonedPastTtl()
                );
            }
        } catch (Exception e) {
            // Never lets one bad pass stop the schedule -- the next tick retries from scratch, the
            // same "best effort, self-healing" contract every other GC-adjacent task in this plugin
            // keeps.
            logger.warn("pin ledger sweep for index [" + indexUuid + "] failed; will retry next tick", e);
        }
    }

    @Override
    public void close() {
        task.cancel();
    }

    /**
     * Runs one sweep pass synchronously and rethrows rather than swallowing, so a test can observe
     * both the result and any exception a production tick would otherwise only log and retry past --
     * the shape a wiring regression (a delete-denied container, say) needs to fail a test loudly
     * rather than leave it silently asserting "not deleted yet" with no reason why. Not used by
     * {@link #sweepOnce()} itself, which keeps its own swallow-and-retry contract for the real
     * scheduled tick.
     */
    public PinLedgerSweeper.SweepResult sweepNowForTesting(long nowMillis) throws Exception {
        return sweeper.sweepOnce(nowMillis, abandonedAfterMillis);
    }
}
