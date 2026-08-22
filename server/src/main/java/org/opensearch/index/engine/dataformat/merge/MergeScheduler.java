/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MergeSchedulerConfig;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.merge.MergeStatsTracker;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * Schedules and coordinates segment merge operations for a shard.
 * <p>
 * This scheduler delegates merge selection to a {@link MergeHandler} and controls
 * concurrency via configurable merge count limits sourced from
 * {@link MergeSchedulerConfig}. Merge tasks are submitted to the OpenSearch
 * {@link ThreadPool} using the {@link ThreadPool.Names#FORCE_MERGE} executor.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class MergeScheduler {

    private final Logger logger;
    private final MergeHandler mergeHandler;
    private final BiConsumer<MergeResult, OneMerge> applyMergeChanges;
    private final Runnable onMergeFailureCleanup;
    private final ThreadPool threadPool;
    private final AtomicInteger activeMerges = new AtomicInteger(0);
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final Semaphore forceMergeLock = new Semaphore(1);
    private final AtomicBoolean frozen = new AtomicBoolean(false);
    private final List<Runnable> onDrainedListeners = new CopyOnWriteArrayList<>();
    private volatile int maxConcurrentMerges;
    private volatile int maxMergeCount;
    private final MergeSchedulerConfig mergeSchedulerConfig;
    private final IndexSettings indexSettings;
    private final MergeStatsTracker mergeStatsTracker = new MergeStatsTracker();

    /** true if we should rate-limit writes for each merge */
    private boolean doAutoIOThrottle = false;

    /** Initial value for IO write rate limit when doAutoIOThrottle is true */
    private static final double START_MB_PER_SEC = 20.0;

    /** Current IO writes throttle rate */
    protected double targetMBPerSec = START_MB_PER_SEC;

    /**
     * Creates a new merge scheduler.
     *
     * @param mergeHandler          the handler that selects and executes merges
     * @param applyMergeChanges     callback to apply merge results (e.g., update the catalog)
     * @param onMergeFailureCleanup callback invoked when a merge fails and cleanup is performed
     * @param shardId               the shard this scheduler is associated with
     * @param indexSettings         the index settings providing merge scheduler configuration
     * @param threadPool            the OpenSearch thread pool for executing merge tasks
     */
    public MergeScheduler(
        MergeHandler mergeHandler,
        BiConsumer<MergeResult, OneMerge> applyMergeChanges,
        Runnable onMergeFailureCleanup,
        ShardId shardId,
        IndexSettings indexSettings,
        ThreadPool threadPool
    ) {
        this.mergeHandler = mergeHandler;
        this.applyMergeChanges = applyMergeChanges;
        this.onMergeFailureCleanup = onMergeFailureCleanup;
        this.threadPool = threadPool;
        logger = Loggers.getLogger(getClass(), shardId);
        this.indexSettings = indexSettings;
        this.mergeSchedulerConfig = indexSettings.getMergeSchedulerConfig();
        refreshConfig();
    }

    /**
     * Refreshes the max concurrent merge thread count and max merge count from
     * the current {@link MergeSchedulerConfig}. No-op if the values have not changed.
     */
    public synchronized void refreshConfig() {
        int newMaxThreadCount = mergeSchedulerConfig.getMaxThreadCount();
        int newMaxMergeCount = mergeSchedulerConfig.getMaxMergeCount();

        if (newMaxThreadCount == this.maxConcurrentMerges && newMaxMergeCount == this.maxMergeCount) {
            return;
        }

        logger.info(
            () -> new ParameterizedMessage(
                "Updating from merge scheduler config: maxThreadCount {} -> {}, " + "maxMergeCount {} -> {}",
                this.maxConcurrentMerges,
                newMaxThreadCount,
                this.maxMergeCount,
                newMaxMergeCount
            )
        );

        this.maxConcurrentMerges = newMaxThreadCount;
        this.maxMergeCount = newMaxMergeCount;
    }

    /**
     * Triggers pending merge operations. Merges are selected by the
     * underlying {@link MergeHandler} and executed up to the configured
     * concurrency limits.
     */
    public void triggerMerges() {
        if (isShutdown.get()) {
            logger.warn("MergeScheduler is shutdown, ignoring merge trigger");
            return;
        }
        // Only register new merges if not frozen. Already-pending merges
        // should still be executed to drain the queue to completion.
        if (!isFrozen()) {
            mergeHandler.findAndRegisterMerges();
        }
        executeMerge();
    }

    /**
     * Forces a merge down to at most {@code maxNumSegment} segments.
     * Runs synchronously on the calling thread, which must be a
     * {@link ThreadPool.Names#FORCE_MERGE} thread. Only one force merge
     * may execute per shard at a time — concurrent callers block until
     * the ongoing force merge completes.
     * <p>
     * A caller that has to queue behind an in-flight force merge counts towards
     * {@link #getActiveMergeCount()} from the moment it enters this method — <em>before</em> it parks on
     * the force-merge lock. Without that, a queued force merge would be invisible to
     * {@link #onDrained(Runnable)}: the drain could report quiescence while a force merge sat parked on
     * the lock, and the parked merge would then mutate the catalog after tiering had already taken its
     * "last ever" flush/upload of the shard.
     * <p>
     * Once the lock is acquired the frozen state is re-checked: tiering may have started (and even
     * drained) while this caller was parked, and the pre-flight check the engine does before calling in
     * is stale by then. A frozen scheduler abandons the merge instead of running it.
     *
     * @param maxNumSegment the maximum number of segments after the force merge
     */
    public void forceMerge(int maxNumSegment) throws IOException {
        assert Thread.currentThread().getName().contains(ThreadPool.Names.FORCE_MERGE)
            : "forceMerge must be called on FORCE_MERGE thread but was: " + Thread.currentThread().getName();
        // Counted as outstanding merge work before parking on the lock — see the method javadoc.
        activeMerges.incrementAndGet();
        try {
            forceMergeLock.acquireUninterruptibly();
            try {
                if (isShutdown.get()) {
                    logger.debug("MergeScheduler is shutdown, skipping force merge");
                    return;
                }
                if (isFrozen()) {
                    logger.debug("MergeScheduler is frozen for tiering, abandoning queued force merge");
                    return;
                }
                runForceMerges(mergeHandler.findForceMerges(maxNumSegment));
            } finally {
                forceMergeLock.release();
            }
        } finally {
            decrementAndFireDrainListeners();
        }
    }

    /**
     * Runs the already-registered force merges serially, guaranteeing that every group which never runs
     * is unregistered again.
     * <p>
     * {@link MergeHandler#findForceMerges(int)} registers <em>all</em> selected groups in
     * {@code currentlyMergingSegments} up front (without queueing them as pending merges), so a group
     * that is never executed would otherwise stay registered forever and be silently excluded from all
     * future background and force merges. Both early-exit paths — a failing merge and a shutdown
     * mid-loop — therefore release the registrations of the groups that were not reached.
     */
    private void runForceMerges(Collection<OneMerge> oneMerges) throws IOException {
        final List<OneMerge> selected = List.copyOf(oneMerges);
        // Index of the first merge whose registration this method still owns. A merge that has been
        // handed to runMerge owns its own cleanup (onMergeFailure on failure, onMergeFinished on success).
        int firstUnrun = 0;
        try {
            for (int i = 0; i < selected.size(); i++) {
                firstUnrun = i;
                if (isShutdown.get()) {
                    logger.debug("MergeScheduler shutdown during force merge, aborting remaining merges");
                    break;
                }
                firstUnrun = i + 1;
                runMerge(selected.get(i));
            }
        } finally {
            if (firstUnrun < selected.size()) {
                mergeHandler.unregisterMerges(selected.subList(firstUnrun, selected.size()));
            }
        }
    }

    /**
     * Turn on dynamic IO throttling, to adaptively rate limit writes bytes/sec to the minimal rate
     * necessary so merges do not fall behind. By default, this is disabled and writes are not
     * rate-limited.
     */
    public synchronized void enableAutoIOThrottle() {
        doAutoIOThrottle = true;
        targetMBPerSec = START_MB_PER_SEC;
    }

    /**
     * Returns the currently set per-merge IO writes rate limit, if {@link #enableAutoIOThrottle} was
     * called, else {@code Double.POSITIVE_INFINITY}.
     */
    public synchronized double getIORateLimitMBPerSec() {
        if (doAutoIOThrottle) {
            return targetMBPerSec;
        }

        return Double.POSITIVE_INFINITY;
    }

    /**
     * Freezes the merge scheduler: blocks new merges (in-flight and already-pending merges still drain
     * to completion). Used during tiering preparation to ensure no catalog mutations from merges.
     * <p>
     * Idempotent via {@code compareAndSet} — only the first call that actually flips the state takes
     * effect; redundant calls are no-ops.
     *
     * @return {@code true} if this call transitioned the scheduler from unfrozen to frozen,
     *         {@code false} if it was already frozen
     */
    public boolean freeze() {
        return frozen.compareAndSet(false, true);
    }

    /**
     * Unfreezes the merge scheduler, allowing merges to resume. Called when tiering is cancelled.
     * <p>
     * Equivalent to {@link #unfreeze(Runnable)} with {@link #triggerMerges()} as the resume action.
     *
     * @return {@code true} if this call transitioned the scheduler from frozen to unfrozen,
     *         {@code false} if it was already unfrozen
     */
    public boolean unfreeze() {
        return unfreeze(this::triggerMerges);
    }

    /**
     * Unfreezes the merge scheduler and runs {@code resumeAction} to resume merging.
     * <p>
     * Idempotent via {@code compareAndSet}: {@code resumeAction} runs only on a real
     * frozen-to-unfrozen transition, so a redundant unfreeze does not kick off a spurious merge cycle.
     * <p>
     * The resume action is injectable so the owning engine can route the resume through its own
     * merge-enabled gate rather than calling {@link #triggerMerges()} unconditionally.
     * <p>
     * Before resuming, any drain listener whose condition is already satisfied is fired: an unfreeze
     * that lands while a tiering prepare is waiting must not leave that listener parked until some
     * unrelated merge happens to complete.
     *
     * @param resumeAction invoked (on the calling thread) only when this call wins the frozen-to-unfrozen
     *                     transition
     * @return {@code true} if this call transitioned the scheduler from frozen to unfrozen,
     *         {@code false} if it was already unfrozen
     */
    public boolean unfreeze(Runnable resumeAction) {
        if (frozen.compareAndSet(true, false)) {
            fireDrainListenersIfDrained();
            resumeAction.run();
            return true;
        }
        return false;
    }

    /**
     * Returns true if the merge scheduler is frozen — either explicitly via {@link #freeze()}
     * or because the index tiering state indicates preparation/migration is in progress.
     */
    public boolean isFrozen() {
        return frozen.get() || isTieringToWarm(indexSettings, logger);
    }

    /**
     * Whether {@code INDEX_TIERING_STATE} indicates a hot-to-warm migration is in progress.
     * Shared by this scheduler and {@code DataFormatAwareEngine}'s own freeze checks so the
     * settings probe cannot drift between them. An unrecognized value is logged and treated
     * as {@code HOT} (not frozen).
     */
    public static boolean isTieringToWarm(IndexSettings indexSettings, Logger logger) {
        String state = indexSettings.getSettings().get(IndexModule.INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT.name());
        try {
            return IndexModule.TieringState.valueOf(state) == IndexModule.TieringState.HOT_TO_WARM;
        } catch (IllegalArgumentException e) {
            logger.warn(
                "Unrecognized {} value [{}]; treating engine as not frozen for tiering",
                IndexModule.INDEX_TIERING_STATE.getKey(),
                state
            );
            return false;
        }
    }

    /**
     * Registers a listener that fires when all active merges complete.
     * If already drained (no active merges and no pending), fires the listener immediately
     * inline. Otherwise, adds the listener to the list — all registered listeners will be
     * invoked on the merge thread when the last merge finishes.
     * <p>
     * Firing depends only on the drain condition, never on the freeze state: a listener registered
     * under a freeze that is subsequently lifted still fires when the merges it was waiting on finish.
     * The "active" count includes a force merge that is merely queued behind another, so a listener
     * cannot fire in the gap between one force merge finishing and the next starting.
     * <p>
     * Multiple listeners can be registered concurrently (thread-safe via CopyOnWriteArrayList).
     * <p>
     * Listeners must be idempotent: under a narrow race between this method's double-check and the
     * merge thread's snapshot-then-clear, the listener may be invoked twice. Gate any side-effects
     * with a {@code compareAndSet} (or equivalent) — see {@code TransportPrepareTieringAction} for
     * the canonical pattern.
     *
     * @param listener the callback to fire when merges are drained
     */
    public void onDrained(Runnable listener) {
        if (activeMerges.get() == 0 && !mergeHandler.hasPendingMerges()) {
            listener.run();
            return;
        }
        onDrainedListeners.add(listener);
        // Double-check after adding — merges may have finished between the check and the add
        if (activeMerges.get() == 0 && !mergeHandler.hasPendingMerges()) {
            if (onDrainedListeners.remove(listener)) {
                listener.run();
            }
        }
    }

    /**
     * Shuts down this merge scheduler, preventing new merges from being submitted.
     */
    public void shutdown() {
        isShutdown.set(true);
    }

    /**
     * Returns the number of currently active (in-flight) merge tasks.
     *
     * @return the active merge count
     */
    public int getActiveMergeCount() {
        return activeMerges.get();
    }

    /**
     * Returns whether there are any merges queued but not yet started.
     * <p>
     * Reports pending state orthogonally from active state: a {@code true} result here
     * means the queue is non-empty regardless of how many merges are currently running.
     * Callers that want a "any work outstanding" signal should combine this with
     * {@link #getActiveMergeCount()}.
     *
     * @return {@code true} if {@link MergeHandler#hasPendingMerges()} is {@code true}
     */
    public boolean hasPendingMerges() {
        return mergeHandler.hasPendingMerges();
    }

    /**
     * Returns the current merge statistics for this scheduler.
     *
     * @return the merge stats
     */
    public MergeStats stats() {
        return mergeStatsTracker.toMergeStats(mergeSchedulerConfig.isAutoThrottle() ? getIORateLimitMBPerSec() : Double.POSITIVE_INFINITY);
    }

    /**
     * Drains the pending-merge queue up to {@link #maxConcurrentMerges},
     * submitting each merge as a task to the thread pool.
     */
    private void executeMerge() {
        while (activeMerges.get() < maxConcurrentMerges && mergeHandler.hasPendingMerges()) {
            OneMerge oneMerge = mergeHandler.getNextMerge();
            if (oneMerge == null) {
                return;
            }
            try {
                submitMergeTask(oneMerge);
            } catch (Exception e) {
                mergeHandler.onMergeFailure(oneMerge);
                onMergeFailureCleanup.run();
                // The rejected merge already gave up its slot in submitMergeTask, and onMergeFailure
                // dropped it from the pending queue — so a drain may have become satisfied right here.
                fireDrainListenersIfDrained();
            }
        }
    }

    /**
     * Submits a merge task to the thread pool's merge executor.
     * <p>
     * The active-merge slot is taken before submission (so the merge is visible to a drain from the
     * moment it is handed to the executor) and given back here if the executor refuses the task —
     * otherwise a rejected submission would pin {@code activeMerges} above zero forever and no drain
     * listener could ever fire again on this shard.
     *
     * @param oneMerge the merge to execute
     */
    private void submitMergeTask(OneMerge oneMerge) {
        activeMerges.incrementAndGet();
        boolean submitted = false;
        try {
            threadPool.executor(ThreadPool.Names.MERGE).execute(() -> {
                try {
                    if (isShutdown.get()) {
                        logger.debug("MergeScheduler is shutdown, skipping merge");
                        return;
                    }
                    runMerge(oneMerge);
                } catch (Exception e) {
                    // runMerge already invoked onMergeFailureCleanup; swallow to prevent
                    // uncaught exception on the merge thread pool.
                } finally {
                    decrementAndFireDrainListeners();
                    // A completed merge may free up capacity for new merges, so check again.
                    executeMerge();
                }
            });
            submitted = true;
        } finally {
            if (submitted == false) {
                activeMerges.decrementAndGet();
            }
        }
    }

    /**
     * Executes a single merge and applies or cleans up the result.
     * <p>
     * This is the single point that owns the merge lifecycle:
     * <ol>
     *   <li>{@code doMerge} — may acquire {@code refreshLock} via the pre-merge-commit hook</li>
     *   <li>On success: {@code applyMergeChanges} — releases {@code refreshLock}</li>
     *   <li>On failure: {@code onMergeFailureCleanup} — releases {@code refreshLock} if still held</li>
     * </ol>
     * By funnelling both background and force merges through this method, the lock
     * release guarantee is maintained in exactly one code path.
     */
    private void runMerge(OneMerge oneMerge) throws IOException {
        long totalSizeInBytes = oneMerge.getTotalSizeInBytes();
        long totalNumDocs = oneMerge.getTotalNumDocs();
        long timeNS = System.nanoTime();
        long tookMS = 0;
        try {
            mergeStatsTracker.beforeMerge(totalNumDocs, totalSizeInBytes);
            MergeResult mergeResult = mergeHandler.doMerge(oneMerge);
            applyMergeChanges.accept(mergeResult, oneMerge);
            mergeHandler.onMergeFinished(oneMerge, isFrozen());
            tookMS = TimeValue.nsecToMSec((System.nanoTime() - timeNS));
            logger.info("Merge {} completed in {}ms, result: {}", oneMerge, tookMS, mergeResult.getMergedWriterFileSet());
        } catch (Exception e) {
            logger.error(new ParameterizedMessage("Merge failed for: {}", oneMerge), e);
            mergeHandler.onMergeFailure(oneMerge);
            onMergeFailureCleanup.run();
            throw e instanceof IOException ? (IOException) e : new IOException(e);
        } finally {
            mergeStatsTracker.afterMerge(tookMS, totalNumDocs, totalSizeInBytes);
        }
    }

    /**
     * Decrements the active merge count and fires the registered drain listeners if nothing is left to
     * drain. Called from both the background merge ({@link #submitMergeTask}) and force merge
     * ({@link #forceMerge}) completion paths.
     */
    private void decrementAndFireDrainListeners() {
        activeMerges.decrementAndGet();
        fireDrainListenersIfDrained();
    }

    /**
     * Fires (and clears) all registered drain listeners if no merges — active or pending — remain.
     * <p>
     * Deliberately <em>not</em> gated on {@link #isFrozen()}. A listener is only ever registered while a
     * tiering prepare is holding the scheduler frozen, but the freeze can be lifted underneath it while
     * merges are still in flight (a terminal prepare failure on a sibling shard flips
     * {@code index.blocks.write}, which drives {@code onSettingsChanged} → unfreeze on every engine of
     * the index). Gating on the freeze state meant that when those merges finally finished the listener
     * was neither fired nor cleared: the shard burned its full prepare timeout and the stale listener
     * stayed on the list across retries. The drain condition alone is the correct predicate — it is
     * exactly what the listener is waiting on, and it never fires early.
     */
    private void fireDrainListenersIfDrained() {
        if (activeMerges.get() == 0 && !mergeHandler.hasPendingMerges() && !onDrainedListeners.isEmpty()) {
            List<Runnable> listeners = List.copyOf(onDrainedListeners);
            onDrainedListeners.clear();
            for (Runnable listener : listeners) {
                try {
                    listener.run();
                } catch (Exception ex) {
                    logger.warn("Exception in onDrained listener", ex);
                }
            }
        }
    }
}
