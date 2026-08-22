/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A variant of {@link AsyncIOProcessor} that allows to batch and buffer processing items at every
 * {@link BufferedAsyncIOProcessor#getBufferInterval()} in a separate threadpool.
 * <p>
 * Requests are buffered till processor thread calls @{link drainAndProcessAndRelease} after bufferInterval.
 * If more requests are enqueued between invocations of drainAndProcessAndRelease, another processor thread
 * gets scheduled. Subsequent requests will get buffered till drainAndProcessAndRelease gets called in this new
 * processor thread.
 * <p>
 * Subclasses may additionally trigger a drain before the interval elapses once the buffered items'
 * cumulative size reaches {@link #getBufferByteThreshold()}, by overriding that method together with
 * {@link #itemSizeInBytes(Object)}. Both default to disabled, so existing subclasses are unaffected.
 *
 * @opensearch.internal
 */
public abstract class BufferedAsyncIOProcessor<Item> extends AsyncIOProcessor<Item> {

    private final ThreadPool threadpool;
    private final Supplier<TimeValue> bufferIntervalSupplier;
    private final AtomicLong bufferedBytes = new AtomicLong();
    // The currently scheduled-but-not-yet-run drain, if any. A byte-threshold trip that finds the
    // promise semaphore already held (a drain is scheduled for later in the interval) cancels and
    // fires this early rather than waiting -- the semaphore itself only gates who may *schedule*, not
    // when, so bringing an already-scheduled drain forward is the only way an early trigger can act.
    private final AtomicReference<Scheduler.ScheduledCancellable> pendingSchedule = new AtomicReference<>();

    protected BufferedAsyncIOProcessor(
        Logger logger,
        int queueSize,
        ThreadContext threadContext,
        ThreadPool threadpool,
        Supplier<TimeValue> bufferIntervalSupplier
    ) {
        super(logger, queueSize, threadContext);
        this.threadpool = threadpool;
        this.bufferIntervalSupplier = bufferIntervalSupplier;
    }

    @Override
    public void put(Item item, Consumer<Exception> listener) {
        Objects.requireNonNull(item, "item must not be null");
        Objects.requireNonNull(listener, "listener must not be null");
        addToQueue(item, listener);
        long threshold = getBufferByteThreshold();
        boolean overThreshold = threshold >= 0 && bufferedBytes.addAndGet(itemSizeInBytes(item)) >= threshold;
        scheduleProcess(overThreshold);
    }

    private void scheduleProcess(boolean immediate) {
        if (getQueue().isEmpty() == false && getPromiseSemaphore().tryAcquire()) {
            try {
                TimeValue delay = immediate ? TimeValue.ZERO : getBufferInterval();
                pendingSchedule.set(threadpool.schedule(this::process, delay, getBufferProcessThreadPoolName()));
            } catch (Exception e) {
                getLogger().error("failed to schedule process");
                processSchedulingFailure(e);
                getPromiseSemaphore().release();
                // This is to make sure that any new items that are added to the queue between processSchedulingFailure
                // and releasing the semaphore is handled by a subsequent refresh and not starved.
                scheduleProcess(false);
            }
        } else if (immediate) {
            // Someone else already holds the promise, meaning a drain is scheduled for later in the
            // interval (or is running right now). Try to bring it forward: cancel() is idempotent and
            // only one racing caller can win it, so at most one caller ever fires the early process().
            // If the drain is already running, cancel() simply returns false and this is a no-op --
            // the in-flight write already covers this item.
            Scheduler.ScheduledCancellable scheduled = pendingSchedule.get();
            if (scheduled != null && scheduled.cancel()) {
                process();
            }
        }
    }

    private void processSchedulingFailure(Exception e) {
        List<Tuple<Item, Consumer<Exception>>> candidates = new ArrayList<>();
        getQueue().drainTo(candidates);
        notifyList(candidates, e);
    }

    private void process() {
        pendingSchedule.set(null);
        bufferedBytes.set(0);
        drainAndProcessAndRelease(new ArrayList<>());
        scheduleProcess(false);
    }

    /**
     * The cumulative buffered-item size, in bytes as reported by {@link #itemSizeInBytes(Object)}, at
     * which a drain is triggered immediately instead of waiting for the next buffer interval tick.
     * Returns a negative value to disable byte-threshold triggering (the default).
     */
    protected long getBufferByteThreshold() {
        return -1;
    }

    /**
     * The size, in bytes, {@code item} contributes toward {@link #getBufferByteThreshold()}. Returns
     * {@code 0} by default, which combined with the default disabled threshold is a no-op.
     */
    protected long itemSizeInBytes(Item item) {
        return 0;
    }

    private TimeValue getBufferInterval() {
        long bufferInterval = bufferIntervalSupplier.get().getNanos();
        long timeSinceLastRunStartInNS = System.nanoTime() - getLastRunStartTimeInNs();
        if (timeSinceLastRunStartInNS >= bufferInterval) {
            return TimeValue.ZERO;
        }
        return TimeValue.timeValueNanos(bufferInterval - timeSinceLastRunStartInNS);
    }

    protected abstract String getBufferProcessThreadPoolName();

    // Exclusively for testing, please do not use it elsewhere.
    public Supplier<TimeValue> getBufferIntervalSupplier() {
        return bufferIntervalSupplier;
    }
}
