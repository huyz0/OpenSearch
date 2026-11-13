/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * The heap a node's streamed batches may hold at once, handed out first come, first served.
 *
 * <p>A slice that does not fit waits its turn without holding a thread: it is granted, on {@code GENERIC}, when slices
 * ahead of it give theirs back. While it waits its connection is not read, so the client is slowed by TCP rather than
 * refused. Only a slice that has waited longer than the timeout is failed, and then with a 429.
 *
 * <p>Besides bytes, at most so many holders at once: applying more slices than the node has cores makes none of them
 * finish sooner, and each one applying holds its operations, its log records and its share of the indexing buffer until
 * it is acknowledged, so slices beyond the cores only add heap and log latency.
 *
 * <p>A request larger than the whole budget is granted the whole budget when it reaches the front, so it waits for the
 * node to be otherwise idle rather than forever.
 */
final class BulkMemoryBudget {

    private final long capacity;
    private final int maxHolders;
    private final ThreadPool threadPool;
    private final ArrayDeque<Waiter> waiters = new ArrayDeque<>();
    private long used;
    private int holders;

    private static final class Waiter {
        final long bytes;
        final Runnable granted;
        final Consumer<Exception> timedOut;
        Scheduler.ScheduledCancellable timeout;
        boolean done;

        Waiter(long bytes, Runnable granted, Consumer<Exception> timedOut) {
            this.bytes = bytes;
            this.granted = granted;
            this.timedOut = timedOut;
        }
    }

    BulkMemoryBudget(long capacity, int maxHolders, ThreadPool threadPool) {
        this.capacity = Math.max(1L, capacity);
        this.maxHolders = Math.max(1, maxHolders);
        this.threadPool = threadPool;
    }

    /** The bytes a request is granted: what it asked for, or the whole budget if it asked for more. */
    long granted(long bytes) {
        return Math.min(Math.max(1L, bytes), capacity);
    }

    /**
     * Asks for bytes, granted at once if they fit and nobody is waiting, otherwise in turn.
     *
     * @param bytes what to hold; more than the budget is granted the whole budget
     * @param waitAtMost how long to wait before giving up
     * @param granted run on {@code GENERIC} once granted; the holder must then {@link #release} {@link #granted(long)}
     * @param timedOut run on {@code GENERIC} if not granted in time
     */
    void acquire(long bytes, TimeValue waitAtMost, Runnable granted, Consumer<Exception> timedOut) {
        final Waiter waiter = new Waiter(granted(bytes), granted, timedOut);
        final boolean now;
        synchronized (this) {
            now = waiters.isEmpty() && fits(waiter.bytes);
            if (now) {
                used += waiter.bytes;
                holders++;
                waiter.done = true;
            } else {
                waiters.addLast(waiter);
            }
        }
        if (now) {
            threadPool.generic().execute(granted);
            return;
        }
        waiter.timeout = threadPool.schedule(() -> {
            synchronized (this) {
                if (waiter.done) {
                    return;
                }
                waiter.done = true;
                waiters.remove(waiter);
            }
            waiter.timedOut.accept(
                new org.opensearch.core.concurrency.OpenSearchRejectedExecutionException(
                    "a streamed batch waited "
                        + waitAtMost
                        + " for "
                        + waiter.bytes
                        + " bytes of the node's batch memory, which other batches held all that time; retry"
                )
            );
            grantWaiting();
        }, waitAtMost, ThreadPool.Names.GENERIC);
    }

    /**
     * Gives bytes back, and grants whoever is waiting and now fits.
     *
     * @param bytes what {@link #granted(long)} returned for the request
     */
    void release(long bytes) {
        synchronized (this) {
            used = Math.max(0L, used - bytes);
            holders = Math.max(0, holders - 1);
        }
        grantWaiting();
    }

    private void grantWaiting() {
        final List<Waiter> ready = new ArrayList<>();
        synchronized (this) {
            while (waiters.isEmpty() == false && fits(waiters.peekFirst().bytes)) {
                final Waiter next = waiters.pollFirst();
                next.done = true;
                used += next.bytes;
                holders++;
                ready.add(next);
            }
        }
        for (Waiter waiter : ready) {
            if (waiter.timeout != null) {
                waiter.timeout.cancel();
            }
            threadPool.generic().execute(waiter.granted);
        }
    }

    private boolean fits(long bytes) {
        return holders < maxHolders && used + bytes <= capacity;
    }

    /** Bytes held now. */
    synchronized long used() {
        return used;
    }

    /** Requests waiting now. */
    synchronized int waiting() {
        return waiters.size();
    }
}
