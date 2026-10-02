/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.lease.Releasable;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * How many writes a node lets wait on the object store at once, adapted to how the store is answering.
 *
 * <p><b>What it replaces.</b> A write is acknowledged only once its log record has landed, and when the store throttles
 * -- {@code 503 SlowDown} -- every append retries with backoff. A fleet throttled for a minute had its writes stall for
 * the client's whole timeout: half a minute at the median, nearly all of them ending in a timeout the client could not
 * tell from a lost write, and every append that finally failed fencing its shard for a reopen that cost the store more.
 * Waiting harder made it worse.
 *
 * <p><b>The rule.</b> Additive increase, multiplicative decrease, on the writes in flight. An append that came back
 * slower than the target, or failed, shrinks the limit by a third -- at most twice a second, so one slow burst is one
 * cut. Every fast one grows it a little. A write that would exceed the limit, or that arrives while appends are taking
 * longer than the budget, is refused at once with {@code 429} and a {@code Retry-After} of about the current append
 * time: an honest "not now" the client can act on, instead of a timeout it cannot interpret.
 *
 * <p><b>Honest by construction.</b> A write is admitted or refused before anything is applied, so a refused write is in
 * neither the engine nor the log, and an admitted one is acknowledged only once its record has landed, as before.
 */
public final class WriteBackpressure {

    /** Refused for back-pressure; renders as 429 with a retry hint. */
    public static final class ThrottledException extends OpenSearchRejectedExecutionException {

        private final int retryAfterSeconds;

        ThrottledException(String message, int retryAfterSeconds) {
            super(message);
            this.retryAfterSeconds = retryAfterSeconds;
        }

        /**
         * Returns how long the caller should wait before trying again.
         *
         * @return seconds, at least one
         */
        public int retryAfterSeconds() {
            return retryAfterSeconds;
        }
    }

    private final long targetNanos;
    private final long budgetNanos;
    private final int minLimit;
    private final int maxLimit;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger refused = new AtomicInteger();
    private double limit;
    private double latencyNanos;
    private long lastDecreaseNanos = Long.MIN_VALUE;

    /**
     * Creates the limiter.
     *
     * @param targetMillis an append slower than this is a sign to back off
     * @param budgetMillis while appends average longer than this, new writes are refused
     * @param minLimit the fewest writes ever let through at once
     * @param maxLimit the most
     */
    public WriteBackpressure(long targetMillis, long budgetMillis, int minLimit, int maxLimit) {
        this.targetNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(1L, targetMillis));
        this.budgetNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(targetMillis, budgetMillis));
        this.minLimit = Math.max(1, minLimit);
        this.maxLimit = Math.max(this.minLimit, maxLimit);
        this.limit = this.maxLimit;
    }

    /**
     * Admits one write, or refuses it.
     *
     * @return released once the write has been acknowledged or has failed
     * @throws ThrottledException if the store is too slow to take it now
     */
    public Releasable admit() {
        final int limitNow;
        final double latencyNow;
        synchronized (this) {
            limitNow = (int) Math.floor(limit);
            latencyNow = latencyNanos;
        }
        final int current = inFlight.incrementAndGet();
        if (current > limitNow || (latencyNow > budgetNanos && current > minLimit)) {
            inFlight.decrementAndGet();
            refused.incrementAndGet();
            final int retryAfter = (int) Math.max(1L, Math.min(30L, TimeUnit.NANOSECONDS.toSeconds((long) latencyNow) + 1));
            throw new ThrottledException(
                "the object store is slow to take writes (an append is taking about "
                    + TimeUnit.NANOSECONDS.toMillis((long) latencyNow)
                    + " ms; "
                    + (current - 1)
                    + " writes already waiting, limit "
                    + limitNow
                    + "); retry after "
                    + retryAfter
                    + " s",
                retryAfter
            );
        }
        return inFlight::decrementAndGet;
    }

    /**
     * Records how one log append went.
     *
     * @param elapsedNanos how long it took
     * @param succeeded whether it landed
     */
    public synchronized void onAppend(long elapsedNanos, boolean succeeded) {
        latencyNanos = latencyNanos == 0d ? elapsedNanos : 0.8 * latencyNanos + 0.2 * elapsedNanos;
        final long now = System.nanoTime();
        // On the smoothed time, not each sample: a store answering in 300 to 600 ms with the odd append past the target
        // was cut by a third twice a second, and a fleet's limits sat at 18 to 27 -- refusing 22,000 writes in five
        // minutes of steady load that the store was taking.
        if (succeeded == false || latencyNanos > targetNanos) {
            if (lastDecreaseNanos == Long.MIN_VALUE || now - lastDecreaseNanos > TimeUnit.MILLISECONDS.toNanos(500)) {
                limit = Math.max(minLimit, limit * (2d / 3d));
                lastDecreaseNanos = now;
            }
        } else if (latencyNanos < targetNanos / 2) {
            // Comfortably inside the target: grow geometrically. Additive growth alone takes a limit cut to its floor by
            // a burst some 130,000 appends to climb back to a thousand -- an hour at a node's steady rate -- so a fleet
            // that had shed a minute of SlowDown went on shedding long after the store had recovered.
            limit = Math.min(maxLimit, limit * 1.02 + 0.5);
        } else {
            limit = Math.min(maxLimit, limit + 1d / Math.max(1d, limit) * 4d);
        }
    }

    /** What the limiter is doing now. */
    public record Stats(int limit, int inFlight, long appendMillis, long refused) {
    }

    /**
     * Returns the current limit, writes in flight, smoothed append time and refusals so far.
     *
     * @return the stats
     */
    public synchronized Stats stats() {
        return new Stats((int) Math.floor(limit), inFlight.get(), TimeUnit.NANOSECONDS.toMillis((long) latencyNanos), refused.get());
    }
}
