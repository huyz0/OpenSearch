/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.Semaphore;

/**
 * Where document writes run, and how many apply to the engine at once.
 *
 * <p><b>Why writes do not run on core's {@code write} pool.</b> A write is acknowledged only once its record has landed in
 * the node log, and the log lands one window at a time (250 ms by default), so a write's thread spends most of its time
 * waiting, not working. Core's {@code write} pool has as many threads as cores, plus one at most: on four cores, four
 * threads each holding a write for about a quarter of a second took about sixteen REST writes a second, whatever the
 * document size, and the rest waited in its queue for seconds. The pool a forwarded write is applied on at its owner
 * was sized the same way. With {@link #ENABLED} on, both have {@link #THREADS_PER_CORE} threads a core, enough for the
 * waiting a node's CPU can feed.
 *
 * <p><b>What stays bounded by cores.</b> Applying an operation to the engine is the CPU-bound part, and the number of
 * threads indexing into a shard at once is the number of in-memory segments Lucene keeps for it, hence the segments each
 * refresh publishes. So the engine apply goes through a gate of as many permits as core's {@code write} pool had threads:
 * as many concurrent applies as before, only the waiting for the log is spread over more threads. A write waiting for the
 * gate has been admitted by the write limiter, which counts it as waiting, so a node short of CPU still sheds load with a
 * 429 as before.
 *
 * <p><b>The in-flight cap.</b> With the waiting no longer bounded by the thread count, the limiter's cap on writes in
 * flight is the bound, and a node needs about 0.35 s times its write rate of it; with this on, its default grows with the
 * cores, {@link #IN_FLIGHT_PER_CORE} a core and never below the old 1,024.
 */
public final class WritePools {

    /** Whether document writes run on {@link #POOL} and forwarded writes on a pool of the same size, with the apply gate. */
    public static final String ENABLED = "serverless.write.wait_pool.enabled";

    /** The default for {@link #ENABLED}. */
    public static final boolean DEFAULT_ENABLED = true;

    /** Threads per core of {@link #POOL}, and of the forwarded-write pool, when {@link #ENABLED} is on. */
    public static final String THREADS_PER_CORE = "serverless.write.wait_pool.threads_per_core";

    /** The default for {@link #THREADS_PER_CORE}. */
    public static final int DEFAULT_THREADS_PER_CORE = 64;

    /** Writes in flight per core the limiter allows by default when {@link #ENABLED} is on. */
    public static final int IN_FLIGHT_PER_CORE = 384;

    /** The limiter's in-flight cap before it grew with cores, and its floor since. */
    public static final int MIN_IN_FLIGHT = 1_024;

    /** The pool document writes run on when {@link #ENABLED} is on. */
    public static final String POOL = "serverless_write";

    private final Semaphore gate;

    private WritePools(Semaphore gate) {
        this.gate = gate;
    }

    /**
     * Builds the gate for a node.
     *
     * @param settings the node settings
     * @return the gate, letting everything through when {@link #ENABLED} is off
     */
    public static WritePools of(Settings settings) {
        return new WritePools(enabled(settings) ? new Semaphore(applyPermits(settings), true) : null);
    }

    /**
     * Reports whether writes run on the waiting-sized pools.
     *
     * @param settings the node settings
     * @return true when {@link #ENABLED} is on
     */
    public static boolean enabled(Settings settings) {
        return settings.getAsBoolean(ENABLED, DEFAULT_ENABLED);
    }

    /**
     * Names the pool a node runs document writes on.
     *
     * @param settings the node settings
     * @return {@link #POOL}, or core's {@code write} pool when {@link #ENABLED} is off
     */
    public static String writePool(Settings settings) {
        return enabled(settings) ? POOL : ThreadPool.Names.WRITE;
    }

    /**
     * How many threads a pool that waits for writes has: as many as cores when {@link #ENABLED} is off, as before.
     *
     * @param settings the node settings
     * @return the thread count
     */
    public static int threads(Settings settings) {
        final int processors = OpenSearchExecutors.allocatedProcessors(settings);
        if (enabled(settings) == false) {
            return processors;
        }
        return Math.max(processors, processors * Math.max(1, settings.getAsInt(THREADS_PER_CORE, DEFAULT_THREADS_PER_CORE)));
    }

    /**
     * As many concurrent engine applies as core's {@code write} pool had threads.
     *
     * @param settings the node settings
     * @return the permits
     */
    public static int applyPermits(Settings settings) {
        return OpenSearchExecutors.allocatedProcessors(settings) + 1;
    }

    /**
     * The limiter's default cap on writes in flight.
     *
     * @param settings the node settings
     * @return 1,024, or with {@link #ENABLED} on, {@link #IN_FLIGHT_PER_CORE} a core and at least 1,024
     */
    public static int defaultMaxInFlight(Settings settings) {
        if (enabled(settings) == false) {
            return MIN_IN_FLIGHT;
        }
        return Math.max(MIN_IN_FLIGHT, IN_FLIGHT_PER_CORE * OpenSearchExecutors.allocatedProcessors(settings));
    }

    /**
     * Takes a permit to apply to the engine, waiting for one if all are taken.
     *
     * @return gives the permit back
     * @throws java.io.IOException if interrupted while waiting, before anything was applied
     */
    public Releasable enterApply() throws java.io.IOException {
        final Semaphore permits = gate;
        if (permits == null) {
            return () -> {};
        }
        try {
            permits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new java.io.IOException("interrupted waiting to apply a write; nothing was applied", e);
        }
        return permits::release;
    }

    /**
     * Reports how many applies could start now, for tests.
     *
     * @return free permits, or -1 with no gate
     */
    public int freePermits() {
        return gate == null ? -1 : gate.availablePermits();
    }
}
