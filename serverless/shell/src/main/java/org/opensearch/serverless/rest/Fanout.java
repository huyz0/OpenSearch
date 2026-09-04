/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

/**
 * Runs per-shard work concurrently, a bounded number at a time.
 *
 * <p><b>Both fan-outs were sequential, and both said so in their own notes without being fixed.</b> A
 * search visited its shards one after another and a bulk request dispatched its per-shard groups the same
 * way, so latency was the <em>sum</em> of the shards rather than the slowest of them. On a filesystem that
 * is a rounding error. Against an object store, where opening a shard that nobody holds costs tens of
 * requests, it is the difference between a query that scales with an index and one that does not.
 *
 * <p><b>Not the search pool.</b> A shard query hands work to {@code ThreadPool.Names.SEARCH} and blocks
 * waiting for it, so running the fan-out there would have threads in that pool waiting on other threads in
 * that pool — fine until the fan-out is wider than the pool, and a deadlock after that. The caller passes
 * the executor it is already running on, which is {@code GENERIC}.
 *
 * <p><b>Bounded by chunking, which has a barrier per chunk and is chosen anyway.</b> A semaphore would
 * avoid the barrier, but every task waiting on a permit occupies a thread while it waits, which is the
 * cost this is trying to avoid. Chunking creates only as many tasks as can run. The barrier means a chunk
 * is as slow as its slowest member; at a default width of {@value #DEFAULT_CONCURRENCY} that only bites on
 * indices with many more shards than that, and it is recorded here rather than hidden.
 */
public final class Fanout {

    private static final Logger logger = LogManager.getLogger(Fanout.class);

    /**
     * How many shards are worked on at once.
     *
     * <p>Reasoned, not measured — the same admission {@code ReconcileScheduler}'s intervals carry. Wide
     * enough that a normal index is one chunk, narrow enough that a thousand-shard search does not put a
     * thousand blocked threads on the generic pool.
     */
    public static final int DEFAULT_CONCURRENCY = 8;

    /** The longest a fan-out waits for its tasks. */
    public static final org.opensearch.common.unit.TimeValue TIMEOUT = org.opensearch.common.unit.TimeValue.timeValueMinutes(5);

    private Fanout() {}

    /**
     * Runs every task, at most {@code concurrency} at a time, and returns the results in task order.
     *
     * <p>A task that throws yields {@code null} in its slot rather than failing the whole fan-out: one
     * shard that could not be reached is not a reason to abandon the others, which is the behaviour both
     * call sites already had when they were loops.
     *
     * @param <T> what a task produces
     * @param executor where to run them; must not be the pool the tasks themselves wait on
     * @param concurrency how many may run at once
     * @param tasks the work, one entry per shard
     * @return one result per task, in order, with nulls where a task failed
     * @throws InterruptedException if the caller is interrupted while waiting
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> run(Executor executor, int concurrency, List<Callable<T>> tasks) throws InterruptedException {
        final Object[] results = new Object[tasks.size()];
        final int width = Math.max(1, concurrency);
        for (int start = 0; start < tasks.size(); start += width) {
            final int end = Math.min(tasks.size(), start + width);
            final CountDownLatch done = new CountDownLatch(end - start);
            for (int i = start; i < end; i++) {
                final int slot = i;
                executor.execute(() -> {
                    try {
                        results[slot] = tasks.get(slot).call();
                    } catch (Exception e) {
                        // Left null. The caller decides what an unanswered shard means; for a search it
                        // is a hole in the coverage it already reports, and for a bulk it is a failed
                        // group.
                        logger.warn("a fanned-out task failed", e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            // Establishes happens-before with every countDown above, so the plain array is safely
            // published without synchronizing each write.
            // Bounded, so a fan-out whose task hangs in the object store fails rather than pinning the
            // coordinator's thread forever.
            if (done.await(TIMEOUT.millis(), java.util.concurrent.TimeUnit.MILLISECONDS) == false) {
                throw new InterruptedException("fanned-out work did not finish within " + TIMEOUT);
            }
        }
        return (List<T>) java.util.Arrays.asList(results);
    }
}
