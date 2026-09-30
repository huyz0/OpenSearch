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
 * <p><b>The caller's own thread is one of the workers.</b> A fan-out that only ever waited for the pool
 * could wait forever: enough coordinators parked on a pool whose every thread was itself a parked
 * coordinator, and nothing left to run the work they were waiting for. That is the shape that once moved
 * the shard tasks to a pool of their own, and it came back one level up -- two nodes forwarding to each
 * other, each one's pool full of threads waiting for the other's. So the caller takes tasks from the same
 * queue the pool's workers do, and a task is taken exactly once. A saturated pool then costs parallelism
 * and never progress: the worst case is the caller doing all of its own work in sequence, which is what
 * the fan-out replaced and is always finite.
 *
 * <p><b>A sliding window, not chunks.</b> It used to hand the pool a chunk of {@code concurrency} tasks and
 * wait for the whole chunk before starting the next, so every chunk was as slow as its slowest member: a
 * thousand-shard search with one cold shard per chunk paid the cold activation once per chunk. Now
 * {@code concurrency - 1} pool workers and the caller each take the next task the moment they finish one,
 * so {@code concurrency} tasks are in flight until the queue drains, and a slow task holds up only its own
 * worker. Still no semaphore: a worker exists only while there is work to take, and never waits for a
 * permit while occupying a thread.
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
     * @param executor where to run them; the caller's own thread runs whatever this has not started
     * @param concurrency how many may run at once
     * @param tasks the work, one entry per shard
     * @return one result per task, in order, with nulls where a task failed
     * @throws InterruptedException if the caller is interrupted while waiting
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> run(Executor executor, int concurrency, List<Callable<T>> tasks) throws InterruptedException {
        final Object[] results = new Object[tasks.size()];
        if (tasks.isEmpty()) {
            return (List<T>) java.util.Arrays.asList(results);
        }
        // The next task to take. Every worker, the caller included, takes by incrementing it, so a task is
        // taken exactly once and the latch below counts each one once whoever runs it.
        final java.util.concurrent.atomic.AtomicInteger next = new java.util.concurrent.atomic.AtomicInteger();
        final CountDownLatch done = new CountDownLatch(tasks.size());
        final int helpers = Math.min(Math.max(1, concurrency), tasks.size()) - 1;
        for (int i = 0; i < helpers; i++) {
            // A helper the pool starts late finds the queue drained and returns at once.
            executor.execute(() -> drain(next, tasks, results, done));
        }
        drain(next, tasks, results, done);
        // What the helpers still hold. Establishes happens-before with every countDown, so the plain array
        // is safely published without synchronizing each write. Bounded, so a fan-out whose task hangs in
        // the object store fails rather than pinning the coordinator's thread forever.
        if (done.await(TIMEOUT.millis(), java.util.concurrent.TimeUnit.MILLISECONDS) == false) {
            throw new InterruptedException("fanned-out work did not finish within " + TIMEOUT);
        }
        return (List<T>) java.util.Arrays.asList(results);
    }

    /** Takes and runs tasks until none are left. */
    private static <T> void drain(
        java.util.concurrent.atomic.AtomicInteger next,
        List<Callable<T>> tasks,
        Object[] results,
        CountDownLatch done
    ) {
        for (int slot = next.getAndIncrement(); slot < tasks.size(); slot = next.getAndIncrement()) {
            try {
                results[slot] = tasks.get(slot).call();
            } catch (Exception e) {
                // Left null. The caller decides what an unanswered shard means; for a search it is a hole in
                // the coverage it already reports, and for a bulk it is a failed group.
                logger.warn("a fanned-out task failed", e);
            } finally {
                done.countDown();
            }
        }
    }
}
