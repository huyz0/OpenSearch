/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.serverless.rest.Fanout;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** The fan-out's window: bounded, barrier-free, and finite on a pool that runs nothing. */
public class FanoutTests extends OpenSearchTestCase {

    /**
     * The first task cannot finish until the last one has started. Chunked with a barrier, the last task waits
     * for the first chunk to finish and the fan-out never completes; with a sliding window the other worker
     * walks on past the stuck task and reaches it.
     */
    public void testASlowTaskDoesNotHoldUpTheTasksAfterIt() throws Exception {
        final ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            final int n = 20;
            final CountDownLatch lastStarted = new CountDownLatch(1);
            final List<Callable<Integer>> tasks = new ArrayList<>();
            tasks.add(() -> {
                assertTrue("the last task never started behind a stuck first one", lastStarted.await(10, TimeUnit.SECONDS));
                return 0;
            });
            for (int i = 1; i < n; i++) {
                final int slot = i;
                tasks.add(() -> {
                    if (slot == n - 1) {
                        lastStarted.countDown();
                    }
                    return slot;
                });
            }
            final List<Integer> results = Fanout.run(pool, 2, tasks);
            for (int i = 0; i < n; i++) {
                assertEquals("every task ran, in its own slot", Integer.valueOf(i), results.get(i));
            }
        } finally {
            terminate(pool);
        }
    }

    public void testNoMoreThanTheWidthRunAtOnce() throws Exception {
        final ExecutorService pool = Executors.newFixedThreadPool(16);
        try {
            final int width = randomIntBetween(1, 6);
            final AtomicInteger inFlight = new AtomicInteger();
            final AtomicInteger most = new AtomicInteger();
            final List<Callable<Integer>> tasks = new ArrayList<>();
            for (int i = 0; i < 60; i++) {
                tasks.add(() -> {
                    final int now = inFlight.incrementAndGet();
                    most.accumulateAndGet(now, Math::max);
                    Thread.onSpinWait();
                    for (int spin = 0; spin < 10_000; spin++) {
                        Thread.onSpinWait();
                    }
                    inFlight.decrementAndGet();
                    return now;
                });
            }
            Fanout.run(pool, width, tasks);
            assertTrue("at most " + width + " at once, saw " + most.get(), most.get() <= width);
        } finally {
            terminate(pool);
        }
    }

    /** A pool that never runs anything leaves the caller to do it all, in sequence, and still finish. */
    public void testASaturatedPoolCostsParallelismNotProgress() throws Exception {
        final List<Callable<String>> tasks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final int slot = i;
            tasks.add(() -> {
                if (slot == 3) {
                    throw new IllegalStateException("one shard could not be reached");
                }
                return "shard-" + slot;
            });
        }
        final List<String> results = Fanout.run(command -> {}, 8, tasks);
        assertNull("a task that threw leaves its slot empty", results.get(3));
        assertEquals("shard-9", results.get(9));
        assertTrue(Fanout.run(command -> {}, 8, List.<Callable<String>>of()).isEmpty());
    }
}
