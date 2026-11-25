/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.serverless.ops.ColdOpenWait;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ColdOpenWaitTests extends OpenSearchTestCase {

    private final ExecutorService pool = Executors.newCachedThreadPool();

    @Override
    public void tearDown() throws Exception {
        pool.shutdownNow();
        super.tearDown();
    }

    public void testAnOpenThatFinishesInTimeReturnsItsResult() throws Exception {
        assertEquals("open", ColdOpenWait.await(pool, 5_000, () -> "open"));
    }

    public void testAnOpenThatOutlastsTheWaitGivesWayAndKeepsRunning() throws Exception {
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch finished = new CountDownLatch(1);
        expectThrows(ColdOpenWait.StillOpening.class, () -> ColdOpenWait.await(pool, 50, () -> {
            release.await();
            finished.countDown();
            return "late";
        }));
        assertEquals("the open was not cancelled", 1, finished.getCount());
        release.countDown();
        assertTrue("the open ran on after the wait", finished.await(10, TimeUnit.SECONDS));
    }

    public void testAFailedOpenKeepsItsOwnException() {
        final IllegalStateException failure = new IllegalStateException("no such index");
        final IllegalStateException thrown = expectThrows(IllegalStateException.class, () -> ColdOpenWait.await(pool, 5_000, () -> {
            throw failure;
        }));
        assertSame(failure, thrown);
    }

    public void testNoWaitRunsTheOpenOnTheCallingThread() throws Exception {
        final AtomicInteger runs = new AtomicInteger();
        final Thread caller = Thread.currentThread();
        final Thread ranOn = ColdOpenWait.await(pool, 0, () -> {
            runs.incrementAndGet();
            return Thread.currentThread();
        });
        assertSame(caller, ranOn);
        assertEquals(1, runs.get());
    }
}
