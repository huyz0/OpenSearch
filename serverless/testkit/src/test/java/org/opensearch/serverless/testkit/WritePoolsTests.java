/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.shell.WritePools;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Document writes wait for the node log on a pool sized for waiting, while no more of them apply to the engine at once
 * than core's {@code write} pool had threads; switched off, everything is as it was.
 */
public class WritePoolsTests extends OpenSearchTestCase {

    private static Settings cores(int processors, boolean enabled) {
        return Settings.builder().put("node.processors", processors).put(WritePools.ENABLED, enabled).build();
    }

    public void testSizesGrowWithCoresWhenOn() {
        final Settings on = cores(4, true);
        assertEquals(WritePools.POOL, WritePools.writePool(on));
        assertEquals(4 * WritePools.DEFAULT_THREADS_PER_CORE, WritePools.threads(on));
        assertEquals(5, WritePools.applyPermits(on));
        assertEquals(1_536, WritePools.defaultMaxInFlight(on));
        assertEquals("never below the old cap", 1_024, WritePools.defaultMaxInFlight(cores(1, true)));
    }

    public void testOffIsAsBefore() {
        final Settings off = cores(4, false);
        assertEquals(ThreadPool.Names.WRITE, WritePools.writePool(off));
        assertEquals(4, WritePools.threads(off));
        assertEquals(1_024, WritePools.defaultMaxInFlight(off));
        assertEquals("no gate", -1, WritePools.of(off).freePermits());
    }

    public void testNoMoreApplyAtOnceThanThePermits() throws Exception {
        final WritePools pools = WritePools.of(cores(2, true));
        final List<Releasable> held = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            held.add(pools.enterApply());
        }
        assertEquals(0, pools.freePermits());
        final AtomicBoolean entered = new AtomicBoolean();
        final CountDownLatch done = new CountDownLatch(1);
        final Thread fourth = new Thread(() -> {
            try (Releasable ignored = pools.enterApply()) {
                entered.set(true);
            } catch (Exception e) {
                throw new AssertionError(e);
            } finally {
                done.countDown();
            }
        });
        fourth.start();
        assertFalse("a fourth apply waits while three hold the gate", done.await(200, TimeUnit.MILLISECONDS));
        held.remove(0).close();
        assertTrue("and goes once one finishes", done.await(10, TimeUnit.SECONDS));
        assertTrue(entered.get());
        held.forEach(Releasable::close);
        fourth.join();
        assertEquals(3, pools.freePermits());
    }
}
