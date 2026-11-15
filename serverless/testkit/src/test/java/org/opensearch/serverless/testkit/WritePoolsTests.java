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
import org.opensearch.serverless.node.WritePools;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Document writes wait for the node log on a pool sized for waiting, while no more of them apply to the engine at once
 * than core's {@code write} pool had threads.
 */
public class WritePoolsTests extends OpenSearchTestCase {

    private static Settings cores(int processors) {
        return Settings.builder().put("node.processors", processors).build();
    }

    public void testSizesGrowWithCores() {
        final Settings on = cores(4);
        assertEquals(4 * WritePools.DEFAULT_THREADS_PER_CORE, WritePools.threads(on));
        assertEquals(5, WritePools.applyPermits(on));
        assertEquals(1_536, WritePools.defaultMaxInFlight(on));
        assertEquals("never below the old cap", 1_024, WritePools.defaultMaxInFlight(cores(1)));
    }

    public void testNoMoreApplyAtOnceThanThePermits() throws Exception {
        final WritePools pools = WritePools.of(cores(2));
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
