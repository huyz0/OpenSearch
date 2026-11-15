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

    /** An apply on another thread, entered or still waiting. */
    private static final class Apply {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch end = new CountDownLatch(1);
        final Thread thread;

        Apply(WritePools pools, String index) {
            thread = new Thread(() -> {
                try (Releasable ignored = pools.enterApply(index)) {
                    entered.countDown();
                    end.await();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            thread.start();
        }

        boolean enteredWithin(long millis) throws InterruptedException {
            return entered.await(millis, TimeUnit.MILLISECONDS);
        }

        void finish() throws InterruptedException {
            end.countDown();
            thread.join(10_000L);
        }
    }

    public void testAnIndexOverItsShareOfAFullGateLetsAnotherGoFirst() throws Exception {
        // Four permits, an index's share two.
        final WritePools pools = WritePools.of(Settings.builder().put(cores(3)).put("serverless.cost.max_wait_millis", 10_000L).build());
        final List<Releasable> hot = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            hot.add(pools.enterApply("hot"));
        }
        assertEquals("alone, an index takes every permit", 0, pools.freePermits());
        final Apply hotAgain = new Apply(pools, "hot");
        assertFalse(hotAgain.enteredWithin(100L));
        final Apply cold = new Apply(pools, "cold");
        assertFalse(cold.enteredWithin(100L));
        hot.remove(0).close();
        assertTrue("the permit given back is the other index's", cold.enteredWithin(2_000L));
        assertFalse("still over its share of a full gate", hotAgain.enteredWithin(200L));
        hot.remove(0).close();
        assertTrue(hotAgain.enteredWithin(2_000L));
        cold.finish();
        hotAgain.finish();
        hot.forEach(Releasable::close);
        assertEquals(4, pools.freePermits());
    }

    public void testOverItsShareAnIndexWaitsForAnApplyQueuedBehindIt() throws Exception {
        final WritePools pools = WritePools.of(Settings.builder().put(cores(1)).put("serverless.cost.max_wait_millis", 200L).build());
        final Releasable a = pools.enterApply("hot");
        final Releasable b = pools.enterApply("hot");
        final Apply cold = new Apply(pools, "cold");
        assertFalse(cold.enteredWithin(100L));
        // The hot index is over its share of one permit, with another index queued: it waits, then goes to the gate.
        final Apply hotAgain = new Apply(pools, "hot");
        a.close();
        assertTrue(cold.enteredWithin(2_000L));
        b.close();
        assertTrue(hotAgain.enteredWithin(2_000L));
        cold.finish();
        hotAgain.finish();
    }

    public void testShareZeroLeavesTheGateAsItWas() throws Exception {
        final WritePools pools = WritePools.of(Settings.builder().put(cores(1)).put("serverless.cost.write.index_share", 0.0).build());
        final Releasable a = pools.enterApply("hot");
        final Releasable b = pools.enterApply("hot");
        assertEquals(0, pools.freePermits());
        a.close();
        b.close();
    }
}
