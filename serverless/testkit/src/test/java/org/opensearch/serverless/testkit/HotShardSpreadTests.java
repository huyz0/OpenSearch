/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.lease.Releasable;
import org.opensearch.serverless.cluster.HotShardSpread;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A cold shard's searches go to its first reader, as before; a hot one's are spread over as many top-ranked readers as
 * its rate needs, by load, and come back to one when it cools.
 */
public class HotShardSpreadTests extends OpenSearchTestCase {

    private static final List<String> RANKED = List.of("r0", "r1", "r2", "r3");
    private static final long MILLI = 1_000_000L;

    /** Searches a shard at a steady rate for a while and counts where they went. */
    private static Map<String, Integer> searches(HotShardSpread spread, AtomicLong clock, double perSecond, long millis) {
        final Map<String, Integer> where = new HashMap<>();
        final long gap = (long) (1e9 / perSecond);
        for (long t = 0; t < millis * MILLI; t += gap) {
            clock.addAndGet(gap);
            where.merge(spread.order("alpha", 0, RANKED).get(0), 1, Integer::sum);
        }
        return where;
    }

    public void testAColdShardKeepsItsFirstReader() {
        final AtomicLong clock = new AtomicLong();
        final HotShardSpread spread = new HotShardSpread(50.0, 4, clock::get);
        assertEquals(Map.of("r0", 300), searches(spread, clock, 10.0, 30_000L));
        assertEquals(1, spread.widthOf("alpha", 0));
    }

    public void testAHotShardSpreadsAsWideAsItsRateNeedsAndNarrowsWhenItCools() {
        final AtomicLong clock = new AtomicLong();
        final HotShardSpread spread = new HotShardSpread(50.0, 4, clock::get);
        searches(spread, clock, 90.0, 30_000L);
        assertEquals(2, spread.widthOf("alpha", 0));
        final Map<String, Integer> two = searches(spread, clock, 90.0, 10_000L);
        assertEquals("only the top two readers: " + two, Set2.of("r0", "r1"), two.keySet());

        final Map<String, Integer> four = searches(spread, clock, 400.0, 30_000L);
        assertEquals(4, spread.widthOf("alpha", 0));
        for (String reader : RANKED) {
            assertTrue("every reader takes a share: " + four, four.getOrDefault(reader, 0) > 1_000);
        }

        clock.addAndGet(60_000L * MILLI);
        assertEquals("a minute quiet and it is cold again", 1, spread.widthOf("alpha", 0));
        assertEquals(List.of("r0", "r1", "r2", "r3"), spread.order("alpha", 0, RANKED));
    }

    public void testABusyReaderIsAvoided() {
        final AtomicLong clock = new AtomicLong();
        final HotShardSpread spread = new HotShardSpread(50.0, 2, clock::get);
        searches(spread, clock, 200.0, 10_000L);
        final List<Releasable> held = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            held.add(spread.begin("r0"));
        }
        final Map<String, Integer> where = searches(spread, clock, 200.0, 1_000L);
        assertEquals("with r0 twenty searches deep, every pick of two goes to r1: " + where, Map.of("r1", 200), where);
        held.forEach(Releasable::close);
    }

    /**
     * What the spread does to a hot shard's latency: readers that each serve one search at a time in 10 ms, offered
     * 180 searches a second for a minute. One reader cannot keep up and its queue grows; four share the load.
     */
    public void testAHotShardsLatencyWithAndWithoutTheSpread() {
        final double one = meanLatencyMillis(new HotShardSpread(1_000_000.0, 1, new AtomicLong()::get), 180.0);
        final double spread = meanLatencyMillis(null, 180.0);
        logger.info("SPREADMETER|180 searches/s, 10 ms each|one reader: mean {} ms|spread: mean {} ms", one, spread);
        assertTrue("one reader falls behind: " + one, one > 1_000.0);
        assertTrue("spread keeps up: " + spread, spread < 50.0);
    }

    private double meanLatencyMillis(HotShardSpread fixed, double perSecond) {
        final AtomicLong clock = new AtomicLong();
        final HotShardSpread spread = fixed != null ? fixed : new HotShardSpread(50.0, 4, clock::get);
        final long service = 10 * MILLI;
        final Map<String, Long> freeAt = new HashMap<>();
        final Map<String, Deque<Long>> finishing = new HashMap<>();
        final Map<String, Deque<Releasable>> marks = new HashMap<>();
        final long gap = (long) (1e9 / perSecond);
        double total = 0.0;
        int count = 0;
        for (long now = 0; now < 60_000L * MILLI; now += gap) {
            clock.set(now);
            // Searches that have finished by now are no longer in flight.
            for (String reader : RANKED) {
                final Deque<Long> done = finishing.computeIfAbsent(reader, r -> new ArrayDeque<>());
                while (done.isEmpty() == false && done.peekFirst() <= now) {
                    done.pollFirst();
                    marks.get(reader).pollFirst().close();
                }
            }
            final String reader = spread.order("alpha", 0, RANKED).get(0);
            marks.computeIfAbsent(reader, r -> new ArrayDeque<>()).addLast(spread.begin(reader));
            final long start = Math.max(now, freeAt.getOrDefault(reader, 0L));
            final long end = start + service;
            freeAt.put(reader, end);
            finishing.get(reader).addLast(end);
            total += (end - now) / (double) MILLI;
            count++;
        }
        return total / count;
    }

    /** {@link java.util.Set#of}, named apart so the assertion reads as the set it is. */
    private static final class Set2 {
        static java.util.Set<String> of(String... names) {
            return java.util.Set.of(names);
        }
    }
}
