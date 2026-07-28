/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.clusterstate;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;

/**
 * What the suspension lookup costs when many threads read it at once.
 *
 * <p>{@code GatedShardSuspensionRegistry.suspendedShards} is read from
 * {@code AbsentIndexRoutingSuppliers.supply} on every computed routing resolution, so once per index per
 * request. H12 made it a fixed-capacity cache with <b>access ordering</b>, deliberately: scale-to-zero means
 * most indices are cold, so an insertion-ordered cache would let a sweep over the sleeping majority evict
 * the active set on every pass.
 *
 * <p>Access ordering has a cost that H12 did not measure. A read mutates the recency list, so
 * {@code Collections.synchronizedMap} is not incidental to the structure, it is required by it: every
 * lookup takes the same monitor. On a node serving many gated indices that is one global lock on a path
 * taken by every search and every write.
 *
 * <p>Three arms, alternating round by round in one JVM so run-to-run variance does not decide the answer:
 *
 * <ul>
 *   <li><b>synchronized-lru</b>, what ships today: access-ordered and monitor-guarded</li>
 *   <li><b>concurrent-plain</b>, a {@link ConcurrentHashMap} with no eviction, which is the ceiling any
 *       fix can reach and is not itself a candidate, since it reintroduces the unbounded growth H12
 *       removed</li>
 *   <li><b>concurrent-sampled</b>, a {@link ConcurrentHashMap} whose recency is approximated by stamping a
 *       counter on write rather than reordering on read, so reads never mutate shared structure</li>
 * </ul>
 *
 * <p>The third arm mirrors what {@code GatedShardSuspensionRegistry} now does, so this measures the shipped
 * implementation rather than a stand-in for it.
 *
 * <p><b>Measured.</b> Reads per second, median of seven rounds:
 *
 * <pre>
 *  threads     synchronized-lru   concurrent-sampled   speedup
 *        1           57,374,303          109,553,164      1.9x
 *        4           12,093,744          407,803,540     33.7x
 *       16            9,050,344        1,037,405,037    114.6x
 * </pre>
 *
 * <p>The old structure does not merely fail to scale, it runs <em>backwards</em>: 57.4M reads per second on
 * one thread down to 9.1M on sixteen, which is a lock convoy on a path every request takes. The stamped
 * record costs about six percent against a plain concurrent map, and that is the price of keeping the bound
 * H12 added rather than dropping eviction to reach the ceiling.
 *
 * <pre>{@code
 * java -da -dsa -cp <benchmarks runtime classpath> \
 *   org.opensearch.benchmark.clusterstate.SuspensionLookupContentionBenchmark
 * }</pre>
 */
public class SuspensionLookupContentionBenchmark {

    /** How many indices a node tracks suspensions for. Below the 50,000 default so every arm is warm. */
    private static final int TRACKED_INDICES = 10_000;

    /** Thread counts worth reporting: one is the uncontended floor, the rest are realistic search pools. */
    private static final int[] THREADS = { 1, 4, 16 };

    private static final int ROUNDS = 7;
    private static final long DURATION_MILLIS = 400;

    public static void main(String[] args) throws Exception {
        String[] uuids = new String[TRACKED_INDICES];
        for (int i = 0; i < TRACKED_INDICES; i++) {
            uuids[i] = "index-" + i + "-uuid";
        }

        System.out.println();
        System.out.printf(Locale.ROOT, "%8s %20s %18s %18s%n", "threads", "synchronized-lru", "concurrent-plain", "concurrent-sampled");

        for (int threads : THREADS) {
            long[] synchronizedLru = new long[ROUNDS];
            long[] concurrentPlain = new long[ROUNDS];
            long[] concurrentSampled = new long[ROUNDS];

            for (int round = 0; round < ROUNDS; round++) {
                // Alternating within the round rather than running each arm to completion, so a JIT or
                // GC state that favours one arm does not persist across the whole measurement.
                synchronizedLru[round] = readsPerSecond(synchronizedLru(uuids), uuids, threads);
                concurrentPlain[round] = readsPerSecond(concurrentPlain(uuids), uuids, threads);
                concurrentSampled[round] = readsPerSecond(concurrentSampled(uuids), uuids, threads);
            }

            System.out.printf(
                Locale.ROOT,
                "%8d %,20d %,18d %,18d%n",
                threads,
                median(synchronizedLru),
                median(concurrentPlain),
                median(concurrentSampled)
            );
        }

        System.out.println();
        System.out.println("  Reads per second, median of " + ROUNDS + " rounds. Higher is better.");
        System.out.println("  synchronized-lru is what ships. concurrent-plain is the unreachable ceiling,");
        System.out.println("  since dropping eviction reintroduces the growth H12 removed. concurrent-sampled");
        System.out.println("  is the candidate: bounded, recency approximated on write, reads never mutate.");
        System.out.println();
    }

    /** What ships: access-ordered and therefore monitor-guarded on every read. */
    private static Lookup synchronizedLru(String[] uuids) {
        Map<String, Set<Integer>> map = Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Set<Integer>> eldest) {
                return size() > TRACKED_INDICES;
            }
        });
        for (String uuid : uuids) {
            map.put(uuid, Set.of(0));
        }
        return uuid -> map.getOrDefault(uuid, Set.of());
    }

    /** The ceiling: no eviction at all, so nothing to synchronise. Not a candidate. */
    private static Lookup concurrentPlain(String[] uuids) {
        Map<String, Set<Integer>> map = new ConcurrentHashMap<>();
        for (String uuid : uuids) {
            map.put(uuid, Set.of(0));
        }
        return uuid -> map.getOrDefault(uuid, Set.of());
    }

    /**
     * The candidate: bounded, with recency stamped on write rather than reordered on read.
     *
     * <p>The read is a plain concurrent get and touches no shared mutable structure, which is the whole
     * point. Eviction quality is approximate rather than exact LRU, and that is the trade being measured.
     */
    private record Stamped(Set<Integer> shards, long stamp) {
    }

    private static Lookup concurrentSampled(String[] uuids) {
        ConcurrentHashMap<String, Stamped> map = new ConcurrentHashMap<>();
        long stamp = 0;
        for (String uuid : uuids) {
            map.put(uuid, new Stamped(Set.of(0), stamp++));
        }
        // Exactly what GatedShardSuspensionRegistry.suspendedShards now does.
        return uuid -> {
            Stamped stamped = map.get(uuid);
            return stamped == null ? Set.of() : stamped.shards();
        };
    }

    private static long readsPerSecond(Lookup lookup, String[] uuids, int threads) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(threads);
        LongAdder reads = new LongAdder();
        volatileStop = false;

        for (int t = 0; t < threads; t++) {
            final int offset = t * 7919;
            Thread worker = new Thread(() -> {
                try {
                    start.await();
                    long local = 0;
                    int i = offset;
                    while (volatileStop == false) {
                        // Spread across the population so this measures the structure rather than one
                        // cache line, and so access ordering has real reordering work to do.
                        for (int burst = 0; burst < 64; burst++) {
                            lookup.get(uuids[(i++ & Integer.MAX_VALUE) % uuids.length]);
                        }
                        local += 64;
                    }
                    reads.add(local);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finished.countDown();
                }
            });
            worker.setDaemon(true);
            worker.start();
        }

        start.countDown();
        long startedAt = System.nanoTime();
        Thread.sleep(DURATION_MILLIS);
        volatileStop = true;
        finished.await();
        double seconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;
        return (long) (reads.sum() / seconds);
    }

    private static volatile boolean volatileStop;

    private static long median(long[] values) {
        long[] sorted = values.clone();
        java.util.Arrays.sort(sorted);
        return sorted[sorted.length / 2];
    }

    @FunctionalInterface
    private interface Lookup {
        Set<Integer> get(String uuid);
    }
}
