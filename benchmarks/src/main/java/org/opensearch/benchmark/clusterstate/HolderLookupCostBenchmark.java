/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.clusterstate;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexMetadataHolder;
import org.opensearch.common.settings.Settings;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * What C5's storage split costs an index that never opts in.
 *
 * <p>{@code StubResolutionSpike} (S10) measured a different shape: a wrapper object always present,
 * with the {@link IndexMetadata} behind it. That cost ~5-6% on every already-resolved lookup, which is
 * what made S10 conclude C5 could not be split into a safe first step -- introducing the indirection
 * without the payoff would be pure cost.
 *
 * <p>The shape that shipped avoids the wrapper: {@link IndexMetadata} implements
 * {@link IndexMetadataHolder} and returns itself from {@code get()}, so a materialized index sits in
 * the map directly. This measures whether that removes the cost, by timing the one thing the change
 * actually adds to a lookup: the {@code holder.get()} call on a map that holds nothing but
 * {@link IndexMetadata}.
 *
 * <p>Both arms run in the same JVM and alternate round by round, because a cross-JVM comparison of a
 * sub-nanosecond difference is swamped by run-to-run variance -- an earlier version of this benchmark
 * measured {@code Metadata.index} before and after in separate processes and the noise (4.9-7.0 ns for
 * the same code) was an order of magnitude larger than the effect. Reported as medians for the same
 * reason.
 *
 * <pre>{@code
 * java -da -dsa -cp <benchmarks runtime classpath> \
 *   org.opensearch.benchmark.clusterstate.HolderLookupCostBenchmark
 * }</pre>
 */
public final class HolderLookupCostBenchmark {

    private HolderLookupCostBenchmark() {}

    private static final int INDICES = 1024;
    private static final int WARMUP_ITERS = 20_000_000;
    private static final int MEASURE_ITERS = 20_000_000;
    private static final int ROUNDS = 15;

    public static void main(String[] args) {
        final String[] names = new String[INDICES];
        final Map<String, IndexMetadata> direct = new HashMap<>();
        final Map<String, IndexMetadataHolder> holders = new HashMap<>();
        for (int i = 0; i < INDICES; i++) {
            names[i] = "index-" + i;
            IndexMetadata meta = index(names[i]);
            direct.put(names[i], meta);
            // The same object, stored as a holder -- which it is, with no wrapper.
            holders.put(names[i], meta);
        }

        blackhole(runDirect(direct, names, WARMUP_ITERS));
        blackhole(runThroughHolder(holders, names, WARMUP_ITERS));

        double[] directNs = new double[ROUNDS];
        double[] holderNs = new double[ROUNDS];
        for (int round = 0; round < ROUNDS; round++) {
            // Alternate the order so any drift over the run hits both arms equally.
            if (round % 2 == 0) {
                directNs[round] = perOp(time(() -> blackhole(runDirect(direct, names, MEASURE_ITERS))));
                holderNs[round] = perOp(time(() -> blackhole(runThroughHolder(holders, names, MEASURE_ITERS))));
            } else {
                holderNs[round] = perOp(time(() -> blackhole(runThroughHolder(holders, names, MEASURE_ITERS))));
                directNs[round] = perOp(time(() -> blackhole(runDirect(direct, names, MEASURE_ITERS))));
            }
        }

        System.out.println("Lookup of a materialized index: direct vs through IndexMetadataHolder.get()");
        System.out.println(ROUNDS + " alternating rounds of " + MEASURE_ITERS + " lookups each\n");
        System.out.printf(Locale.ROOT, "%-8s %12s %12s%n", "round", "direct", "via holder");
        for (int round = 0; round < ROUNDS; round++) {
            System.out.printf(Locale.ROOT, "%-8d %9.3f ns %9.3f ns%n", round + 1, directNs[round], holderNs[round]);
        }

        double directMedian = median(directNs);
        double holderMedian = median(holderNs);
        System.out.printf(Locale.ROOT, "%n%-8s %9.3f ns %9.3f ns%n", "median", directMedian, holderMedian);
        System.out.printf(
            Locale.ROOT,
            "%-8s %9.3f ns  (%+.1f%%)%n",
            "delta",
            holderMedian - directMedian,
            (holderMedian / directMedian - 1) * 100
        );
        System.out.printf(Locale.ROOT, "%-8s %9.3f ns / %.3f ns%n", "spread", spread(directNs), spread(holderNs));
    }

    private static int runDirect(Map<String, IndexMetadata> map, String[] names, int iters) {
        int acc = 0;
        for (int i = 0; i < iters; i++) {
            acc += map.get(names[i & (INDICES - 1)]).getNumberOfShards();
        }
        return acc;
    }

    private static int runThroughHolder(Map<String, IndexMetadataHolder> map, String[] names, int iters) {
        int acc = 0;
        for (int i = 0; i < iters; i++) {
            acc += map.get(names[i & (INDICES - 1)]).get().getNumberOfShards();
        }
        return acc;
    }

    private static IndexMetadata index(String name) {
        return IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
    }

    private static double perOp(long ns) {
        return (double) ns / MEASURE_ITERS;
    }

    private static double median(double[] values) {
        double[] sorted = values.clone();
        Arrays.sort(sorted);
        int mid = sorted.length / 2;
        return sorted.length % 2 == 1 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
    }

    /** Max minus min, so the reader can see whether the delta is bigger than the noise. */
    private static double spread(double[] values) {
        double[] sorted = values.clone();
        Arrays.sort(sorted);
        return sorted[sorted.length - 1] - sorted[0];
    }

    private static long time(Runnable r) {
        long start = System.nanoTime();
        r.run();
        return System.nanoTime() - start;
    }

    private static void blackhole(int v) {
        if (v == Integer.MIN_VALUE) {
            throw new IllegalStateException("unreachable");
        }
    }
}
