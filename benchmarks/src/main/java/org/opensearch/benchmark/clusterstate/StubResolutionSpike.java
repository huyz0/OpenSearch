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
import org.opensearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Spike for C5, the piece {@code rfc-serverless-control-cell-diet.md} left unresolved.
 *
 * <p>That document's adversarial review concluded the mechanism was undecided: making
 * {@code Metadata} stop holding a fully-materialized {@link IndexMetadata} for every index requires
 * its per-index storage to become a union of "the real thing" and "a compact stub that resolves on
 * demand", and it was not established whether such a union can be zero-cost for the indices that
 * never opt in. That is the question here, and it is asked in isolation -- nothing in {@code server/}
 * is touched, because the point is to decide whether the change is worth attempting at all.
 *
 * <p>Two things are measured:
 * <ol>
 *   <li><b>Is a resolved entry free?</b> A caller touching an already-materialized index must not
 *       pay for the union existing. Compares direct map access against access through the union.</li>
 *   <li><b>What does a stub-holding map cost?</b> The heap of N entries where most are stubs, versus
 *       N fully-materialized {@link IndexMetadata}.</li>
 * </ol>
 *
 * <pre>{@code
 * java -cp <benchmarks runtime classpath> \
 *   org.opensearch.benchmark.clusterstate.StubResolutionSpike
 * }</pre>
 */
public final class StubResolutionSpike {

    private StubResolutionSpike() {}

    private static final int INDICES = 200_000;
    private static final int WARMUP_ITERS = 5_000_000;
    private static final int MEASURE_ITERS = 20_000_000;

    /**
     * The union. Holds either a materialized {@link IndexMetadata} or the inputs needed to produce
     * one on demand. Deliberately shaped so the resolved case is a single volatile read plus a null
     * check -- the cheapest form that is still safe under concurrent access.
     */
    static final class IndexMetadataOrStub {
        private volatile IndexMetadata resolved;
        private final String name;
        private final Function<String, IndexMetadata> loader;

        IndexMetadataOrStub(IndexMetadata resolved) {
            this.resolved = resolved;
            this.name = null;
            this.loader = null;
        }

        IndexMetadataOrStub(String name, Function<String, IndexMetadata> loader) {
            this.resolved = null;
            this.name = name;
            this.loader = loader;
        }

        IndexMetadata get() {
            IndexMetadata local = resolved;
            if (local != null) {
                return local;
            }
            return resolveSlow();
        }

        private synchronized IndexMetadata resolveSlow() {
            if (resolved == null) {
                resolved = loader.apply(name);
            }
            return resolved;
        }

        boolean isResolved() {
            return resolved != null;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== C5 spike: union-typed Metadata storage ===\n");
        measureResolvedAccessCost();
        measureStubHeap();
    }

    /** Question 1: does routing an already-resolved read through the union cost anything? */
    private static void measureResolvedAccessCost() {
        final Map<String, IndexMetadata> direct = new HashMap<>();
        final Map<String, IndexMetadataOrStub> union = new HashMap<>();
        final String[] names = new String[1024];
        for (int i = 0; i < names.length; i++) {
            names[i] = "index-" + i;
            IndexMetadata meta = index(names[i]);
            direct.put(names[i], meta);
            union.put(names[i], new IndexMetadataOrStub(meta));
        }

        // Warm up both paths so the JIT has compiled them before timing.
        blackhole(runDirect(direct, names, WARMUP_ITERS));
        blackhole(runUnion(union, names, WARMUP_ITERS));

        long directNs = time(() -> blackhole(runDirect(direct, names, MEASURE_ITERS)));
        long unionNs = time(() -> blackhole(runUnion(union, names, MEASURE_ITERS)));

        System.out.println("1. Cost of reading an ALREADY-RESOLVED entry (" + MEASURE_ITERS + " lookups):");
        System.out.printf(
            java.util.Locale.ROOT,
            "   direct map.get()      : %6.1f ms  (%.2f ns/op)%n",
            directNs / 1e6,
            (double) directNs / MEASURE_ITERS
        );
        System.out.printf(
            java.util.Locale.ROOT,
            "   through the union     : %6.1f ms  (%.2f ns/op)%n",
            unionNs / 1e6,
            (double) unionNs / MEASURE_ITERS
        );
        System.out.printf(java.util.Locale.ROOT, "   overhead              : %+.1f%%%n%n", ((double) unionNs / directNs - 1) * 100);
    }

    /** Question 2: what does a mostly-stub map retain versus a fully-materialized one? */
    private static void measureStubHeap() throws Exception {
        System.out.println("2. Retained heap for " + INDICES + " indices:");
        long full = measureRetained(() -> {
            Map<String, IndexMetadata> m = new HashMap<>();
            Random r = new Random(42);
            for (int i = 0; i < INDICES; i++) {
                String n = "tenant-" + Long.toHexString(r.nextLong());
                m.put(n, index(n));
            }
            return m;
        });

        final AtomicInteger resolutions = new AtomicInteger();
        long stubs = measureRetained(() -> {
            Map<String, IndexMetadataOrStub> m = new HashMap<>();
            Random r = new Random(42);
            for (int i = 0; i < INDICES; i++) {
                String n = "tenant-" + Long.toHexString(r.nextLong());
                m.put(n, new IndexMetadataOrStub(n, name -> {
                    resolutions.incrementAndGet();
                    return index(name);
                }));
            }
            return m;
        });

        System.out.printf(
            java.util.Locale.ROOT,
            "   fully materialized    : %6.1f MB  (%.0f B/index)%n",
            full / 1024.0 / 1024,
            (double) full / INDICES
        );
        System.out.printf(
            java.util.Locale.ROOT,
            "   all stubs, unresolved : %6.1f MB  (%.0f B/index)%n",
            stubs / 1024.0 / 1024,
            (double) stubs / INDICES
        );
        System.out.printf(
            java.util.Locale.ROOT,
            "   reduction             : %.1fx   (resolutions triggered: %d)%n",
            (double) full / stubs,
            resolutions.get()
        );
    }

    private static int runDirect(Map<String, IndexMetadata> map, String[] names, int iters) {
        int acc = 0;
        for (int i = 0; i < iters; i++) {
            IndexMetadata m = map.get(names[i & 1023]);
            acc += m.getNumberOfShards();
        }
        return acc;
    }

    private static int runUnion(Map<String, IndexMetadataOrStub> map, String[] names, int iters) {
        int acc = 0;
        for (int i = 0; i < iters; i++) {
            IndexMetadata m = map.get(names[i & 1023]).get();
            acc += m.getNumberOfShards();
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

    private static long time(Runnable r) {
        long start = System.nanoTime();
        r.run();
        return System.nanoTime() - start;
    }

    private static long measureRetained(java.util.function.Supplier<Object> builder) throws Exception {
        Object warm = builder.get();
        if (warm.hashCode() == Integer.MIN_VALUE) {
            throw new IllegalStateException("unreachable");
        }
        warm = null;
        gc();
        long before = used();
        Object held = builder.get();
        gc();
        long after = used();
        if (held.hashCode() == Integer.MIN_VALUE) {
            throw new IllegalStateException("unreachable, keeps held live");
        }
        return after - before;
    }

    private static void gc() throws InterruptedException {
        for (int i = 0; i < 5; i++) {
            System.gc();
            Thread.sleep(150);
        }
    }

    private static long used() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }

    private static void blackhole(int v) {
        if (v == Integer.MIN_VALUE) {
            throw new IllegalStateException("unreachable");
        }
    }
}
