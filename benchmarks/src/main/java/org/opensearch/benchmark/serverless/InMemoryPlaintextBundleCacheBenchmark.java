/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.serverless;

import org.opensearch.serverless.storage.format.BundleFileEntry;
import org.opensearch.serverless.storage.format.BundleFileReader;
import org.opensearch.serverless.storage.format.InMemoryPlaintextBundleCache;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Measures concurrent cache-HIT throughput of {@link InMemoryPlaintextBundleCache} -- every hit
 * re-links the touched entry to the most-recently-used end of a striped {@code LinkedHashMap}
 * under that stripe's own lock (see that class's own javadoc), so a hit is a write, not a read.
 * The class's whole striped-lock design exists specifically to keep this scaling under concurrent
 * access from many reader shards on one node rather than serializing all of them behind one lock;
 * this benchmark is the regression guard for that design, run with 16 threads to approximate
 * that concurrent access pattern.
 *
 * <p>{@code maxTotalBytesCap} is fixed well above {@code MIN_BYTES_PER_STRIPE * MAX_STRIPES}
 * (32&nbsp;KiB), so every parameterization here always exercises the full striped code path, not
 * the small-budget single-stripe fallback -- that fallback is exact pre-striping behavior and
 * already covered by {@code InMemoryPlaintextBundleCacheTests}'s own correctness tests, not a
 * throughput concern. {@code keyCount} instead varies how many
 * distinct hot bundle names spread across the fixed 32 stripes: a small key count has a real chance
 * of several hot keys landing on the same stripe (and so contending for the same lock), while a
 * large one spreads far more evenly.
 *
 * <p>{@code fileSizeBytes * keyCount} is kept far below {@code maxTotalBytesCap} for every
 * parameterization, so no entry is ever evicted during the timed portion of a run -- this isolates
 * hit-path lock contention from eviction churn, which is a different, separately-testable cost.
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Threads(16)
public class InMemoryPlaintextBundleCacheBenchmark {

    @Benchmark
    public void hit(CacheParameters parameters, Blackhole blackhole) throws Exception {
        blackhole.consume(parameters.readRandomHotKey());
    }

    @State(Scope.Benchmark)
    public static class CacheParameters {

        private static final long MAX_TOTAL_BYTES_CAP = 64L * 1024 * 1024; // 64 MiB -- always forces the full 32 stripes.
        private static final int FILE_SIZE_BYTES = 4096;

        @Param({ "8", "64", "512" })
        int keyCount;

        private InMemoryPlaintextBundleCache cache;
        private String[] bundleNames;
        private BundleFileEntry[] entries;
        private BundleFileReader onMiss;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            byte[] fileBytes = new byte[FILE_SIZE_BYTES];
            onMiss = (bundleName, entry) -> fileBytes;
            cache = new InMemoryPlaintextBundleCache(MAX_TOTAL_BYTES_CAP);
            bundleNames = new String[keyCount];
            entries = new BundleFileEntry[keyCount];
            for (int i = 0; i < keyCount; i++) {
                bundleNames[i] = "bundle-" + i;
                entries[i] = new BundleFileEntry("file-" + i, 0, FILE_SIZE_BYTES, 12345L);
                cache.readFile(bundleNames[i], entries[i], onMiss); // warm it -- every subsequent read is a hit.
            }
        }

        byte[] readRandomHotKey() throws Exception {
            int i = ThreadLocalRandom.current().nextInt(keyCount);
            return cache.readFile(bundleNames[i], entries[i], onMiss);
        }
    }
}
