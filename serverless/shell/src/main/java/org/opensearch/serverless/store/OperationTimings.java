/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.annotation.InternalApi;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

/**
 * Node-scoped operation timings with fixed memory usage and no index, shard, or tenant labels.
 * Histograms have logarithmic nanosecond buckets. Percentiles are bucket upper bounds, not exact
 * latency measurements. Concurrent snapshots are approximate and never block operation completion.
 */
@InternalApi
public final class OperationTimings {

    /** Independently measured stages; durations overlap and must not be summed into request latency. */
    public enum Stage {
        /** A caller's group-commit wait, including its group's physical append. */
        WAL_APPEND,
        /** One physical group append attempt, including serialization and backend retries. */
        WAL_FLUSH,
        /** An explicit reader refresh requested by a document mutation. */
        MUTATION_REFRESH,
        /** A publication attempt, including flush, upload, manifest publication, and log pruning. */
        SEGMENT_PUBLICATION
    }

    private final Histogram[] histograms = new Histogram[Stage.values().length];

    /** Creates an empty, fixed-size timing registry. */
    public OperationTimings() {
        for (int i = 0; i < histograms.length; i++) {
            histograms[i] = new Histogram();
        }
    }

    /**
     * Records a completed stage. Negative elapsed durations are clamped to zero.
     *
     * @param stage the stage
     * @param elapsedNanos elapsed monotonic time
     * @param successful whether the stage completed successfully
     */
    public void record(Stage stage, long elapsedNanos, boolean successful) {
        final Histogram histogram = histograms[stage.ordinal()];
        final long nanos = Math.max(0L, elapsedNanos);
        histogram.total.getAndAccumulate(nanos, (total, value) -> total > Long.MAX_VALUE - value ? Long.MAX_VALUE : total + value);
        histogram.max.accumulateAndGet(nanos, Math::max);
        if (successful == false) {
            histogram.failures.increment();
        }
        histogram.buckets.incrementAndGet(nanos == 0 ? 0 : Long.SIZE - Long.numberOfLeadingZeros(nanos));
    }

    /**
     * Returns approximate snapshots for all stages, including stages without samples.
     *
     * @return immutable snapshots keyed by stage
     */
    public Map<Stage, Snapshot> snapshots() {
        final Map<Stage, Snapshot> result = new EnumMap<>(Stage.class);
        for (Stage stage : Stage.values()) {
            result.put(stage, histograms[stage.ordinal()].snapshot());
        }
        return Map.copyOf(result);
    }

    /** One approximate histogram snapshot. All durations are nanoseconds. */
    public record Snapshot(long samples, long failures, long totalNanos, long maxNanos, long p50UpperBoundNanos, long p95UpperBoundNanos,
        long p99UpperBoundNanos) {
    }

    private static final class Histogram {
        private final AtomicLongArray buckets = new AtomicLongArray(Long.SIZE);
        private final AtomicLong total = new AtomicLong();
        private final LongAdder failures = new LongAdder();
        private final AtomicLong max = new AtomicLong();

        private Snapshot snapshot() {
            final long[] counts = new long[buckets.length()];
            long samples = 0;
            for (int i = 0; i < counts.length; i++) {
                counts[i] = buckets.get(i);
                samples += counts[i];
            }
            return new Snapshot(
                samples,
                failures.sum(),
                total.get(),
                max.get(),
                percentile(counts, samples, 50),
                percentile(counts, samples, 95),
                percentile(counts, samples, 99)
            );
        }

        private static long percentile(long[] counts, long samples, int percentile) {
            if (samples == 0) {
                return 0L;
            }
            final long rank = (long) Math.ceil(samples * (percentile / 100.0));
            long seen = 0;
            for (int i = 0; i < counts.length; i++) {
                seen += counts[i];
                if (seen >= rank) {
                    return i == Long.SIZE - 1 ? Long.MAX_VALUE : (1L << i) - 1;
                }
            }
            throw new AssertionError("histogram rank exceeds sample count");
        }
    }
}
