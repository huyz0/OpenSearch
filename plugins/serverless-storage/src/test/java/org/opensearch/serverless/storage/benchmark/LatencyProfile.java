/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.benchmark;

import org.opensearch.common.Randomness;

/**
 * A simulated per-operation-type latency configuration for {@link LatencyInjectingBlobContainer}.
 * Test/benchmark support only -- not for production use.
 *
 * <p>Each operation type has an independent {@code [min, max]} millisecond range; a call draws a
 * uniform random value from that range each time. This is intentionally simple (no tail-latency
 * distribution, no correlation between calls) -- good enough to give the RFC's milestones
 * (rfc-serverless-opensearch.md cold-start/p95/p99 section) a reproducible order-of-magnitude
 * signal, not a substitute for measuring against a real object store.
 */
public final class LatencyProfile {

    /**
     * Roughly matches typical intra-region S3 single-object GET/PUT/LIST latencies at the low end
     * of their normal range. Approximate and illustrative only, not a substitute for real
     * measurement.
     */
    public static final LatencyProfile LOW = new LatencyProfile(5, 10, 10, 20, 15, 25);

    /**
     * Roughly matches published single-region S3 GET/PUT/LIST latency figures under typical
     * conditions (GET ~20-40ms, PUT ~40-80ms, LIST ~50-100ms). Approximate and illustrative only,
     * not a substitute for real measurement.
     */
    public static final LatencyProfile TYPICAL = new LatencyProfile(20, 40, 40, 80, 50, 100);

    /**
     * Roughly matches degraded/cross-region or throttled object-store conditions. Approximate and
     * illustrative only, not a substitute for real measurement.
     */
    public static final LatencyProfile HIGH = new LatencyProfile(80, 160, 150, 300, 200, 400);

    private final long readMinMillis;
    private final long readMaxMillis;
    private final long writeMinMillis;
    private final long writeMaxMillis;
    private final long listMinMillis;
    private final long listMaxMillis;

    /**
     * @param readMinMillis minimum simulated read latency, in milliseconds.
     * @param readMaxMillis maximum simulated read latency, in milliseconds.
     * @param writeMinMillis minimum simulated write latency, in milliseconds.
     * @param writeMaxMillis maximum simulated write latency, in milliseconds.
     * @param listMinMillis minimum simulated list latency, in milliseconds.
     * @param listMaxMillis maximum simulated list latency, in milliseconds.
     */
    public LatencyProfile(
        long readMinMillis,
        long readMaxMillis,
        long writeMinMillis,
        long writeMaxMillis,
        long listMinMillis,
        long listMaxMillis
    ) {
        if (readMinMillis < 0 || readMinMillis > readMaxMillis) {
            throw new IllegalArgumentException("invalid read latency range: [" + readMinMillis + ", " + readMaxMillis + "]");
        }
        if (writeMinMillis < 0 || writeMinMillis > writeMaxMillis) {
            throw new IllegalArgumentException("invalid write latency range: [" + writeMinMillis + ", " + writeMaxMillis + "]");
        }
        if (listMinMillis < 0 || listMinMillis > listMaxMillis) {
            throw new IllegalArgumentException("invalid list latency range: [" + listMinMillis + ", " + listMaxMillis + "]");
        }
        this.readMinMillis = readMinMillis;
        this.readMaxMillis = readMaxMillis;
        this.writeMinMillis = writeMinMillis;
        this.writeMaxMillis = writeMaxMillis;
        this.listMinMillis = listMinMillis;
        this.listMaxMillis = listMaxMillis;
    }

    /** @return a simulated read-operation delay, in milliseconds, drawn from this profile's read range. */
    public long sampleReadMillis() {
        return sample(readMinMillis, readMaxMillis);
    }

    /** @return a simulated write-operation delay, in milliseconds, drawn from this profile's write range. */
    public long sampleWriteMillis() {
        return sample(writeMinMillis, writeMaxMillis);
    }

    /** @return a simulated list-operation delay, in milliseconds, drawn from this profile's list range. */
    public long sampleListMillis() {
        return sample(listMinMillis, listMaxMillis);
    }

    /** @return this profile's minimum possible read-operation delay, in milliseconds. */
    public long readMinMillis() {
        return readMinMillis;
    }

    /** @return this profile's minimum possible write-operation delay, in milliseconds. */
    public long writeMinMillis() {
        return writeMinMillis;
    }

    /** @return this profile's minimum possible list-operation delay, in milliseconds. */
    public long listMinMillis() {
        return listMinMillis;
    }

    private static long sample(long minMillis, long maxMillis) {
        if (minMillis == maxMillis) {
            return minMillis;
        }
        long range = maxMillis - minMillis + 1;
        return minMillis + Math.floorMod(Randomness.get().nextLong(), range);
    }
}
