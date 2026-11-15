/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.nodelog;

import java.util.concurrent.TimeUnit;

/**
 * How long a node-log PUT waits before it is sent a second time: the configured delay, raised to the recent PUTs'
 * tail and scaled by a blob's size, so that hedging sends at most about one PUT in twenty twice.
 */
final class NodeLogHedgeDelay {

    private volatile long hedgeAfterNanos;

    /** The most recent PUT durations, for the hedge delay; guarded by itself. */
    private final long[] recentPutNanos = new long[RECENT_PUTS];
    /** The sizes of the same PUTs, to scale a larger blob's hedge delay; guarded by {@link #recentPutNanos}. */
    private final long[] recentPutBytes = new long[RECENT_PUTS];
    private int recentPuts;
    private static final int RECENT_PUTS = 128;
    /** The share of PUTs, at most, that hedging may send twice: the hedge waits at least this quantile of recent ones. */
    private static final double HEDGE_QUANTILE = 0.95;

    /**
     * Sets how long a PUT may go unanswered before it is sent again.
     *
     * @param afterMillis the delay; zero or less never sends a second
     */
    void configure(long afterMillis) {
        this.hedgeAfterNanos = afterMillis <= 0 ? 0L : TimeUnit.MILLISECONDS.toNanos(afterMillis);
    }

    /** Remembers how long a PUT that landed took, and how big it was. */
    void notePutTook(long nanos, long bytes) {
        synchronized (recentPutNanos) {
            recentPutBytes[recentPuts % RECENT_PUTS] = bytes;
            recentPutNanos[recentPuts++ % RECENT_PUTS] = nanos;
        }
    }

    /**
     * How long a PUT of this many bytes waits before it is sent again: the configured delay, or the 95th percentile of
     * the recent PUTs when that is longer, so that a store slower than the setting expects is hedged on its slowest
     * twentieth at most rather than on most of its PUTs; then scaled up by how many times the median of the recent PUTs'
     * sizes the blob is. A blob ten times the usual size takes about ten times as long to send; hedged on the usual PUTs'
     * tail, it was nearly always sent twice.
     *
     * @param blobBytes the blob's size; zero or less scales nothing
     */
    long delayNanos(long blobBytes) {
        final long configured = hedgeAfterNanos;
        if (configured <= 0) {
            return 0L;
        }
        final long[] sorted;
        final long[] sizes;
        synchronized (recentPutNanos) {
            final int n = Math.min(recentPuts, RECENT_PUTS);
            sorted = java.util.Arrays.copyOf(recentPutNanos, n);
            sizes = java.util.Arrays.copyOf(recentPutBytes, n);
        }
        if (sorted.length == 0) {
            return configured;
        }
        java.util.Arrays.sort(sorted);
        final long delay = Math.max(configured, sorted[(int) Math.min(sorted.length - 1, Math.floor(HEDGE_QUANTILE * sorted.length))]);
        if (blobBytes <= 0) {
            return delay;
        }
        java.util.Arrays.sort(sizes);
        final long median = Math.max(1L, sizes[sizes.length / 2]);
        if (blobBytes <= median) {
            return delay;
        }
        return (long) Math.min(Long.MAX_VALUE / 4.0, delay * ((double) blobBytes / median));
    }
}
