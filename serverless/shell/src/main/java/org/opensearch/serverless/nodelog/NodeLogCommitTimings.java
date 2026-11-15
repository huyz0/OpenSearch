/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.nodelog;

import java.util.List;

/**
 * Where a {@link NodeLogCommitter#commit} waits: for its window to close, behind the blob before it, and for its own blob's write;
 * and, per blob, how that write splits between recording an epoch, checking shards new to the epoch, and the PUT.
 */
public final class NodeLogCommitTimings {

    private final java.util.concurrent.atomic.LongAdder callers = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder windowNanos = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder behindNanos = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder writeNanos = new java.util.concurrent.atomic.LongAdder();
    final java.util.concurrent.atomic.LongAdder epochNanos = new java.util.concurrent.atomic.LongAdder();
    final java.util.concurrent.atomic.LongAdder admissionNanos = new java.util.concurrent.atomic.LongAdder();
    final java.util.concurrent.atomic.LongAdder admissionChecks = new java.util.concurrent.atomic.LongAdder();
    final java.util.concurrent.atomic.LongAdder putNanos = new java.util.concurrent.atomic.LongAdder();
    final java.util.concurrent.atomic.LongAdder puts = new java.util.concurrent.atomic.LongAdder();
    final java.util.concurrent.atomic.LongAdder hedges = new java.util.concurrent.atomic.LongAdder();
    final java.util.concurrent.atomic.LongAdder hedgesWon = new java.util.concurrent.atomic.LongAdder();
    final java.util.concurrent.atomic.LongAdder epochsOpened = new java.util.concurrent.atomic.LongAdder();

    NodeLogCommitTimings() {}

    void caller(long joinedNanos, NodeLogWindow window) {
        if (window.flushStartedNanos == 0L) {
            return;
        }
        final long waited = Math.max(0L, window.flushStartedNanos - joinedNanos);
        final long forWindow = Math.min(waited, Math.max(0L, window.closesAtNanos - joinedNanos));
        callers.increment();
        windowNanos.add(forWindow);
        behindNanos.add(waited - forWindow);
        writeNanos.add(Math.max(0L, window.doneNanos - window.flushStartedNanos));
    }

    private static double mean(java.util.concurrent.atomic.LongAdder nanos, java.util.concurrent.atomic.LongAdder count) {
        final long n = count.sum();
        return n == 0 ? 0 : nanos.sum() / 1e6 / n;
    }

    /**
     * Returns how many PUTs of blobs were tried since the last reset.
     *
     * @return the count
     */
    public long puts() {
        return puts.sum();
    }

    /**
     * Returns how many blobs were sent a second time since the last reset: the PUTs hedging added.
     *
     * @return the count
     */
    public long hedges() {
        return hedges.sum();
    }

    /**
     * Returns how many of those hedges landed before the PUT they repeated.
     *
     * @return the count
     */
    public long hedgesWon() {
        return hedgesWon.sum();
    }

    /** Starts the sums over. */
    public void reset() {
        for (java.util.concurrent.atomic.LongAdder each : List.of(
            callers,
            windowNanos,
            behindNanos,
            writeNanos,
            epochNanos,
            admissionNanos,
            admissionChecks,
            putNanos,
            puts,
            hedges,
            hedgesWon,
            epochsOpened
        )) {
            each.reset();
        }
    }

    /**
     * Describes the means, for a log line: per caller, the wait for its window to close, the wait behind the blob in
     * flight, and its own blob's write; per blob, recording an epoch, checking new shards, and the PUT.
     *
     * @return the means in milliseconds
     */
    public String describe() {
        return String.format(
            java.util.Locale.ROOT,
            "callers=%d window=%.1f behind=%.1f write=%.1f | blobs=%d epoch=%.1f admission=%.1f (%d head reads) put=%.1f ms"
                + " | hedged=%d won=%d | epochs opened=%d",
            callers.sum(),
            mean(windowNanos, callers),
            mean(behindNanos, callers),
            mean(writeNanos, callers),
            puts.sum(),
            mean(epochNanos, puts),
            mean(admissionNanos, puts),
            admissionChecks.sum(),
            mean(putNanos, puts),
            hedges.sum(),
            hedgesWon.sum(),
            epochsOpened.sum()
        );
    }
}
