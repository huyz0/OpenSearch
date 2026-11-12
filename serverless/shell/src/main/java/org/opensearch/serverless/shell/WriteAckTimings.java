/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * Where an acknowledged write spends its time on this node, step by step, summed over every write since the last reset.
 *
 * <p>A write's acknowledgement is the sum of the steps below, run one after another on the caller's thread. Each is
 * summed separately, so a step that waits on the store shows up as itself rather than inside a total. The node log's own
 * split of {@link Step#LOG} (window wait, waiting behind the blob in flight, the PUT) is the committer's to report; see
 * {@link org.opensearch.serverless.store.NodeLogCommitter#timings}.
 *
 * <p>Every write counts in {@link Step#TOTAL}; the steps before the log are timed for single-document index writes only,
 * so their means are of those. Two clock reads per step, on the write's own thread; nothing here waits or allocates.
 */
public final class WriteAckTimings {

    /** One step of the write path, in the order a write runs them. */
    public enum Step {
        /** Waiting for a hold to end, and admission against back-pressure. */
        ADMIT,
        /** Taking the shard's fence and the reconciler's write guard. */
        FENCE,
        /** The checks before the engine runs: write fence, and the incarnation read when it is due. */
        CHECK_BEFORE,
        /** The engine applying the operation, including any mapping growth. */
        APPLY,
        /** The node's own lease, and whether this term's log was begun here. */
        LEASE_CHECK,
        /** Logging the records: the window, the blob in flight before it, and the PUT. */
        LOG,
        /** Handing the acknowledged records to the shard's search nodes. */
        TAIL_PUSH,
        /** The checks after the records landed: the lease, and the incarnation read when it is due. */
        CHECK_AFTER,
        /** The local translog sync. */
        SYNC,
        /** The whole write, from the call to its return. */
        TOTAL
    }

    private final Map<Step, LongAdder> nanos = new EnumMap<>(Step.class);
    private final LongAdder writes = new LongAdder();

    WriteAckTimings() {
        for (Step step : Step.values()) {
            nanos.put(step, new LongAdder());
        }
    }

    void add(Step step, long elapsedNanos) {
        nanos.get(step).add(elapsedNanos);
        if (step == Step.TOTAL) {
            writes.increment();
        }
    }

    /**
     * Returns how many writes have been timed since the last reset.
     *
     * @return the count
     */
    public long writes() {
        return writes.sum();
    }

    /**
     * Returns the mean time a write spent in a step, in milliseconds.
     *
     * @param step the step
     * @return the mean; zero before any write
     */
    public double meanMillis(Step step) {
        final long count = writes.sum();
        return count == 0 ? 0 : nanos.get(step).sum() / 1e6 / count;
    }

    /** Starts the sums over. */
    public void reset() {
        for (LongAdder each : nanos.values()) {
            each.reset();
        }
        writes.reset();
    }

    /**
     * Describes the mean of every step, for a log line.
     *
     * @return the means, in step order
     */
    public String describe() {
        final StringBuilder out = new StringBuilder("writes=").append(writes());
        for (Step step : Step.values()) {
            out.append(' ').append(step.name().toLowerCase(Locale.ROOT)).append('=');
            out.append(String.format(Locale.ROOT, "%.1f", meanMillis(step)));
        }
        return out.append(" ms").toString();
    }
}
