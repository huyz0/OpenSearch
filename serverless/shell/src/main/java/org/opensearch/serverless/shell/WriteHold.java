/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shell;

import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A hold on a shard's writes, for a caller that hands the shard's documents elsewhere, such as a shard split.
 *
 * <p>Taken with {@link WriteFences#hold}, which returns once every write already under way is acknowledged, so the
 * node log holds all of them. From then on a write to the shard waits, and nothing is applied. The holder ends the
 * wait one of two ways: {@link #resume} lets the waiting writes, and every later one, go on here as before;
 * {@link #forwardTo} hands each waiting write, and every later one, to a {@link Target} instead, until the holder
 * {@link #close}s the hold. A hold the holder neither resumes nor forwards within its time lets the writes go on
 * here, as a resume would, so a holder that dies cannot stall a shard; the holder learns of it from the answer of its
 * own {@link #forwardTo}.
 *
 * <p>A write waits for the hold before it is admitted, so it uses no admission slot while it waits, and a forwarded
 * write is admitted again where it is sent.
 */
public final class WriteHold implements AutoCloseable {

    /** Where a forwarded write goes: the same three calls the write path takes, for the same shard. */
    public interface Target {
        /**
         * Applies a forwarded index.
         *
         * @param shardId the shard the write was addressed to
         * @param id the document id
         * @param source the document source
         * @param ifSeqNo the sequence number the document must be at, or unassigned
         * @param ifPrimaryTerm the primary term the document must be at, or 0
         * @param requireAbsent whether the document must not exist yet
         * @param mayGrowMapping whether the write may add fields to the mapping
         * @return the outcome, as the caller is told it
         * @throws IOException if it fails
         */
        WriteOutcome index(
            ShardId shardId,
            String id,
            String source,
            long ifSeqNo,
            long ifPrimaryTerm,
            boolean requireAbsent,
            boolean mayGrowMapping
        ) throws IOException;

        /**
         * Applies a forwarded deletion.
         *
         * @param shardId the shard the write was addressed to
         * @param id the document id
         * @param ifSeqNo the sequence number the document must be at, or unassigned
         * @param ifPrimaryTerm the primary term the document must be at, or 0
         * @return the outcome, as the caller is told it
         * @throws IOException if it fails
         */
        WriteOutcome delete(ShardId shardId, String id, long ifSeqNo, long ifPrimaryTerm) throws IOException;

        /**
         * Applies a forwarded batch.
         *
         * @param shardId the shard the batch was addressed to
         * @param batch the operations, in request order
         * @return one outcome per operation, in the same order
         * @throws IOException if it fails
         */
        List<BulkOutcome> bulk(ShardId shardId, List<BulkOperation> batch) throws IOException;
    }

    /** Thrown inside the write path, before anything is applied, to send a write to the hold's target. */
    static final class Forwarded extends IOException {
        final transient Target target;

        Forwarded(ShardId shardId, Target target) {
            super("writes to " + shardId + " are forwarded");
            this.target = target;
        }
    }

    private static final int HOLDING = 0;
    private static final int RESUMED = 1;
    private static final int FORWARDED = 2;
    private static final int EXPIRED = 3;

    private final ShardId shardId;
    private final long deadlineNanos;
    private final Map<ShardId, WriteHold> holds;
    private final AtomicInteger state = new AtomicInteger(HOLDING);
    private final CountDownLatch ended = new CountDownLatch(1);
    private volatile Target target;

    WriteHold(ShardId shardId, long maxMillis, Map<ShardId, WriteHold> holds) {
        this.shardId = shardId;
        this.deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxMillis);
        this.holds = holds;
    }

    /**
     * Returns the shard this hold is on.
     *
     * @return the shard
     */
    public ShardId shardId() {
        return shardId;
    }

    /**
     * Lets the waiting writes, and every later one, go on here, and ends the hold.
     *
     * @return true if the hold was still in force, so no write went past it; false if it had already ended
     */
    public boolean resume() {
        final boolean wasHolding = state.compareAndSet(HOLDING, RESUMED);
        end();
        return wasHolding;
    }

    /**
     * Hands each waiting write, and every later one, to a target, until {@link #close}.
     *
     * @param to where the writes go
     * @return true if the hold was still in force, so every write since the hold went or will go to {@code to};
     *     false if it had ended, by a resume or by running out of time, and writes may have been applied here since
     */
    public boolean forwardTo(Target to) {
        target = to;
        if (state.compareAndSet(HOLDING, FORWARDED) == false) {
            return false;
        }
        ended.countDown();
        return true;
    }

    /** Ends the hold: writes go on here again, also after a {@link #forwardTo}. */
    @Override
    public void close() {
        state.compareAndSet(HOLDING, RESUMED);
        end();
    }

    private void end() {
        holds.remove(shardId, this);
        ended.countDown();
    }

    /**
     * Waits out the hold for a write about to enter the write path.
     *
     * @throws Forwarded if the write goes to the target instead
     * @throws IOException if the wait is interrupted
     */
    void await() throws IOException {
        if (state.get() == HOLDING) {
            try {
                final long left = deadlineNanos - System.nanoTime();
                if (left <= 0 || ended.await(left, TimeUnit.NANOSECONDS) == false) {
                    if (state.compareAndSet(HOLDING, EXPIRED)) {
                        ServerlessNode.logger.warn("the hold on {}'s writes ran out of time; its writes go on here", shardId);
                        end();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted waiting out the hold on " + shardId + "'s writes", e);
            }
        }
        if (state.get() == FORWARDED) {
            throw new Forwarded(shardId, target);
        }
    }

    /** Whether a write that got past {@link #await} must wait again. */
    boolean holding() {
        return state.get() == HOLDING;
    }
}
