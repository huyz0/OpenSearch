/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogReader;
import org.opensearch.index.translog.TranslogWriter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Local translog retention driven by object-store durability instead of core's default
 * age/size/file-count floors (rfc-serverless-opensearch.md &sect;7.1.1's design writeup, Option
 * A: trim to the bare minimum as soon as it's safe, not a decaying margin).
 *
 * <p>A translog generation is deletable as soon as every operation it holds is covered by a
 * manifest generation that {@link ObjectStoreCommitHeadPublisher#publishCommitAsHead} has actually
 * returned {@code true} for -- not merely uploaded, published (see {@link
 * ObjectStoreCommitPublisher}'s own "never the reverse" invariant, and &sect;7.1.1's "precise
 * safety condition"). This is a strictly more aggressive floor than core's default
 * age/size/total-files retention: this plugin's failover path already recovers by manifest + WAL
 * replay, never by replaying local translog (&sect;7.1), so local translog beyond what a same-node
 * restart can use as a fast-path is not load-bearing for correctness. It only ever *narrows*
 * retention relative to what a lock (see {@link #getMinTranslogGenRequiredByLocks}) demands --
 * {@link #recordDurablePublication} can only raise the durability watermark, never lower it, and
 * generations still held by an open lock are never returned as deletable regardless of durability.
 *
 * <p>{@link #recordDurablePublication} is called by {@link ObjectStoreWriterEngine} after each
 * commit publish that returns success, not by anything in this class -- this class only ever reads
 * that watermark, it never reaches out to check durability itself, keeping this a pure, easily
 * testable policy object.
 */
public final class ObjectStoreDurabilityTranslogDeletionPolicy extends TranslogDeletionPolicy {

    /**
     * Creates a policy with no durability watermark recorded yet, so no translog generation is
     * initially eligible for deletion on durability grounds alone.
     */
    public ObjectStoreDurabilityTranslogDeletionPolicy() {}

    private final AtomicLong durablyPublishedMaxSeqNo = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

    /**
     * Monotonic: a call with a lower {@code maxSeqNo} than already recorded is a no-op, never a regression.
     *
     * @param maxSeqNo the maximum sequence number now known to be durably published
     */
    public void recordDurablePublication(long maxSeqNo) {
        durablyPublishedMaxSeqNo.accumulateAndGet(maxSeqNo, Math::max);
    }

    /** Test/inspection-only -- not used by this class's own retention decision, which always re-reads the live watermark. */
    long durablyPublishedMaxSeqNo() {
        return durablyPublishedMaxSeqNo.get();
    }

    @Override
    public synchronized long minTranslogGenRequired(List<TranslogReader> readers, TranslogWriter writer) throws IOException {
        long minByLocks = getMinTranslogGenRequiredByLocks();
        long durableMaxSeqNo = durablyPublishedMaxSeqNo.get();

        long minByDurability = writer.getGeneration();
        for (TranslogReader reader : readers) {
            if (reader.getMaxSeqNo() > durableMaxSeqNo) {
                // The oldest generation not fully covered by a durable publish -- it and every
                // generation after it (including the writer's own current one) must be retained.
                minByDurability = reader.getGeneration();
                break;
            }
        }

        return Math.min(minByDurability, minByLocks);
    }

    /**
     * Durability-driven, not size-driven -- see class javadoc. Retained only for interface compatibility.
     *
     * @param bytes ignored
     */
    @Override
    public void setRetentionSizeInBytes(long bytes) {}

    /**
     * Durability-driven, not age-driven -- see class javadoc. Retained only for interface compatibility.
     *
     * @param ageInMillis ignored
     */
    @Override
    public void setRetentionAgeInMillis(long ageInMillis) {}

    /**
     * Durability-driven, not file-count-driven -- see class javadoc. Retained only for interface compatibility.
     *
     * @param retentionTotalFiles ignored
     */
    @Override
    protected void setRetentionTotalFiles(int retentionTotalFiles) {}
}
