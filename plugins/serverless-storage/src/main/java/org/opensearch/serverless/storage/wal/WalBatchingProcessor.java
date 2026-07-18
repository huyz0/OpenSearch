/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.BufferedAsyncIOProcessor;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.serverless.storage.security.EncryptionKeyProvider;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The one node-shared group-commit processor for WAL mirroring (rfc-serverless-opensearch.md
 * &sect;6.4), enabled by {@code serverless_storage.wal_flush.batching.enabled}. Every writer shard's
 * {@link org.opensearch.serverless.storage.translog.WalMirroringTranslog#add} enqueues its record
 * here via {@link #put}; once per {@code serverless_storage.wal_flush.interval} tick a single drain
 * writes every record enqueued in that window into one chunk, replacing the one-PUT-per-operation
 * cost the synchronous legacy path pays.
 *
 * <p>This is a thin subclass of core's own {@link BufferedAsyncIOProcessor} -- the exact
 * "batch many concurrent callers onto one shared periodic flush, notify each caller via a listener
 * when its batch completes" primitive core's {@code RemoteFsTranslog} already uses for remote-store
 * translog upload group-commit. The queueing, the one-drain-at-a-time promise semaphore, the
 * per-caller success/failure notification, and the {@link java.util.concurrent.ArrayBlockingQueue}
 * backpressure once the queue fills are all inherited from that base class, proven correct there;
 * this subclass only supplies the actual chunk write and the thread pool the drain runs on.
 *
 * <p><b>Encryption</b> is applied here rather than by the per-shard {@code EncryptingWalChunkService}
 * decorator the legacy path uses: in batching mode the translog never routes through that decorator
 * (it enqueues a plaintext record straight to this processor), so this drain re-applies the same
 * per-record {@link WalRecordCrypto#encrypt} the decorator would have, keyed off the node-level
 * {@link EncryptionKeyProvider}. A single node-level provider covers a mixed batch spanning several
 * indices because {@code WalRecordCrypto} derives each record's key from its own {@code indexUuid}.
 *
 * <p><b>Per-shard fairness</b> (the budget {@link WalChunkService} applies on the legacy path) is
 * deliberately <em>not</em> reapplied here: an interval-driven drain empties the entire queue every
 * tick regardless of which shard populated it, so no shard's records can wait longer than one buffer
 * interval behind another shard's volume -- the specific starvation the budget guards against on the
 * legacy path is structurally closed here, and the queue capacity is the uniform memory bound
 * instead (see {@code ServerlessStoragePlugin#SERVERLESS_STORAGE_WAL_FLUSH_QUEUE_CAPACITY_SETTING}).
 */
public final class WalBatchingProcessor extends BufferedAsyncIOProcessor<WalRecord> {

    private final WalChunkService walChunkService;
    private final EncryptionKeyProvider encryptionKeyProvider;

    /**
     * The highest chunk sequence a drain has confirmed durably written -- the batching-path source
     * for {@code WalMirroringTranslog#lastFlushedWalChunkSequence()}. Because chunk sequences under
     * the shared epoch are strictly increasing regardless of which shard's record advanced them (see
     * {@link WalChunkService}'s javadoc), this single node-level value is a valid resume lower bound
     * for every shard, exactly the granularity the manifest's {@code WalPosition} needs.
     */
    private volatile long lastWrittenChunkSequence = -1;

    /**
     * The cumulative buffered-payload size at which a drain is triggered immediately instead of
     * waiting for the next interval tick; non-positive disables this early-flush trigger, leaving
     * batching purely interval-driven. See {@code
     * ServerlessStoragePlugin#SERVERLESS_STORAGE_WAL_FLUSH_BYTE_THRESHOLD_SETTING}.
     */
    private final long bufferByteThreshold;

    /**
     * The cumulative payload size of every record currently enqueued or mid-write (put but not yet
     * notified) -- the real-time backlog {@link #isOverBacklogRejectThreshold()} checks, distinct from
     * {@code bufferedBytes} in the base class which resets every drain and only drives the early-flush
     * trigger. Incremented in {@link #put}, decremented once a batch's write attempt finishes
     * (successfully or not -- either way those records are no longer backlog).
     */
    private final AtomicLong backlogBytes = new AtomicLong();

    /**
     * The backlog threshold above which {@link #isOverBacklogRejectThreshold()} reports true, so a
     * caller can reject new writes with backpressure instead of enqueueing them (rfc-serverless-
     * opensearch.md &sect;6.4: "the WAL buffer is bounded; when upload backlog crosses a threshold the
     * node rejects new indexing with 429"). Non-positive disables rejection -- the queue's own bounded
     * capacity remains the only backstop, which blocks the calling thread rather than rejecting it.
     */
    private final long backlogRejectThreshold;

    /**
     * Creates the shared processor.
     *
     * @param logger the logger the base class logs drain failures through
     * @param queueCapacity the bounded queue capacity; a {@link #put} once full blocks until a drain frees space
     * @param threadContext the thread context the base class preserves across each queued listener
     * @param threadPool the thread pool each interval-scheduled drain runs on
     * @param bufferIntervalSupplier supplies the group-commit interval between drains
     * @param bufferByteThreshold cumulative buffered-payload size that triggers an immediate drain; non-positive disables it
     * @param backlogRejectThreshold cumulative backlog size above which {@link #isOverBacklogRejectThreshold()} reports true; non-positive disables it
     * @param walChunkService the chunk service each drain group-commits its batch into
     * @param encryptionKeyProvider {@code null} leaves records unencrypted; non-null encrypts each record's payload before it is written
     */
    public WalBatchingProcessor(
        Logger logger,
        int queueCapacity,
        ThreadContext threadContext,
        ThreadPool threadPool,
        Supplier<TimeValue> bufferIntervalSupplier,
        long bufferByteThreshold,
        long backlogRejectThreshold,
        WalChunkService walChunkService,
        EncryptionKeyProvider encryptionKeyProvider
    ) {
        super(logger, queueCapacity, threadContext, threadPool, bufferIntervalSupplier);
        this.bufferByteThreshold = bufferByteThreshold;
        this.backlogRejectThreshold = backlogRejectThreshold;
        this.walChunkService = walChunkService;
        this.encryptionKeyProvider = encryptionKeyProvider;
    }

    @Override
    public void put(WalRecord item, Consumer<Exception> listener) {
        backlogBytes.addAndGet(item.payload().length);
        super.put(item, listener);
    }

    @Override
    protected void write(List<Tuple<WalRecord, Consumer<Exception>>> candidates) throws IOException {
        // batchBytes is computed from the candidates themselves (their payload lengths are exactly
        // what put() already added to backlogBytes at enqueue time), so the decrement below is
        // correct regardless of whether encryption below succeeds -- computing it up front, before
        // the try, lets the try/finally cover the encryption loop too. Encryption failing partway
        // through a batch must still release those bytes from the backlog counter: put() already
        // counted every candidate's bytes, and there is no other decrement site, so a failure here
        // that skipped the finally would leak backlogBytes permanently (compounding on every retry)
        // and eventually wedge WalMirroringTranslog#add into rejecting all future writes forever.
        long batchBytes = 0;
        for (Tuple<WalRecord, Consumer<Exception>> candidate : candidates) {
            batchBytes += candidate.v1().payload().length;
        }
        try {
            List<WalRecord> records = new ArrayList<>(candidates.size());
            for (Tuple<WalRecord, Consumer<Exception>> candidate : candidates) {
                WalRecord record = candidate.v1();
                records.add(encryptionKeyProvider == null ? record : WalRecordCrypto.encrypt(record, encryptionKeyProvider));
            }
            long chunkSequence = walChunkService.writeChunkWithRetry(records);
            if (chunkSequence >= 0) {
                lastWrittenChunkSequence = chunkSequence;
            }
            // The base class notifies every candidate's listener with null (success) once this returns
            // without throwing, or with the thrown exception if it does -- both handled there, not here.
        } finally {
            // Decremented on every path, success or failure (including an encryption failure that
            // never reaches writeChunkWithRetry): either way these records are no longer sitting in
            // the backlog once this write attempt is done -- a failed batch's callers get the
            // exception and, per WalMirroringTranslog#ensureSynced's contract, must retry the whole
            // operation (which re-enqueues and re-counts it), not silently resurrect the old backlog.
            backlogBytes.addAndGet(-batchBytes);
        }
    }

    @Override
    protected String getBufferProcessThreadPoolName() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected long getBufferByteThreshold() {
        return bufferByteThreshold > 0 ? bufferByteThreshold : -1;
    }

    @Override
    protected long itemSizeInBytes(WalRecord record) {
        return record.payload().length;
    }

    /** The highest chunk sequence a drain has confirmed durably written, or {@code -1} if none yet -- see {@link #lastWrittenChunkSequence}. */
    public long lastWrittenChunkSequence() {
        return lastWrittenChunkSequence;
    }

    /** The current WAL upload backlog in bytes -- see {@link #backlogBytes}. */
    public long backlogBytes() {
        return Math.max(0, backlogBytes.get());
    }

    /**
     * Whether the current backlog exceeds {@link #backlogRejectThreshold}, the signal a caller (see
     * {@code WalMirroringTranslog#add}) uses to reject a new write with backpressure instead of
     * enqueueing it. Always {@code false} when rejection is disabled ({@code backlogRejectThreshold <= 0}).
     */
    public boolean isOverBacklogRejectThreshold() {
        return backlogRejectThreshold > 0 && backlogBytes() >= backlogRejectThreshold;
    }
}
