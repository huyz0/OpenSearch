/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.translog;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.translog.LocalTranslog;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.opensearch.serverless.storage.wal.WalAppendTarget;
import org.opensearch.serverless.storage.wal.WalBatchingProcessor;
import org.opensearch.serverless.storage.wal.WalChunkService;
import org.opensearch.serverless.storage.wal.WalRecord;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * A {@link LocalTranslog} that additionally mirrors every appended operation into the node-level
 * {@link WalChunkService}, per rfc-serverless-opensearch.md &sect;6.4. Local translog files remain
 * the source of truth for recovery on this node exactly as they are today; the WAL chunk mirror is
 * what makes the operation durable against loss of this node, ahead of the next segment bundle and
 * commit manifest.
 *
 * <p>This deliberately keeps the entire recovery/replay/generation-rolling machinery of {@link
 * Translog} untouched -- only {@link #add} and, on the batching path, {@link #ensureSynced} are
 * overridden -- rather than reimplementing translog file management on top of the WAL chunk format.
 *
 * <p><b>Two paths, chosen by whether a group-commit processor is attached</b> (see {@link
 * WalAppendTarget#batchingProcessor()}):
 * <ul>
 *   <li><b>Legacy (no processor):</b> {@link #add} synchronously appends and flushes the record to
 *       the object store before returning -- roughly one PUT per operation, unchanged from before
 *       batching existed.
 *   <li><b>Batching (processor attached):</b> {@link #add} appends locally then enqueues the record
 *       onto the shared {@link WalBatchingProcessor} and returns immediately without blocking on any
 *       object-store I/O; a later {@link #ensureSynced} is what waits for that record's group-commit
 *       upload to complete. Whether the client's own write waits for it is governed entirely by the
 *       standard {@code index.translog.durability} setting, exactly as it already governs remote-store
 *       translog upload: {@code REQUEST} calls {@link #ensureSynced} on the request thread before
 *       acking, {@code ASYNC} lets a background sync task call it instead. No plugin-specific "wait
 *       or not" knob is introduced -- this reuses core's own {@code RemoteFsTranslog} precedent.
 * </ul>
 */
public class WalMirroringTranslog extends LocalTranslog {

    /**
     * The bounded retry budget a transient object-store error mirroring one operation gets before
     * the failure propagates to the caller. The retry policy itself now lives on {@link
     * WalChunkService#writeChunkWithRetry} (shared with the group-commit batching path) rather than
     * being reimplemented here; this alias is retained so existing tests asserting on the budget
     * keep naming it from the caller's vantage point.
     */
    static final int MAX_MIRROR_FLUSH_ATTEMPTS = WalChunkService.MAX_WRITE_ATTEMPTS;

    private final WalAppendTarget walChunkService;
    private final String indexUuid;
    private final int shardId;
    private final LongSupplier primaryTermSupplier;

    /**
     * The node-shared group-commit processor, or {@code null} on the legacy synchronous path. Its
     * presence is the batching-enabled signal (see this class's javadoc); derived once at
     * construction from {@link WalAppendTarget#batchingProcessor()}.
     */
    private final WalBatchingProcessor batchingProcessor;

    /**
     * The batching path's per-op completion handle: {@link #add} stores the future for the record it
     * just enqueued here, and {@link #ensureSynced} waits on it. Waiting on the <em>most recent</em>
     * enqueued future is sufficient -- the shared processor drains the whole queue in FIFO order one
     * batch at a time, so when this shard's latest record's batch completes, every earlier record it
     * enqueued has necessarily been written too. {@code null} until the first {@link #add} on the
     * batching path. Unused on the legacy path.
     */
    private volatile CompletableFuture<Void> latestPendingWalFuture;

    /** The chunk sequence, under this WAL service's shared epoch, most recently confirmed durably written on the legacy path -- see {@link #lastFlushedWalChunkSequence()}. */
    private volatile long lastFlushedWalChunkSequence = -1;

    /**
     * Creates a local translog that additionally mirrors every appended operation into {@code walChunkService}.
     *
     * @param config the translog configuration, forwarded to {@link LocalTranslog}
     * @param translogUUID the translog UUID, forwarded to {@link LocalTranslog}
     * @param deletionPolicy the deletion policy, forwarded to {@link LocalTranslog}
     * @param globalCheckpointSupplier supplies the current global checkpoint, forwarded to {@link LocalTranslog}
     * @param primaryTermSupplier supplies the current primary term, used both by {@link LocalTranslog}
     *                            and to tag each mirrored {@link WalRecord}
     * @param persistedSequenceNumberConsumer notified of persisted sequence numbers, forwarded to {@link LocalTranslog}
     * @param translogOperationHelper helper used to read/write operations, forwarded to {@link LocalTranslog}
     * @param walChunkService the node-level WAL chunk service every appended operation is mirrored into
     */
    public WalMirroringTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        TranslogOperationHelper translogOperationHelper,
        WalAppendTarget walChunkService
    ) throws IOException {
        super(
            config,
            translogUUID,
            deletionPolicy,
            globalCheckpointSupplier,
            primaryTermSupplier,
            persistedSequenceNumberConsumer,
            translogOperationHelper,
            null
        );
        this.walChunkService = walChunkService;
        this.indexUuid = config.getShardId().getIndex().getUUID();
        this.shardId = config.getShardId().getId();
        this.primaryTermSupplier = primaryTermSupplier;
        this.batchingProcessor = walChunkService.batchingProcessor();
    }

    @Override
    public Location add(final Operation operation) throws IOException {
        Location location = super.add(operation);
        WalRecord record = toWalRecord(operation);
        if (batchingProcessor != null) {
            // Explicit backpressure (rfc-serverless-opensearch.md &sect;6.4): once the upload backlog
            // crosses serverless_storage.wal_flush.backlog_reject_threshold, reject this write instead
            // of enqueueing it and letting the backlog (and the memory it holds) keep growing. The
            // local super.add() above has already happened, so the operation is not silently lost --
            // the caller (indexing/replication path) sees a clean rejection and the client can retry,
            // rather than the object store's own slowdown surfacing later as unbounded node memory
            // growth or an indefinitely blocked indexing thread once the queue eventually fills.
            if (batchingProcessor.isOverBacklogRejectThreshold()) {
                throw new OpenSearchRejectedExecutionException(
                    "rejected WAL mirror write: upload backlog ["
                        + batchingProcessor.backlogBytes()
                        + " bytes] exceeds configured threshold for ["
                        + indexUuid
                        + "]["
                        + shardId
                        + "]"
                );
            }
            // Batching path: enqueue and return immediately. The indexing thread is no longer held
            // for any object-store I/O -- the shared processor group-commits this record (and every
            // other shard's records enqueued in the same interval) into one chunk on its own thread.
            // Durability is deferred to ensureSynced(); under index.translog.durability=REQUEST the
            // request thread calls that before acking, under ASYNC a background sync task does.
            CompletableFuture<Void> future = new CompletableFuture<>();
            batchingProcessor.put(record, exception -> {
                if (exception != null) {
                    future.completeExceptionally(exception);
                } else {
                    future.complete(null);
                }
            });
            latestPendingWalFuture = future;
            return location;
        }
        // Legacy path: flushing per-operation gives the same per-write durability guarantee a local
        // translog fsync gives. WalChunkService#flush applies the bounded retry-with-backoff itself
        // (see #flushWithRetry's javadoc), so this call already carries that policy.
        walChunkService.append(record);
        flushWithRetry();
        return location;
    }

    /** Serializes {@code operation} into the plaintext {@link WalRecord} tagged with this shard's identity and current primary term. */
    private WalRecord toWalRecord(Operation operation) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        Operation.writeOperation(out, operation);
        return new WalRecord(
            indexUuid,
            shardId,
            primaryTermSupplier.getAsLong(),
            operation.seqNo(),
            org.opensearch.core.common.bytes.BytesReference.toBytes(out.bytes())
        );
    }

    /**
     * On the batching path, blocks until this shard's most recently enqueued WAL record has been
     * group-committed to the object store, after first ensuring the local translog is fsynced up to
     * {@code location} exactly as {@link LocalTranslog} does. This is the method
     * {@code index.translog.durability=REQUEST} drives on the client's own request thread (via
     * {@code IndexShard.sync()} -> {@code translogSyncProcessor} -> {@code
     * TranslogManager#ensureTranslogSynced} -> {@code Translog#ensureSynced(Stream)} -> here),
     * making a WAL-mirrored write's ack wait for its durable upload; under {@code ASYNC} the same
     * call happens on a background sync task instead. No bespoke timeout is added, matching {@code
     * RemoteFsTranslog#ensureSynced}'s own precedent -- the overall request-level timeout bounds the
     * wait. On the legacy path there is nothing extra to wait for (the mirror write already completed
     * inside {@link #add}), so this simply defers to {@link LocalTranslog}.
     *
     * @param location the translog location to ensure is durable (local fsync, plus the WAL mirror upload on the batching path).
     * @return {@code true} iff this call caused an actual local sync, exactly as {@link LocalTranslog#ensureSynced} reports it.
     */
    @Override
    public boolean ensureSynced(Location location) throws IOException {
        boolean localResult = super.ensureSynced(location);
        if (batchingProcessor == null) {
            return localResult;
        }
        CompletableFuture<Void> pending = latestPendingWalFuture;
        if (pending != null) {
            try {
                pending.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting for WAL mirror group-commit upload", e);
            } catch (ExecutionException e) {
                // Preserve the original failure type: the processor's write() propagates the
                // IOException WalChunkService#writeChunkWithRetry threw after exhausting its retries,
                // matching the legacy path's own "add throws IOException on exhaustion" contract.
                Throwable cause = e.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new IOException("WAL mirror group-commit upload failed", cause);
            }
        }
        return localResult;
    }

    /**
     * The highest chunk sequence, under this WAL service's shared epoch, that a {@link
     * #flushWithRetry} call from this translog has confirmed durably written -- {@code -1} if
     * none yet. This is what {@code ObjectStoreCommitHeadPublisher#publishCommitAsHead}'s caller
     * uses to build the manifest's real {@code WalPosition} instead of a placeholder, so recovery
     * knows where to resume reading from (rfc-serverless-opensearch.md &sect;7.1's failover
     * description). Because chunk sequences under a shared epoch are strictly increasing
     * regardless of which shard's flush most recently advanced them, this value only ever needs
     * to be a lower bound safe to resume scanning from -- replay's own per-record
     * {@code (indexUuid, shardId, primaryTerm)} filter (see {@link WalRecord}'s javadoc) is what
     * actually determines which records apply, not this position alone.
     *
     * <p>On the batching path this reads the shared processor's own last-written sequence rather than
     * this shard's field: the same node-shared, strictly-increasing sequence is the correct resume
     * lower bound for every shard, and the group-commit that advances it happens off this translog's
     * threads (see {@link WalBatchingProcessor#lastWrittenChunkSequence()}).
     */
    public long lastFlushedWalChunkSequence() {
        if (batchingProcessor != null) {
            return batchingProcessor.lastWrittenChunkSequence();
        }
        return lastFlushedWalChunkSequence;
    }

    /**
     * The operation is already durable in the local translog by the time this runs (see {@link
     * #add}), so a transient failure writing its WAL mirror chunk should not immediately fail the
     * whole engine -- {@link WalChunkService#flush} retries a few times first (see {@link
     * WalChunkService#writeChunkWithRetry}, where that policy now lives). {@code flush} re-attempts
     * writing everything currently buffered (not just this operation's record), so a retry never
     * loses or duplicates records: on success the buffer is drained exactly once.
     */
    private void flushWithRetry() throws IOException {
        long chunkSequence = walChunkService.flush();
        if (chunkSequence >= 0) {
            lastFlushedWalChunkSequence = chunkSequence;
        }
    }
}
