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
import java.util.concurrent.ConcurrentSkipListMap;
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
     * The batching path's per-<em>location</em> completion handles: {@link #add} records, against
     * the exact {@link Location} it is about to return, the future that completes when that record's
     * group-commit upload lands; {@link #ensureSynced} waits on the entry covering the location it
     * was actually asked about.
     *
     * <p><b>This replaces a single {@code latestPendingWalFuture} field, which was a real
     * acknowledged-write-loss bug rather than a tidiness issue.</b> {@code Translog#add} is called
     * concurrently by many indexing threads on one primary (it holds only a read lock), and the old
     * code assigned that field <em>after</em> the enqueue, so two concurrent appends could interleave
     * as: A enqueues (batch B1), B enqueues (batch B2, drains strictly after B1), B assigns F2, A
     * assigns F1. The field then held the <em>older</em> future, and {@code ensureSynced} -- which
     * ignored its {@code location} argument entirely -- returned as soon as B1 landed. Operation 2
     * was acked to the client with its chunk not yet written; losing the node lost it, with no
     * manifest and no WAL chunk ever covering it. Making the field's update monotone would have
     * closed that particular interleaving but still leaves {@code ensureSynced} answering a question
     * it was not asked ("is the newest record durable?" instead of "is <em>this</em> record
     * durable?"), which is both stricter than needed under {@code ASYNC} and wrong for any caller
     * syncing an older location.
     *
     * <p>A {@link java.util.concurrent.ConcurrentSkipListMap} keyed by {@link Location} (which is
     * {@link Comparable}, ordered by translog generation then offset, i.e. exactly append order) is
     * what makes the per-location lookup possible without a lock: {@link #ensureSynced} takes
     * {@code floorEntry(location)} -- the newest record enqueued at or before the location it must
     * make durable. If that location's own entry is still present, that is precisely its future. If
     * it has already been removed, then it has already completed, and because the shared processor
     * drains one batch at a time in FIFO order, every earlier entry has completed too -- so the
     * older entry {@code floorEntry} returns instead is already done and waiting on it is a no-op.
     * Entries are removed by the completion callback itself, so the map never holds more than the
     * records currently in flight.
     *
     * <p>Empty until the first {@link #add} on the batching path; unused entirely on the legacy path.
     */
    private final ConcurrentSkipListMap<Location, CompletableFuture<Void>> pendingWalFuturesByLocation = new ConcurrentSkipListMap<>();

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
            // Registered BEFORE the enqueue, not after: the listener below can fire on the drain
            // thread the instant put() returns (or even inside put(), if the bounded queue rejects
            // or the calling thread is interrupted), and it removes this entry. Publishing the entry
            // first means the removal can never race ahead of the insertion and leave a completed
            // record's future stranded in the map forever. Inserting an already-registered,
            // not-yet-enqueued future is safe in the other direction too: the only reader is
            // ensureSynced, which is only ever called for a location this method has already
            // returned, i.e. strictly after the put() below has happened.
            pendingWalFuturesByLocation.put(location, future);
            try {
                batchingProcessor.put(record, exception -> {
                    // Remove before completing, so a waiter released by the completion can never
                    // observe a stale entry, and a failed batch leaks nothing.
                    pendingWalFuturesByLocation.remove(location, future);
                    if (exception != null) {
                        future.completeExceptionally(exception);
                    } else {
                        future.complete(null);
                    }
                });
            } catch (RuntimeException e) {
                // put() itself failing (rather than notifying the listener) means this record will
                // never be drained and its listener will never fire -- drop the entry we just
                // published, or every later ensureSynced whose floorEntry lands on it would block
                // forever on a future nothing will ever complete.
                pendingWalFuturesByLocation.remove(location, future);
                throw e;
            }
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
     * On the batching path, blocks until <em>the WAL record enqueued for {@code location}</em> has
     * been group-committed to the object store, after first ensuring the local translog is fsynced up
     * to {@code location} exactly as {@link LocalTranslog} does. The wait is per-location, not
     * "whatever was enqueued most recently" -- see {@link #pendingWalFuturesByLocation}'s own javadoc
     * for the acknowledged-write-loss race the latter had. This is the method
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
        // floorEntry, not get: see pendingWalFuturesByLocation's javadoc. The exact entry is what is
        // normally found; an older one is only ever returned once this location's own record has
        // already completed and been removed, and FIFO drain order makes that older entry already
        // complete too, so waiting on it returns immediately.
        java.util.Map.Entry<Location, CompletableFuture<Void>> entry = pendingWalFuturesByLocation.floorEntry(location);
        if (entry != null) {
            CompletableFuture<Void> pending = entry.getValue();
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
