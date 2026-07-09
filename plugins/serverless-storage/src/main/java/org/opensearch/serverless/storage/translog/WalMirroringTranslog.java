/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.translog;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.index.translog.LocalTranslog;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.opensearch.serverless.storage.wal.WalAppendTarget;
import org.opensearch.serverless.storage.wal.WalChunkService;
import org.opensearch.serverless.storage.wal.WalRecord;

import java.io.IOException;
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
 * Translog} untouched -- only {@link #add} is overridden -- rather than reimplementing translog
 * file management on top of the WAL chunk format.
 */
public class WalMirroringTranslog extends LocalTranslog {

    /**
     * A transient object-store error mirroring one operation should not fail the whole engine on
     * the first blip: retry a bounded number of times with a short backoff before giving up and
     * propagating the failure to the caller.
     */
    static final int MAX_MIRROR_FLUSH_ATTEMPTS = 3;
    static final long RETRY_BASE_DELAY_MILLIS = 10;

    private final WalAppendTarget walChunkService;
    private final String indexUuid;
    private final int shardId;
    private final LongSupplier primaryTermSupplier;

    /** The chunk sequence, under this WAL service's shared epoch, most recently confirmed durably written -- see {@link #lastFlushedWalChunkSequence()}. */
    private volatile long lastFlushedWalChunkSequence = -1;

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
    }

    @Override
    public Location add(final Operation operation) throws IOException {
        Location location = super.add(operation);
        BytesStreamOutput out = new BytesStreamOutput();
        Operation.writeOperation(out, operation);
        walChunkService.append(
            new WalRecord(
                indexUuid,
                shardId,
                primaryTermSupplier.getAsLong(),
                operation.seqNo(),
                org.opensearch.core.common.bytes.BytesReference.toBytes(out.bytes())
            )
        );
        // Flushing per-operation gives the same per-write durability guarantee a local translog
        // fsync gives; a deployment that wants real cross-shard group-commit batching would flush
        // WalChunkService on a timer/size threshold from a shared scheduler instead, independent
        // of any single shard's Translog.
        flushWithRetry();
        return location;
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
     */
    public long lastFlushedWalChunkSequence() {
        return lastFlushedWalChunkSequence;
    }

    /**
     * The operation is already durable in the local translog by the time this runs (see {@link
     * #add}), so a transient failure writing its WAL mirror chunk should not immediately fail the
     * whole engine -- retry a few times first. {@link WalChunkService#flush} re-attempts writing
     * everything currently buffered (not just this operation's record), so a retry here never
     * loses or duplicates records: on success the buffer is drained exactly once.
     */
    private void flushWithRetry() throws IOException {
        for (int attempt = 1; attempt <= MAX_MIRROR_FLUSH_ATTEMPTS; attempt++) {
            try {
                long chunkSequence = walChunkService.flush();
                if (chunkSequence >= 0) {
                    lastFlushedWalChunkSequence = chunkSequence;
                }
                return;
            } catch (IOException e) {
                if (attempt == MAX_MIRROR_FLUSH_ATTEMPTS) {
                    throw e;
                }
                try {
                    Thread.sleep(RETRY_BASE_DELAY_MILLIS * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }
}
