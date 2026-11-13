/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.common.lease.Releasable;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogException;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.TranslogRecoveryRunner;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.index.translog.listener.TranslogEventListener;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

/**
 * A writer's translog that keeps nothing: no file, no fsync, no operations in memory.
 *
 * <p><b>Why a writer can do without one.</b> An acknowledged write is durable because its record landed in the node's log in
 * the object store, and a shard is always opened from a published commit plus that log; nothing ever replays a local
 * translog. The disk translog was kept only because the engine expects one, and it cost every open writer two file
 * descriptors and every acknowledged write a local fsync.
 *
 * <p><b>What it still does.</b> Each added operation gets a location that grows, so refresh listeners ({@code wait_for})
 * still tell a refresh that covers a write from one that started before it. The highest sequence number added is kept. A
 * sync marks the sequence numbers added since the last one as persisted, as the disk translog's fsync does: the write path
 * syncs only after the record is in the node log, so the persisted checkpoint still trails what is durable. The
 * recovery hooks run as the disk translog runs them, over nothing, so the engine still flushes after an open.
 *
 * <p><b>What it gives up.</b> A realtime get of a document not yet refreshed cannot be read back from the translog, so the
 * engine refreshes the shard instead, which is the path core takes whenever the translog has no copy. That is a local
 * refresh, not a publish.
 */
public final class DiscardingTranslogManager implements TranslogManager {

    private final String translogUUID;
    private final TranslogDeletionPolicy deletionPolicy;
    private final LongSupplier globalCheckpoint;
    private final TranslogEventListener listener;
    private final AtomicLong operations = new AtomicLong();
    private final AtomicLong maxSeqNo = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
    private final LongConsumer markPersisted;
    /** Sequence numbers added since the last sync; drained by every sync, so it holds at most one write window's worth. */
    private long[] pending = new long[8];
    private int pendingCount;

    /**
     * Creates the manager.
     *
     * @param translogUUID the translog UUID the last commit names, which every later commit carries on
     * @param deletionPolicy the engine's translog deletion policy, which commits consult
     * @param globalCheckpoint the shard's global checkpoint, reported as synced since there is nothing to sync
     * @param listener the engine's translog events
     * @param markPersisted marks a sequence number as persisted in the engine's local checkpoint tracker
     */
    public DiscardingTranslogManager(
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpoint,
        TranslogEventListener listener,
        LongConsumer markPersisted
    ) {
        this.translogUUID = translogUUID;
        this.deletionPolicy = deletionPolicy;
        this.globalCheckpoint = globalCheckpoint;
        this.listener = listener;
        this.markPersisted = markPersisted;
    }

    @Override
    public void rollTranslogGeneration() throws TranslogException {}

    @Override
    public int recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long localCheckpoint, long recoverUpToSeqNo)
        throws IOException {
        listener.onBeginTranslogRecovery();
        translogRecoveryRunner.run(EMPTY);
        listener.onAfterTranslogRecovery();
        return 0;
    }

    @Override
    public synchronized boolean isTranslogSyncNeeded() {
        return pendingCount > 0;
    }

    @Override
    public boolean ensureTranslogSynced(Stream<Translog.Location> locations) {
        return markPendingPersisted();
    }

    @Override
    public void syncTranslog() {
        markPendingPersisted();
    }

    private boolean markPendingPersisted() {
        final long[] drained;
        final int count;
        synchronized (this) {
            if (pendingCount == 0) {
                return false;
            }
            drained = pending;
            count = pendingCount;
            pending = new long[Math.max(8, Math.min(count, 1024))];
            pendingCount = 0;
        }
        for (int i = 0; i < count; i++) {
            markPersisted.accept(drained[i]);
        }
        return true;
    }

    @Override
    public TranslogStats getTranslogStats() {
        return new TranslogStats();
    }

    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        return new Translog.Location(0, operations.get(), 0);
    }

    @Override
    public void trimUnreferencedTranslogFiles() {}

    @Override
    public boolean shouldRollTranslogGeneration() {
        return false;
    }

    @Override
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) {}

    @Override
    public void ensureCanFlush() {}

    @Override
    public void setMinSeqNoToKeep(long seqNo) {}

    @Override
    public int restoreLocalHistoryFromTranslog(long processedCheckpoint, TranslogRecoveryRunner translogRecoveryRunner) {
        return 0;
    }

    @Override
    public void skipTranslogRecovery() {}

    @Override
    public Translog.Operation readOperation(Translog.Location location) {
        return null;
    }

    @Override
    public Translog.Location add(Translog.Operation operation) {
        final long seqNo = operation.seqNo();
        maxSeqNo.accumulateAndGet(seqNo, Math::max);
        if (seqNo >= 0) {
            synchronized (this) {
                if (pendingCount == pending.length) {
                    pending = java.util.Arrays.copyOf(pending, pendingCount * 2);
                }
                pending[pendingCount++] = seqNo;
            }
        }
        return new Translog.Location(0, operations.incrementAndGet(), 1);
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(long fromSeqNo, long toSeqNo, boolean requiredFullRange) {
        throw new UnsupportedOperationException("a writer that keeps no local translog has no history in it");
    }

    @Override
    public void onDelete() {}

    @Override
    public Releasable drainSync() {
        return () -> {};
    }

    @Override
    public Translog.TranslogGeneration getTranslogGeneration() {
        return new Translog.TranslogGeneration(translogUUID, 1);
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return globalCheckpoint.getAsLong();
    }

    @Override
    public long getMaxSeqNo() {
        return maxSeqNo.get();
    }

    @Override
    public void trimUnreferencedReaders() {}

    @Override
    public boolean shouldPeriodicallyFlush(long localCheckpointOfLastCommit, long flushThreshold) {
        return false;
    }

    @Override
    public Exception getTragicExceptionIfClosed() {
        return null;
    }

    @Override
    public TranslogDeletionPolicy getDeletionPolicy() {
        return deletionPolicy;
    }

    @Override
    public String getTranslogUUID() {
        return translogUUID;
    }

    @Override
    public void close() {}

    private static final Translog.Snapshot EMPTY = new Translog.Snapshot() {
        @Override
        public int totalOperations() {
            return 0;
        }

        @Override
        public Translog.Operation next() {
            return null;
        }

        @Override
        public void close() {}
    };
}
