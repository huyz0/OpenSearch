/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.serverless.store.WalStore;

import java.io.IOException;
import java.util.List;

/**
 * Core's own writer engine, plus the one thing this deployment's durability needs it to know: that some
 * of its history lives in an object-store log rather than in a local translog.
 *
 * <p><b>Why an engine at all, rather than replaying after the shard starts.</b> Replay used to happen in
 * {@code ShardReconciler} after {@code updateShardState} had marked the shard STARTED, which meant it had
 * to go in through {@code applyIndexOperationOnPrimary} — the path that <em>generates</em> a sequence
 * number. Every replayed operation therefore got a brand new one, and {@code _seq_no} meant nothing
 * across a failover. The operations have to be replayed as the operations they were, and core only
 * accepts a caller-supplied sequence number under a recovery origin, which in turn is only legal while
 * the shard is still RECOVERING — a window that closes before the reconciler ever regains control.
 *
 * <p>{@link org.opensearch.index.engine.Engine#engineRecoverySnapshot()} is the seam core provides for
 * exactly this, and its own documentation describes exactly this case. {@code IndexShard} calls it inside
 * recovery and feeds the result through {@code runTranslogRecovery} under
 * {@code Origin.LOCAL_TRANSLOG_RECOVERY}, which preserves each operation's sequence number, primary term
 * and version.
 *
 * <p><b>The fencing obligation, stated because core's javadoc requires it to be.</b> That javadoc warns
 * that replayed operations must be fenced against a competing writer which has taken the shard over since
 * they were written, or "a naive implementation reintroduces a lost-write bug". Here, that fencing is the
 * replay cutoff. A snapshot of how far the log had been written is taken when the replay is armed, and
 * nothing appended after it is read — so a writer which has lost its head but has not yet noticed, and
 * which goes on appending correctly-tagged records under its own still-valid-looking term, cannot have
 * that history replayed as though it had been acknowledged. See {@link WalStore#position()} for why a
 * snapshot is sufficient, and {@code m49-fencing-notes.md} for the window it does not close.
 */
public final class ServerlessWriterEngine extends InternalEngine {

    /**
     * Supplies the records a shard should replay on open, already bounded by the cutoff taken when the
     * replay was armed. Reading the log is I/O, so this throws rather than pretending it cannot fail.
     */
    @FunctionalInterface
    public interface ReplayLog {
        /**
         * Returns the write-ahead records a shard being opened must replay.
         *
         * @param shardId the shard being opened
         * @return the records to replay, in order; empty if this shard should not replay any
         * @throws IOException if the log cannot be read
         */
        List<WalRecord> recordsFor(ShardId shardId) throws IOException;

        default void forEachRecord(ShardId shardId, org.opensearch.common.CheckedConsumer<WalRecord, IOException> consumer)
            throws IOException {
            for (WalRecord record : recordsFor(shardId)) {
                consumer.accept(record);
            }
        }

        /** True transfers ownership until the reconciler completes legacy replay. */
        default boolean replayed(ShardId shardId, ReplaySpool spool) throws IOException {
            return false;
        }
    }

    private final ReplayLog replayLog;

    /**
     * Creates the engine.
     *
     * @param config core's engine configuration
     * @param replayLog supplies the records a shard should replay on open, or null for a shard that
     *     should not replay — a reader, a frozen view, or a writer opening a shard with no log behind it
     */
    public ServerlessWriterEngine(EngineConfig config, ReplayLog replayLog) {
        super(config);
        this.replayLog = replayLog;
    }

    private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(
        ServerlessWriterEngine.class
    );

    /**
     * Says what a recovery is about to replay over what it restored, and shouts about any record the replay would skip.
     *
     * <p>Core's recovery skips an operation whose sequence number the restored commit has already processed. That is
     * right when the commit holds that operation, and silent loss when it holds a different one under the same number:
     * a fleet run lost acknowledged writes whose records were in the log at a takeover and never reached the
     * successor's commit. Each record at or below the processed checkpoint is checked for its document in the commit:
     * one that is not there is logged as an error -- it may have been deleted since, which is not a loss -- and one
     * whose sequence number a different live document holds is a proven collision, and the open is refused. A shard
     * that will not open is an outage someone sees; a shard opened without an acknowledged write is a loss nobody does.
     */
    private void reportReplay(ShardId shardId, Translog.Snapshot operations) {
        final long processed = getProcessedLocalCheckpoint();
        long min = Long.MAX_VALUE;
        long max = -1L;
        final java.util.Set<Long> terms = new java.util.TreeSet<>();
        int atOrBelow = 0;
        int skippedAbsent = 0;
        String collision = null;
        try (org.opensearch.index.engine.Engine.Searcher searcher = acquireSearcher("serverless-replay-check", SearcherScope.INTERNAL)) {
            for (Translog.Operation operation; (operation = operations.next()) != null;) {
                min = Math.min(min, operation.seqNo());
                max = Math.max(max, operation.seqNo());
                if (terms.size() < 32) {
                    terms.add(operation.primaryTerm());
                }
                if (operation.seqNo() > processed || operation instanceof Translog.Index == false) {
                    continue;
                }
                atOrBelow++;
                final String id = ((Translog.Index) operation).id();
                final int found = searcher.count(
                    new org.apache.lucene.search.TermQuery(
                        new org.apache.lucene.index.Term(
                            org.opensearch.index.mapper.IdFieldMapper.NAME,
                            org.opensearch.index.mapper.Uid.encodeId(id)
                        )
                    )
                );
                if (found == 0) {
                    skippedAbsent++;
                    final String holder = holderOf(searcher, operation.seqNo());
                    logger.error(
                        "{} replay would SKIP record {} (seqNo {}, term {}): the restored commit has processed through {} and does not "
                            + "hold this document; seqNo {} is held there by {}",
                        shardId,
                        id,
                        operation.seqNo(),
                        operation.primaryTerm(),
                        processed,
                        operation.seqNo(),
                        holder == null ? "no live document (deleted since, or merged away)" : "document " + holder
                    );
                    if (holder != null && holder.equals(id) == false) {
                        collision = "record "
                            + id
                            + " at seqNo "
                            + operation.seqNo()
                            + " term "
                            + operation.primaryTerm()
                            + " collides with document "
                            + holder
                            + " in the restored commit";
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("could not check the replay of " + shardId + " against its restored commit", e);
        }
        if (collision != null) {
            throw new EngineException(
                shardId,
                "refusing to open: two acknowledged operations share a sequence number and replay would drop one -- " + collision
            );
        }
        logger.debug(
            "{} replaying {} records (seqNo {}..{}, terms {}) over a commit processed through {}; {} at or below it, {} absent from it",
            shardId,
            operations.totalOperations(),
            operations.totalOperations() == 0 ? "-" : min,
            operations.totalOperations() == 0 ? "-" : max,
            terms,
            processed,
            atOrBelow,
            skippedAbsent
        );
    }

    /** The id of the live document holding a sequence number in the commit, or null if none does. */
    private static String holderOf(org.opensearch.index.engine.Engine.Searcher searcher, long seqNo) throws IOException {
        final org.apache.lucene.search.TopDocs top = searcher.search(
            org.apache.lucene.document.LongPoint.newExactQuery(org.opensearch.index.mapper.SeqNoFieldMapper.NAME, seqNo),
            1
        );
        if (top.scoreDocs.length == 0) {
            return null;
        }
        final org.apache.lucene.document.Document stored = searcher.storedFields().document(top.scoreDocs[0].doc);
        final org.apache.lucene.util.BytesRef idBytes = stored.getBinaryValue(org.opensearch.index.mapper.IdFieldMapper.NAME);
        return idBytes == null
            ? "(unreadable id)"
            : org.opensearch.index.mapper.Uid.decodeId(
                java.util.Arrays.copyOfRange(idBytes.bytes, idBytes.offset, idBytes.offset + idBytes.length)
            );
    }

    @Override
    public Translog.Snapshot engineRecoverySnapshot() throws IOException {
        if (replayLog == null) {
            return super.engineRecoverySnapshot();
        }
        final ShardId shardId = config().getShardId();
        final ReplaySpool spool = ReplaySpool.captureExclusive(
            config().getTranslogConfig().getTranslogPath().getParent().resolve("wal-replay"),
            consumer -> replayLog.forEachRecord(shardId, consumer)
        );
        Translog.Snapshot snapshot = null;
        boolean returned = false;
        try {
            try (var check = spool.snapshot()) {
                reportReplay(shardId, check);
            }
            if (spool.modernCount() > 0) {
                // Replayed operations can overwrite restored documents. Disable the append-only fast
                // path through the entire replay before core applies any of those operations.
                advanceMaxSeqNoOfUpdatesOrDeletes(spool.highestSeqNo());
            }
            snapshot = spool.snapshot();
            if (replayLog.replayed(shardId, spool)) {
                returned = true;
                return snapshot;
            }
            final Translog.Snapshot replay = snapshot;
            final Translog.Snapshot owned = new Translog.Snapshot() {
                @Override
                public int totalOperations() {
                    return replay.totalOperations();
                }

                @Override
                public Translog.Operation next() throws IOException {
                    return replay.next();
                }

                @Override
                public void close() throws IOException {
                    try {
                        replay.close();
                    } finally {
                        spool.close();
                    }
                }
            };
            returned = true;
            return owned;
        } finally {
            if (returned == false) {
                try {
                    if (snapshot != null) {
                        snapshot.close();
                    }
                } finally {
                    spool.close();
                }
            }
        }
    }
}
