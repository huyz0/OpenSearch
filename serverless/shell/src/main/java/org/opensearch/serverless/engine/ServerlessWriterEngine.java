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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
 * <p>{@link org.opensearch.index.engine.Engine#engineRecoveryOperations()} is the seam core provides for
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
    private void reportReplay(ShardId shardId, List<Translog.Operation> operations) {
        final long processed = getProcessedLocalCheckpoint();
        long min = Long.MAX_VALUE;
        long max = -1L;
        final java.util.Set<Long> terms = new java.util.TreeSet<>();
        int atOrBelow = 0;
        int skippedAbsent = 0;
        String collision = null;
        // Two operations in the log itself under one sequence number: core applies the first and skips the second,
        // and the publish after this open trims the record. A fleet run lost three acknowledged writes so, when a
        // writer's record landed past its successor's fence. The same operation twice -- a record kept for a fence
        // and replayed again -- is the same id at the same term, and is not one.
        final java.util.Map<Long, String> bySeqNo = new java.util.HashMap<>();
        for (Translog.Operation operation : operations) {
            final String identity = operation.opType() + " " + identityOf(operation) + " term " + operation.primaryTerm();
            final String earlier = bySeqNo.putIfAbsent(operation.seqNo(), identity);
            if (earlier != null && earlier.equals(identity) == false && collision == null) {
                collision = "seqNo " + operation.seqNo() + " is in the log twice: " + earlier + ", and " + identity;
                logger.error("{} replay would drop an acknowledged operation: {}", shardId, collision);
            }
        }
        try (org.opensearch.index.engine.Engine.Searcher searcher = acquireSearcher("serverless-replay-check", SearcherScope.INTERNAL)) {
            for (Translog.Operation operation : operations) {
                min = Math.min(min, operation.seqNo());
                max = Math.max(max, operation.seqNo());
                terms.add(operation.primaryTerm());
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
            operations.size(),
            operations.isEmpty() ? "-" : min,
            operations.isEmpty() ? "-" : max,
            terms,
            processed,
            atOrBelow,
            skippedAbsent
        );
    }

    /** The document an operation is about, or "-" for one about none. */
    private static String identityOf(Translog.Operation operation) {
        if (operation instanceof Translog.Index index) {
            return index.id();
        }
        if (operation instanceof Translog.Delete delete) {
            return delete.id();
        }
        return "-";
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
    public List<Translog.Operation> engineRecoveryOperations() {
        if (replayLog == null) {
            return List.of();
        }
        final ShardId shardId = config().getShardId();
        try {
            final List<Translog.Operation> operations = new ArrayList<>();
            long highest = org.opensearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
            for (WalRecord record : replayLog.recordsFor(shardId)) {
                if (record.hasSequenceIdentity() == false) {
                    // Written before the log carried sequence numbers. It cannot be replayed as the
                    // operation it was, because the record does not say which operation that is. Left for
                    // ShardReconciler to replay the old way, as a fresh primary operation, rather than
                    // dropped -- losing an acknowledged write to a format change would be far worse than
                    // replaying it with a new sequence number, which is all this ever did before.
                    continue;
                }
                highest = Math.max(highest, record.seqNo());
                if (record.isDeletion()) {
                    operations.add(new Translog.Delete(record.id(), record.seqNo(), record.primaryTerm(), record.version()));
                } else {
                    operations.add(
                        new Translog.Index(
                            record.id(),
                            record.seqNo(),
                            record.primaryTerm(),
                            record.version(),
                            record.source().getBytes(StandardCharsets.UTF_8),
                            null,
                            -1
                        )
                    );
                }
            }
            reportReplay(shardId, operations);
            if (operations.isEmpty() == false) {
                // Raise the update-or-delete watermark to cover what is about to be replayed, before it
                // is replayed. The engine's append-only fast path is only sound while every operation it
                // sees sits above this mark, and these operations do not: a replayed record may well
                // overwrite or delete a document that is already in the restored commit. Peer recovery in
                // classic OpenSearch raises the same watermark for the same reason before replaying a
                // primary's history, and without it the engine asserts -- which is how this surfaced.
                //
                // Deliberately the overall maximum rather than a narrower "which of these were updates"
                // count: setting it too high only forgoes an optimisation, while setting it too low is a
                // correctness bug that shows up as a lost overwrite.
                advanceMaxSeqNoOfUpdatesOrDeletes(highest);
            }
            return operations;
        } catch (IOException e) {
            // Not swallowed into an empty list. An unreadable log during recovery means the shard cannot
            // be rebuilt to the state its predecessor acknowledged, and opening it anyway would serve a
            // silently truncated index.
            throw new EngineException(shardId, "could not read the write-ahead log for replay", e);
        }
    }
}
