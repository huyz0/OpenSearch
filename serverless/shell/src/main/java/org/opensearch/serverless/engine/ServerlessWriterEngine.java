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
import java.util.function.Function;

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
 * term-scoped log: a writer at term T appends only under {@code wal/t=T}, and
 * {@link WalStore#replayable()} reads terms in ascending order up to the successor's own. What that does
 * <em>not</em> yet close is a writer which has lost its head but not yet noticed, and which goes on
 * appending correctly-tagged records at its own still-valid-looking term. That window is real, is the
 * same one {@code plugins/serverless-storage} documents as open in its own WAL, and is recorded in
 * {@code m48-sequence-numbers-notes.md} rather than papered over here.
 */
public final class ServerlessWriterEngine extends InternalEngine {

    private final Function<ShardId, WalStore> replayLog;

    /**
     * Creates the engine.
     *
     * @param config core's engine configuration
     * @param replayLog supplies the log a shard should replay on open, or null for a shard that should
     *     not replay — a reader, a frozen view, or a writer opening a shard with no log behind it
     */
    public ServerlessWriterEngine(EngineConfig config, Function<ShardId, WalStore> replayLog) {
        super(config);
        this.replayLog = replayLog;
    }

    @Override
    public List<Translog.Operation> engineRecoveryOperations() {
        if (replayLog == null) {
            return List.of();
        }
        final ShardId shardId = config().getShardId();
        final WalStore wal = replayLog.apply(shardId);
        if (wal == null) {
            return List.of();
        }
        try {
            final List<Translog.Operation> operations = new ArrayList<>();
            long highest = org.opensearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
            for (WalRecord record : wal.replayable()) {
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
