/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.serverless.log.LogCheckpoint;
import org.opensearch.serverless.log.LogicalCommand;
import org.opensearch.serverless.log.LogicalOwnershipSession;
import org.opensearch.serverless.log.LogicalStreamRecord;
import org.opensearch.serverless.log.RatisJournal;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * Immutable result-only proposal tied to the actual admitted private native prefix. The host must
 * look up retries before execution and hold its ownership/schema, pending-request and lifecycle
 * gates throughout construction/submission/application. Native checkpoint equality guards against
 * hiding a speculative mutation or assigned gap behind a rejection. It is not a continuing lease,
 * native schema certificate or independent proof that the supplied client response is appropriate.
 * The host must bind verified history to this activation; matching counters alone do not prove content.
 */
@InternalApi
public final class PreparedLogicalResultOnly {
    private final LogicalStreamRecord assignment;
    private final LogicalCommand command;
    private final long epoch;
    private final long term;
    private final long schema;

    private PreparedLogicalResultOnly(LogicalStreamRecord assignment, LogicalCommand command, long epoch, long term, long schema) {
        this.assignment = assignment;
        this.command = command;
        this.epoch = epoch;
        this.term = term;
        this.schema = schema;
    }

    /**
     * Checks scope and actual max/processed native sequence before allocating encoded result bytes.
     * The host must reserve rendering/encoded bytes and classify the result as non-mutating. An
     * unchanged historical owner projection alone is insufficient if private execution assigned a slot.
     * @param assignment permanent catalog assignment
     * @param ownership current successfully applied session projection
     * @param preparation admitted private context used for this request
     * @param requestId stable original internal client request UUID
     * @param outcome immutable original no-op or non-mutating rejection response
     * @param firstObservedIndex immutable original group observation
     * @param inputDigest original-input SHA-256 digest
     * @return immutable result-only proposal consuming no document sequence
     * @throws IOException on changed binding, native progress or excess complete payload size
     */
    public static PreparedLogicalResultOnly create(
        LogicalStreamRecord assignment,
        LogicalOwnershipSession.Ownership ownership,
        PrivatePreparationResources preparation,
        UUID requestId,
        ResolvedResultOnly outcome,
        long firstObservedIndex,
        byte[] inputDigest
    ) throws IOException {
        Objects.requireNonNull(requestId);
        Objects.requireNonNull(outcome);
        final var state = LogicalPreparationBinding.require(assignment, ownership, firstObservedIndex, inputDigest);
        final var engine = Objects.requireNonNull(preparation).engine();
        final var shard = engine.config().getShardId();
        if (preparation.group().equals(assignment.group()) == false
            || shard.getIndex().getUUID().equals(assignment.stream().indexUuid()) == false
            || shard.id() != assignment.stream().shardNumber()
            || engine.config().getPrimaryTermSupplier().getAsLong() != state.primaryTerm()
            || engine.getSeqNoStats(SequenceNumbers.NO_OPS_PERFORMED).getMaxSeqNo() != state.lastSequenceNumber()
            || engine.getProcessedLocalCheckpoint() != state.lastSequenceNumber()) {
            throw new IOException("result-only proposal differs from its admitted private native prefix");
        }
        final var command = new LogicalCommand(
            LogicalCommand.Action.OUTCOME_ONLY,
            requestId,
            firstObservedIndex,
            inputDigest,
            state.owner(),
            -1,
            outcome.encode(LogicalCommand.maximumBodyBytes(assignment.maximumPayloadBytes()))
        );
        return new PreparedLogicalResultOnly(assignment, command, state.producerEpoch(), state.primaryTerm(), state.schemaVersion());
    }

    /** Returns the immutable proposal; it is neither a commitment nor refreshed-reader proof. */
    public LogicalCommand command() {
        return command;
    }

    /**
     * Proposes with validated bound envelope fields. A physical reply still requires semantic
     * acceptance and ordered consumer application before the host completes its pending gate.
     * @param journal authorized local journal matching this assignment
     * @return detached physical acknowledgement, not completed client authority
     * @throws IOException on changed history, leadership, producer or journal admission
     */
    public CompletionStage<LogCheckpoint> appendTo(RatisJournal journal) throws IOException {
        return Objects.requireNonNull(journal).appendLogical(assignment, command, epoch, term, schema);
    }
}
