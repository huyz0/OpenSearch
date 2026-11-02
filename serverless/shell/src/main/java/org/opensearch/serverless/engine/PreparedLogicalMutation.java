/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.serverless.log.LogCheckpoint;
import org.opensearch.serverless.log.LogicalCommand;
import org.opensearch.serverless.log.LogicalOwnershipSession;
import org.opensearch.serverless.log.LogicalStreamRecord;
import org.opensearch.serverless.log.RatisJournal;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * Immutable proposal built from a successful native resolution and its current applied owner cut.
 * The host must reserve input/parsed/encoded bytes, perform retry lookup before preparation and
 * serialize preparation through verified logical serving application. A construction/submission
 * failure after speculative native preparation requires fencing that context. This class neither
 * owns native resources nor makes a historical owner projection into continuing write authority.
 */
@InternalApi
public final class PreparedLogicalMutation {
    private final LogicalStreamRecord assignment;
    private final LogicalCommand command;
    private final long epoch;
    private final long term;
    private final long schema;

    private PreparedLogicalMutation(LogicalStreamRecord assignment, LogicalCommand command, long epoch, long term, long schema) {
        this.assignment = assignment;
        this.command = command;
        this.epoch = epoch;
        this.term = term;
        this.schema = schema;
    }

    /**
     * Validates native/header identities and complete frame admission before irreversible proposal.
     * Body encoding starts only after the host's request byte reservation; native journal framing
     * subsequently uses the journal's own permit. No script is executed by this method.
     * @param assignment permanent catalog assignment
     * @param ownership current applied projection obtained from the serialized ownership session
     * @param mutation successful resolved native operation with its original stable request UUID
     * @param firstObservedIndex immutable initial group observation, retained across ambiguous retries
     * @param inputDigest 32-byte digest of the original request, not the resolved result
     * @return immutable correctly bound proposal
     * @throws IOException on stale/incomplete ownership, mismatched native identity or excess payload
     */
    public static PreparedLogicalMutation create(
        LogicalStreamRecord assignment,
        LogicalOwnershipSession.Ownership ownership,
        ResolvedMutation mutation,
        long firstObservedIndex,
        byte[] inputDigest
    ) throws IOException {
        Objects.requireNonNull(assignment);
        Objects.requireNonNull(ownership);
        Objects.requireNonNull(mutation);
        if (firstObservedIndex < -1 || Objects.requireNonNull(inputDigest).length != 32) {
            throw new IllegalArgumentException("invalid original logical request identity");
        }
        final var state = Objects.requireNonNull(ownership.state());
        final var barrier = Objects.requireNonNull(ownership.checkpoint());
        if (barrier.logId().equals(assignment.stream()) == false
            || barrier.index() < 0
            || barrier.entryTerm() < 1
            || state.owner() == null
            || state.producerEpoch() < 1
            || state.primaryTerm() < 1
            || state.retired()
            || state.schemaVersion() < 1
            || state.schemaCheckpoint() == null
            || state.schemaCheckpoint().logId().equals(assignment.stream()) == false
            || state.schemaCheckpoint().index() < 0
            || state.schemaCheckpoint().entryTerm() < 1
            || state.lastIndex() < barrier.index()
            || state.lastIndex() < state.schemaCheckpoint().index()
            || state.lastSequenceNumber() < -1
            || state.lastSequenceNumber() == Long.MAX_VALUE
            || mutation.sequenceNumber() != state.lastSequenceNumber() + 1
            || mutation.primaryTerm() != state.primaryTerm()) {
            throw new IOException("native resolution differs from applied logical owner, schema or sequence prefix");
        }
        final byte[] body = mutation.encode(LogicalCommand.maximumBodyBytes(assignment.maximumPayloadBytes()));
        final var command = new LogicalCommand(
            LogicalCommand.Action.MUTATION,
            mutation.operationId(),
            firstObservedIndex,
            inputDigest,
            state.owner(),
            mutation.sequenceNumber(),
            body
        );
        return new PreparedLogicalMutation(assignment, command, state.producerEpoch(), state.primaryTerm(), state.schemaVersion());
    }

    /** Returns the immutable proposal, including its original request identity; not a commit proof. */
    public LogicalCommand command() {
        return command;
    }

    /**
     * Proposes on the ready local journal leader with the validated bound envelope fields. A physical
     * reply still requires logical acceptance and native application before completing the host gate.
     * @param journal authorized local journal matching the permanent assignment
     * @return detached physical completion; not original-result or search-visibility proof
     * @throws IOException on changed history, producer, leadership or proposal admission
     */
    public CompletionStage<LogCheckpoint> appendTo(RatisJournal journal) throws IOException {
        return Objects.requireNonNull(journal).appendLogical(assignment, command, epoch, term, schema);
    }
}
