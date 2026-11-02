/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class LogicalStreamStateTests extends OpenSearchTestCase {
    private static final UUID FIRST = new UUID(1, 1);
    private static final UUID SECOND = new UUID(2, 2);

    public void testOwnershipSchemaAndSequenceAreEnforcedAcrossTakeoverAndRetirement() throws Exception {
        final var fixture = new Fixture(32);
        assertAccepted(fixture.step(command(LogicalCommand.Action.OWNERSHIP, FIRST, 0, -1), 1, 1, 0));
        assertRejected(
            LogicalStreamState.Rejection.MISSING_SCHEMA,
            fixture.step(command(LogicalCommand.Action.MUTATION, FIRST, 0, 0), 1, 1, 0)
        );
        assertAccepted(fixture.step(command(LogicalCommand.Action.SCHEMA, FIRST, 0, 1), 1, 1, 1));
        assertAccepted(fixture.step(command(LogicalCommand.Action.MUTATION, FIRST, 0, 2), 1, 1, 1));
        assertRejected(
            LogicalStreamState.Rejection.STALE_EPOCH,
            fixture.step(command(LogicalCommand.Action.OWNERSHIP, SECOND, 0, 3), 1, 2, 1)
        );
        assertAccepted(fixture.step(command(LogicalCommand.Action.OWNERSHIP, SECOND, 1, 4), 2, 2, 1));
        assertRejected(
            LogicalStreamState.Rejection.STALE_EPOCH,
            fixture.step(command(LogicalCommand.Action.MUTATION, FIRST, 1, 5), 1, 1, 1)
        );
        assertRejected(
            LogicalStreamState.Rejection.ACTOR_MISMATCH,
            fixture.step(command(LogicalCommand.Action.MUTATION, FIRST, 1, 6), 2, 2, 1)
        );
        assertRejected(
            LogicalStreamState.Rejection.TERM_MISMATCH,
            fixture.step(command(LogicalCommand.Action.MUTATION, SECOND, 1, 7), 2, 1, 1)
        );
        assertRejected(
            LogicalStreamState.Rejection.SCHEMA_MISMATCH,
            fixture.step(command(LogicalCommand.Action.MUTATION, SECOND, 1, 8), 2, 2, 2)
        );
        assertRejected(
            LogicalStreamState.Rejection.SEQUENCE_MISMATCH,
            fixture.step(command(LogicalCommand.Action.MUTATION, SECOND, 2, 9), 2, 2, 1)
        );
        assertEquals(0, fixture.state.view().lastSequenceNumber());
        assertAccepted(fixture.step(command(LogicalCommand.Action.MUTATION, SECOND, 1, 10), 2, 2, 1));
        assertAccepted(fixture.step(command(LogicalCommand.Action.RETIRE, SECOND, -1, 11), 2, 2, 1));
        assertRejected(LogicalStreamState.Rejection.RETIRED, fixture.step(command(LogicalCommand.Action.MUTATION, SECOND, 2, 12), 2, 2, 1));
        assertRejected(LogicalStreamState.Rejection.RETIRED, fixture.step(command(LogicalCommand.Action.OWNERSHIP, FIRST, 2, 13), 3, 3, 1));
        assertEquals(1, fixture.state.view().lastSequenceNumber());
        assertEquals(SECOND, fixture.state.view().owner());
        assertTrue(fixture.state.view().retired());
    }

    public void testRetryReturnsOriginalCheckpointWithoutReexecutingAgainstANewOwner() throws Exception {
        final var fixture = active(16);
        final var original = command(LogicalCommand.Action.MUTATION, FIRST, 0, 1);
        final var accepted = fixture.step(original, 1, 1, 1);
        assertAccepted(accepted);
        assertAccepted(fixture.step(command(LogicalCommand.Action.OWNERSHIP, SECOND, 1, 2), 2, 2, 1));
        final var retry = new LogicalCommand(
            LogicalCommand.Action.MUTATION,
            original.requestId(),
            original.firstObservedIndex(),
            original.requestDigest(),
            SECOND,
            1,
            new byte[] { 99 }
        );
        final var duplicate = fixture.step(retry, 2, 2, 1);
        assertEquals(LogicalStreamState.Status.DUPLICATE, duplicate.status());
        assertEquals(accepted.originalCheckpoint(), duplicate.originalCheckpoint());
        assertEquals(0, fixture.state.view().lastSequenceNumber());
        final byte[] different = original.requestDigest();
        different[0] ^= 1;
        assertRejected(
            LogicalStreamState.Rejection.IDENTITY_CONFLICT,
            fixture.step(
                new LogicalCommand(retry.action(), retry.requestId(), retry.firstObservedIndex(), different, SECOND, 1, new byte[] { 99 }),
                2,
                2,
                1
            )
        );
        assertRejected(
            LogicalStreamState.Rejection.IDENTITY_CONFLICT,
            fixture.step(
                new LogicalCommand(retry.action(), retry.requestId(), 5, original.requestDigest(), SECOND, 1, new byte[] { 99 }),
                2,
                2,
                1
            )
        );
        assertEquals(0, fixture.state.view().lastSequenceNumber());
    }

    public void testRetryKeepsOriginalOutcomeWhenResolutionChangesBetweenMutationAndNoop() throws Exception {
        final var fixture = active(16);
        final var mutation = command(LogicalCommand.Action.MUTATION, FIRST, 0, 1);
        final var originalMutation = fixture.step(mutation, 1, 1, 1);
        assertAccepted(originalMutation);
        assertAccepted(fixture.step(command(LogicalCommand.Action.OWNERSHIP, SECOND, 1, 2), 2, 2, 1));
        final var nowNoop = new LogicalCommand(
            LogicalCommand.Action.OUTCOME_ONLY,
            mutation.requestId(),
            mutation.firstObservedIndex(),
            mutation.requestDigest(),
            SECOND,
            -1,
            new byte[] { 99 }
        );
        final var mutationRetry = fixture.step(nowNoop, 2, 2, 1);
        assertEquals(LogicalStreamState.Status.DUPLICATE, mutationRetry.status());
        assertEquals(originalMutation.originalCheckpoint(), mutationRetry.originalCheckpoint());
        final var noop = command(LogicalCommand.Action.OUTCOME_ONLY, SECOND, -1, fixture.index - 1);
        final var originalNoop = fixture.step(noop, 2, 2, 1);
        assertAccepted(originalNoop);
        final var nowMutation = new LogicalCommand(
            LogicalCommand.Action.MUTATION,
            noop.requestId(),
            noop.firstObservedIndex(),
            noop.requestDigest(),
            SECOND,
            1,
            new byte[] { 88 }
        );
        final var noopRetry = fixture.step(nowMutation, 2, 2, 1);
        assertEquals(LogicalStreamState.Status.DUPLICATE, noopRetry.status());
        assertEquals(originalNoop.originalCheckpoint(), noopRetry.originalCheckpoint());
        assertEquals(0, fixture.state.view().lastSequenceNumber());
        final var differentControl = new LogicalCommand(
            LogicalCommand.Action.SCHEMA,
            noop.requestId(),
            noop.firstObservedIndex(),
            noop.requestDigest(),
            SECOND,
            1,
            new byte[] { 88 }
        );
        assertRejected(LogicalStreamState.Rejection.IDENTITY_CONFLICT, fixture.step(differentControl, 2, 2, 2));
        assertAccepted(fixture.step(command(LogicalCommand.Action.MUTATION, SECOND, 1, fixture.index - 1), 2, 2, 1));
        assertEquals(1, fixture.state.view().lastSequenceNumber());
    }

    public void testEvictedOutcomeCannotBecomeANewMutation() throws Exception {
        final var fixture = active(2);
        final var original = command(LogicalCommand.Action.MUTATION, FIRST, 0, 1);
        assertAccepted(fixture.step(original, 1, 1, 1));
        assertAccepted(fixture.step(command(LogicalCommand.Action.MUTATION, FIRST, 1, 2), 1, 1, 1));
        assertAccepted(fixture.step(command(LogicalCommand.Action.MUTATION, FIRST, 2, 3), 1, 1, 1));
        assertEquals(2, fixture.state.view().retainedOutcomes());
        assertEquals(2, fixture.state.view().minimumRequestIndex());
        assertRejected(
            LogicalStreamState.Rejection.OBSERVATION_EXPIRED,
            fixture.step(
                new LogicalCommand(
                    original.action(),
                    original.requestId(),
                    original.firstObservedIndex(),
                    original.requestDigest(),
                    FIRST,
                    3,
                    new byte[] { 88 }
                ),
                1,
                1,
                1
            )
        );
        assertEquals(2, fixture.state.view().lastSequenceNumber());
        assertRejected(
            LogicalStreamState.Rejection.OBSERVATION_EXPIRED,
            fixture.step(command(LogicalCommand.Action.MUTATION, FIRST, 3, 1), 1, 1, 1)
        );
        assertAccepted(fixture.step(command(LogicalCommand.Action.MUTATION, FIRST, 3, 6), 1, 1, 1));
        assertEquals(3, fixture.state.view().lastSequenceNumber());
        assertEquals(2, fixture.state.view().retainedOutcomes());
    }

    public void testOutcomeOnlyAndExactSchemaPointerDoNotMintDocumentSequenceNumbers() throws Exception {
        final var fixture = active(8);
        final var view = fixture.state.view();
        assertEquals(1, view.schemaVersion());
        assertEquals(1, view.schemaCheckpoint().index());
        assertEquals("ca358758f6d27e6cf45272937977a748fd88391db679ceda7dc7bf1f005ee879", view.schemaDigest());
        assertEquals(view, fixture.state.view());
        final var outcome = command(LogicalCommand.Action.OUTCOME_ONLY, FIRST, -1, 1);
        final var accepted = fixture.step(outcome, 1, 1, 1);
        assertAccepted(accepted);
        assertEquals(-1, fixture.state.view().lastSequenceNumber());
        assertEquals(LogicalStreamState.Status.DUPLICATE, fixture.step(outcome, 1, 1, 1).status());
        assertRejected(
            LogicalStreamState.Rejection.SCHEMA_MISMATCH,
            fixture.step(command(LogicalCommand.Action.SCHEMA, FIRST, 0, 3), 1, 1, 2)
        );
        assertEquals(1, fixture.state.view().schemaVersion());
        assertAccepted(fixture.step(command(LogicalCommand.Action.SCHEMA, FIRST, 1, 4), 1, 1, 2));
        assertEquals(-1, fixture.state.view().lastSequenceNumber());
        assertEquals(5, fixture.state.view().schemaCheckpoint().index());
    }

    public void testLegacyHistoryCannotSilentlySwitchProtocolsAndLogicalHistoryRejectsRawWrites() throws Exception {
        final var fixture = new Fixture(8);
        assertTrue(fixture.state.observeLegacy(fixture.checkpoint()));
        assertRejected(
            LogicalStreamState.Rejection.LEGACY_HISTORY,
            fixture.step(command(LogicalCommand.Action.OWNERSHIP, FIRST, 0, 0), 1, 1, 0)
        );
        assertFalse(fixture.state.observeLegacy(fixture.checkpoint()));
        assertNull(fixture.state.view().owner());
        final var controlled = active(8);
        assertFalse(controlled.state.observeLegacy(controlled.checkpoint()));
        assertEquals(FIRST, controlled.state.view().owner());
    }

    public void testMalformedFutureAndOutOfOrderInputsCannotChangeLogicalAuthority() throws Exception {
        final var fixture = new Fixture(8);
        final var noOwner = command(LogicalCommand.Action.MUTATION, FIRST, 0, -1);
        assertRejected(LogicalStreamState.Rejection.NO_OWNER, fixture.step(noOwner, 1, 1, 1));
        assertRejected(
            LogicalStreamState.Rejection.FUTURE_OBSERVATION,
            fixture.step(command(LogicalCommand.Action.OWNERSHIP, FIRST, 0, 1), 1, 1, 0)
        );
        assertNull(fixture.state.view().owner());
        final var cp = fixture.checkpoint();
        assertRejected(
            LogicalStreamState.Rejection.MALFORMED,
            fixture.state.apply(cp, new OperationEnvelope(fixture.stream, OperationEnvelope.Kind.LOGICAL, 1, 1, 0, new byte[0]))
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> fixture.state.apply(
                cp,
                new OperationEnvelope(
                    fixture.stream,
                    OperationEnvelope.Kind.LOGICAL,
                    1,
                    1,
                    0,
                    command(LogicalCommand.Action.OWNERSHIP, FIRST, 0, 1).encode(4096)
                )
            )
        );
        assertNull(fixture.state.view().owner());
        assertEquals(0, fixture.state.view().retainedOutcomes());
    }

    public void testLegacyFramingCannotSelectTheLogicalProtocol() throws Exception {
        final var fixture = new Fixture(8);
        final var cp = fixture.checkpoint();
        final var command = command(LogicalCommand.Action.OWNERSHIP, FIRST, 0, -1);
        for (var kind : OperationEnvelope.Kind.values()) {
            if (kind != OperationEnvelope.Kind.LOGICAL) {
                expectThrows(
                    IllegalArgumentException.class,
                    () -> fixture.state.apply(cp, new OperationEnvelope(fixture.stream, kind, 1, 1, 0, command.encode(4096)))
                );
                assertEquals(-1, fixture.state.view().lastIndex());
                assertFalse(fixture.state.view().controlled());
            }
        }
        assertAccepted(
            fixture.state.apply(cp, new OperationEnvelope(fixture.stream, OperationEnvelope.Kind.LOGICAL, 1, 1, 0, command.encode(4096)))
        );
    }

    public void testRebuildingTheOrderedPrefixReproducesOutcomesAndRetentionFence() throws Exception {
        final var fixture = active(4);
        LogicalCommand retry = null;
        for (int i = 0; i < 12; i++) {
            final var next = command(LogicalCommand.Action.MUTATION, FIRST, i, fixture.index - 1);
            assertAccepted(fixture.step(next, 1, 1, 1));
            if (i == 10) {
                retry = next;
            }
        }
        assertAccepted(fixture.step(command(LogicalCommand.Action.OWNERSHIP, SECOND, 1, fixture.index - 1), 2, 2, 1));
        final var duplicate = new LogicalCommand(
            retry.action(),
            retry.requestId(),
            retry.firstObservedIndex(),
            retry.requestDigest(),
            SECOND,
            12,
            new byte[] { 99 }
        );
        assertEquals(LogicalStreamState.Status.DUPLICATE, fixture.step(duplicate, 2, 2, 1).status());
        final var recovered = new LogicalStreamState(fixture.stream, 4, 4096);
        for (var frame : fixture.frames) {
            assertEquals(frame.decision(), recovered.apply(frame.checkpoint(), frame.envelope()));
        }
        assertEquals(fixture.state.view(), recovered.view());
        assertEquals(4, recovered.view().retainedOutcomes());
        assertTrue(recovered.view().minimumRequestIndex() > 0);
    }

    private static Fixture active(int outcomes) throws Exception {
        final var fixture = new Fixture(outcomes);
        assertAccepted(fixture.step(command(LogicalCommand.Action.OWNERSHIP, FIRST, 0, -1), 1, 1, 0));
        assertAccepted(fixture.step(command(LogicalCommand.Action.SCHEMA, FIRST, 0, 0), 1, 1, 1));
        return fixture;
    }

    private static LogicalCommand command(LogicalCommand.Action action, UUID actor, long argument, long observed) {
        final boolean payload = action == LogicalCommand.Action.SCHEMA
            || action == LogicalCommand.Action.MUTATION
            || action == LogicalCommand.Action.OUTCOME_ONLY;
        return new LogicalCommand(
            action,
            UUID.randomUUID(),
            observed,
            new byte[32],
            actor,
            argument,
            payload ? new byte[] { 7 } : new byte[0]
        );
    }

    private static void assertAccepted(LogicalStreamState.Decision result) {
        assertEquals(result.toString(), LogicalStreamState.Status.ACCEPTED, result.status());
        assertEquals(LogicalStreamState.Rejection.NONE, result.rejection());
        assertNotNull(result.originalCheckpoint());
    }

    private static void assertRejected(LogicalStreamState.Rejection expected, LogicalStreamState.Decision result) {
        assertEquals(result.toString(), LogicalStreamState.Status.REJECTED, result.status());
        assertEquals(expected, result.rejection());
        assertNull(result.originalCheckpoint());
    }

    private static final class Fixture {
        private final ShardLogId stream = new ShardLogId(new UUID(1, 2), "index-uuid", 0, new UUID(3, 4));
        private final LogicalStreamState state;
        private long index;
        private final List<Frame> frames = new ArrayList<>();

        private record Frame(LogCheckpoint checkpoint, OperationEnvelope envelope, LogicalStreamState.Decision decision) {
        }

        private Fixture(int outcomes) {
            state = new LogicalStreamState(stream, outcomes, 4096);
        }

        private LogCheckpoint checkpoint() {
            return new LogCheckpoint(stream, index++, 1);
        }

        private LogicalStreamState.Decision step(LogicalCommand command, long epoch, long term, long schema) throws Exception {
            final var cp = checkpoint();
            final var envelope = new OperationEnvelope(stream, OperationEnvelope.Kind.LOGICAL, epoch, term, schema, command.encode(4096));
            final var decision = state.apply(cp, envelope);
            frames.add(new Frame(cp, envelope, decision));
            return decision;
        }
    }
}
