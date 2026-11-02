/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class LogicalOwnershipSessionTests extends OpenSearchTestCase {
    private static final UUID PRODUCER = new UUID(5, 6);

    public void testPhysicalCommitAndAppliedBarrierAreBothRequiredAndCancellationIsDetached() throws Exception {
        final var fixture = new Fixture();
        final var session = fixture.session(effect -> fixture.applied.add(effect));
        session.advance(16, 4096);
        final var notification = session.beginClaim(1).toCompletableFuture();
        assertTrue(notification.cancel(true));
        assertFalse(fixture.pending.isCancelled());
        assertEquals(LogicalOwnershipSession.Stage.CLAIMING, session.stage());
        fixture.commit(false);
        session.advance(16, 4096);
        assertEquals(1, fixture.applied.size());
        assertTrue(session.ownership().isEmpty());
        fixture.pending.complete(fixture.proposed.checkpoint());
        session.advance(16, 4096);
        assertEquals(LogicalOwnershipSession.Stage.OWNED, session.stage());
        assertEquals(PRODUCER, session.ownership().orElseThrow().state().owner());
        assertEquals(fixture.proposed.checkpoint(), session.ownership().orElseThrow().checkpoint());
        assertEquals(1, fixture.applied.size());
        session.close();
    }

    public void testClaimRequiresAppliedObservedPrefixAndAnAdvancingTerm() throws Exception {
        final var fixture = new Fixture();
        fixture.skip();
        fixture.skip();
        final var session = fixture.session(effect -> {});
        expectThrows(IOException.class, () -> session.beginClaim(1));
        session.advance(1, 4096);
        expectThrows(IOException.class, () -> session.beginClaim(1));
        assertEquals(0, fixture.submissions);
        session.advance(1, 4096);
        expectThrows(IllegalArgumentException.class, () -> session.beginClaim(0));
        assertEquals(LogicalOwnershipSession.Stage.REPLAYING, session.stage());
        session.beginClaim(1);
        assertEquals(1, fixture.claim.firstObservedIndex());
        assertEquals(1, fixture.submissions);
        session.close();
    }

    public void testAmbiguousPhysicalFailureFencesWithoutApplyingOrReusingTheContext() throws Exception {
        final var fixture = new Fixture();
        final var session = fixture.session(fixture.applied::add);
        session.advance(16, 4096);
        session.beginClaim(1);
        fixture.commit(false);
        fixture.pending.completeExceptionally(new IOException("ambiguous persistence reply"));
        expectThrows(IOException.class, () -> session.advance(16, 4096));
        assertEquals(LogicalOwnershipSession.Stage.FENCED, session.stage());
        assertTrue(session.ownership().isEmpty());
        assertTrue(fixture.applied.isEmpty());
        expectThrows(IOException.class, () -> session.beginClaim(2));
        final var rebuilt = fixture.session(fixture.applied::add);
        rebuilt.advance(16, 4096);
        rebuilt.beginClaim(2);
        fixture.commit(true);
        rebuilt.advance(16, 4096);
        assertEquals(2, rebuilt.ownership().orElseThrow().state().producerEpoch());
        assertEquals(2, rebuilt.ownership().orElseThrow().state().primaryTerm());
        rebuilt.close();
        session.close();
    }

    public void testApplicationFailureAfterCommitNeverPublishesOwnership() throws Exception {
        final var fixture = new Fixture();
        final var session = fixture.session(effect -> { throw new IOException("native application failed"); });
        session.advance(16, 4096);
        session.beginClaim(1);
        fixture.commit(true);
        assertTrue(expectThrows(IOException.class, () -> session.advance(16, 4096)).getMessage().contains("application"));
        assertEquals(LogicalOwnershipSession.Stage.FENCED, session.stage());
        assertTrue(session.ownership().isEmpty());
        expectThrows(IOException.class, () -> session.advance(16, 4096));
        session.close();
    }

    public void testCompetingCommittedClaimMakesPhysicalSuccessInsufficient() throws Exception {
        final var fixture = new Fixture();
        final var session = fixture.session(fixture.applied::add);
        session.advance(16, 4096);
        fixture.beforeAppend = () -> fixture.control(new UUID(7, 8), 0, 1, 2, 0);
        session.beginClaim(1);
        fixture.commit(true);
        expectThrows(IOException.class, () -> session.advance(16, 4096));
        assertEquals(LogicalOwnershipSession.Stage.FENCED, session.stage());
        assertEquals(1, fixture.applied.size());
        assertTrue(session.ownership().isEmpty());
        session.close();
    }

    public void testSupersededClaimAndLostLeaderReadinessFenceInstalledOwnership() throws Exception {
        for (boolean takeover : new boolean[] { false, true }) {
            final var fixture = new Fixture();
            final var session = fixture.session(fixture.applied::add);
            session.advance(16, 4096);
            session.beginClaim(1);
            fixture.commit(true);
            session.advance(16, 4096);
            assertTrue(session.ownership().isPresent());
            if (takeover) {
                fixture.control(new UUID(7, 8), 1, 2, 2, 0);
            } else {
                fixture.leader = false;
            }
            expectThrows(IOException.class, () -> session.advance(16, 4096));
            assertEquals(LogicalOwnershipSession.Stage.FENCED, session.stage());
            assertTrue(session.ownership().isEmpty());
            session.close();
        }
    }

    public void testEarlierDataAndSchemaAreAppliedBeforeOwnershipCanBeExposed() throws Exception {
        final var fixture = new Fixture();
        final var earlier = new UUID(7, 8);
        fixture.control(earlier, 0, 1, 1, 0);
        for (var command : List.of(
            new LogicalCommand(LogicalCommand.Action.SCHEMA, UUID.randomUUID(), 0, new byte[32], earlier, 0, new byte[] { 7 }),
            new LogicalCommand(LogicalCommand.Action.MUTATION, UUID.randomUUID(), 1, new byte[32], earlier, 0, new byte[] { 8 })
        )) {
            fixture.history.add(
                new RatisJournal.CommittedOperation(
                    new OperationEnvelope(fixture.stream, OperationEnvelope.Kind.LOGICAL, 1, 1, 1, command.encode(4096)),
                    new LogCheckpoint(fixture.stream, fixture.history.size(), 1)
                )
            );
        }
        final var holder = new LogicalOwnershipSession[1];
        holder[0] = fixture.session(effect -> {
            assertTrue(holder[0].ownership().isEmpty());
            fixture.applied.add(effect);
        });
        final var session = holder[0];
        session.advance(16, 4096);
        assertEquals(3, fixture.applied.size());
        session.beginClaim(2);
        fixture.commit(true);
        session.advance(16, 4096);
        assertEquals(4, fixture.applied.size());
        assertEquals(0, session.ownership().orElseThrow().state().lastSequenceNumber());
        assertEquals(1, session.ownership().orElseThrow().state().schemaVersion());
        for (int i = 0; i < 4; i++) {
            assertEquals(i, fixture.applied.get(i).checkpoint().index());
        }
        session.close();
    }

    public void testAppliedSchemaTransitionKeepsOwnerAndUpdatesTheInstalledProjection() throws Exception {
        final var fixture = new Fixture();
        final var holder = new LogicalOwnershipSession[1];
        holder[0] = fixture.session(effect -> {
            assertTrue(holder[0].ownership().isEmpty());
            fixture.applied.add(effect);
        });
        final var session = holder[0];
        session.advance(16, 4096);
        session.beginClaim(1);
        fixture.commit(true);
        session.advance(16, 4096);
        final var barrier = session.ownership().orElseThrow().checkpoint();
        final var schema = new LogicalCommand(
            LogicalCommand.Action.SCHEMA,
            UUID.randomUUID(),
            0,
            new byte[32],
            PRODUCER,
            0,
            new byte[] { 7 }
        );
        fixture.history.add(
            new RatisJournal.CommittedOperation(
                new OperationEnvelope(fixture.stream, OperationEnvelope.Kind.LOGICAL, 1, 1, 1, schema.encode(4096)),
                new LogCheckpoint(fixture.stream, 1, 1)
            )
        );
        session.advance(16, 4096);
        assertEquals(LogicalOwnershipSession.Stage.OWNED, session.stage());
        assertEquals(barrier, session.ownership().orElseThrow().checkpoint());
        assertEquals(1, session.ownership().orElseThrow().state().schemaVersion());
        assertEquals(1, session.ownership().orElseThrow().state().producerEpoch());
        assertEquals(2, fixture.applied.size());
        session.close();
    }

    public void testCloseWithAnUnresolvedClaimDoesNotCancelOrApplyLateCommit() throws Exception {
        final var fixture = new Fixture();
        final var session = fixture.session(fixture.applied::add);
        session.advance(16, 4096);
        session.beginClaim(1);
        final int scanned = fixture.scans;
        session.close();
        assertEquals(LogicalOwnershipSession.Stage.CLOSED, session.stage());
        assertFalse(fixture.pending.isCancelled());
        fixture.commit(true);
        expectThrows(IOException.class, () -> session.advance(16, 4096));
        assertEquals(scanned, fixture.scans);
        assertTrue(fixture.applied.isEmpty());
    }

    public void testReadOnlyFollowerAndInvalidBoundsCannotBecomeReady() throws Exception {
        final var fixture = new Fixture();
        final var session = fixture.session(fixture.applied::add);
        expectThrows(IllegalArgumentException.class, () -> session.advance(0, 4096));
        assertEquals(LogicalOwnershipSession.Stage.REPLAYING, session.stage());
        session.advance(16, 4096);
        fixture.leader = false;
        expectThrows(IOException.class, () -> session.beginClaim(1));
        assertEquals(0, fixture.submissions);
        assertTrue(session.ownership().isEmpty());
        session.close();
    }

    private static final class Fixture implements LogicalOwnershipSession.Source {
        private final ShardLogId stream = new ShardLogId(UUID.randomUUID(), "index", 0, UUID.randomUUID());
        private final UUID group = UUID.randomUUID();
        private final LogicalStreamRecord assignment = new LogicalStreamRecord(
            stream,
            group,
            new LogCheckpoint(JournalGroupRecord.controlIdentity(stream.deploymentId(), group), 2, 1),
            8,
            4096
        );
        private final List<RatisJournal.CommittedOperation> history = new ArrayList<>();
        private final List<RatisJournal.CommittedOperation> applied = new ArrayList<>();
        private RatisJournal.CommittedOperation proposed;
        private LogicalCommand claim;
        private CompletableFuture<LogCheckpoint> pending;
        private boolean leader = true;
        private int submissions;
        private int scans;
        private Runnable beforeAppend = () -> {};

        private LogicalOwnershipSession session(LogicalOwnershipSession.EffectApplier applier) {
            return new LogicalOwnershipSession(this, assignment, PRODUCER, applier);
        }

        @Override
        public RatisJournal.ScanBatch scan(long after, int entries, int bytes) {
            scans++;
            final long committed = history.size() - 1;
            final long through = Math.min(committed, after + entries);
            return new RatisJournal.ScanBatch(
                history.stream()
                    .filter(
                        op -> op.checkpoint().index() > after && op.checkpoint().index() <= through && op.operation().logId().equals(stream)
                    )
                    .toList(),
                through,
                committed
            );
        }

        @Override
        public CompletionStage<LogCheckpoint> append(LogicalCommand command, long epoch, long term, long schema) throws IOException {
            beforeAppend.run();
            submissions++;
            claim = command;
            proposed = new RatisJournal.CommittedOperation(
                new OperationEnvelope(stream, OperationEnvelope.Kind.LOGICAL, epoch, term, schema, command.encode(4096)),
                new LogCheckpoint(stream, history.size(), 1)
            );
            pending = new CompletableFuture<>();
            return pending;
        }

        @Override
        public boolean leaderReady() {
            return leader;
        }

        private void commit(boolean acknowledge) {
            history.add(proposed);
            if (acknowledge) {
                pending.complete(proposed.checkpoint());
            }
        }

        private void control(UUID actor, long expected, long epoch, long term, long schema) {
            try {
                final var command = new LogicalCommand(
                    LogicalCommand.Action.OWNERSHIP,
                    UUID.randomUUID(),
                    history.size() - 1,
                    new byte[32],
                    actor,
                    expected,
                    new byte[0]
                );
                history.add(
                    new RatisJournal.CommittedOperation(
                        new OperationEnvelope(stream, OperationEnvelope.Kind.LOGICAL, epoch, term, schema, command.encode(4096)),
                        new LogCheckpoint(stream, history.size(), 1)
                    )
                );
            } catch (IOException impossible) {
                throw new AssertionError(impossible);
            }
        }

        private void skip() {
            final var other = new ShardLogId(stream.deploymentId(), "other", 0, UUID.randomUUID());
            history.add(
                new RatisJournal.CommittedOperation(
                    new OperationEnvelope(other, OperationEnvelope.Kind.DOCUMENTS, 1, 1, 0, new byte[0]),
                    new LogCheckpoint(other, history.size(), 1)
                )
            );
        }
    }
}
