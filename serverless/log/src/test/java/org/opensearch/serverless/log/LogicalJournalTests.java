/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Local logical-protocol persistence histories, isolated from other suites' process-owned volumes. */
@SuppressFileSystems("*")
@ThreadLeakLingering(linger = 70_000)
@ThreadLeakFilters(filters = RatisJournalTests.GlobalTimerFilter.class)
public class LogicalJournalTests extends JournalTestCase {
    public void testLogicalReplayFiltersCommittedRetriesAndStaleWritersAfterLeaderLoss() throws Exception {
        final var fixture = fixture(1);
        final var journals = new ArrayList<RatisJournal>();
        try {
            startAll(fixture, journals, true);
            final UUID group = fixture.configurations().getFirst().groups().getFirst().id();
            final var stream = new ShardLogId(fixture.deployment(), "logical-index", 0, UUID.randomUUID());
            final var identities = new java.util.IdentityHashMap<RatisJournal, UUID>();
            for (int i = 0; i < journals.size(); i++) {
                identities.put(journals.get(i), fixture.configurations().get(i).localVoterId());
            }
            final var originalLeader = leader(journals, group);
            final UUID first = identities.get(originalLeader);
            final var owner = logicalCommand(LogicalCommand.Action.OWNERSHIP, first, 0, -1);
            final var ownerCp = originalLeader.appendLogical(group, stream, owner, 1, 1, 0).toCompletableFuture().get(10, TimeUnit.SECONDS);
            final var schema = logicalCommand(LogicalCommand.Action.SCHEMA, first, 0, ownerCp.index());
            final var schemaCp = originalLeader.appendLogical(group, stream, schema, 1, 1, 1)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
            final var mutation = logicalCommand(LogicalCommand.Action.MUTATION, first, 0, schemaCp.index());
            final var originalCp = originalLeader.appendLogical(group, stream, mutation, 1, 1, 1)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
            originalLeader.close();
            journals.remove(originalLeader);
            final var replacement = leader(journals, group);
            final UUID second = identities.get(replacement);
            final var takeover = logicalCommand(LogicalCommand.Action.OWNERSHIP, second, 1, originalCp.index());
            replacement.appendLogical(group, stream, takeover, 2, 2, 1).toCompletableFuture().get(10, TimeUnit.SECONDS);
            final var retry = new LogicalCommand(
                LogicalCommand.Action.OUTCOME_ONLY,
                mutation.requestId(),
                mutation.firstObservedIndex(),
                mutation.requestDigest(),
                second,
                -1,
                new byte[] { 99 }
            );
            final var duplicateCp = replacement.appendLogical(group, stream, retry, 2, 2, 1)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
            final var outcome = logicalCommand(LogicalCommand.Action.OUTCOME_ONLY, second, -1, duplicateCp.index());
            final var outcomeCp = replacement.appendLogical(group, stream, outcome, 2, 2, 1)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
            final var changedOutcome = new LogicalCommand(
                LogicalCommand.Action.MUTATION,
                outcome.requestId(),
                outcome.firstObservedIndex(),
                outcome.requestDigest(),
                second,
                1,
                new byte[] { 88 }
            );
            final var outcomeRetryCp = replacement.appendLogical(group, stream, changedOutcome, 2, 2, 1)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
            final var stale = logicalCommand(LogicalCommand.Action.MUTATION, second, 1, outcomeRetryCp.index());
            final var staleCp = replacement.appendLogical(group, stream, stale, 1, 1, 1).toCompletableFuture().get(10, TimeUnit.SECONDS);
            final var replay = new LogicalStreamReplay(replacement, group, stream, 4, 16384);
            final var effects = new ArrayList<RatisJournal.CommittedOperation>();
            final var receipts = new ArrayList<LogicalStreamReplay.Receipt>();
            LogicalStreamReplay.Batch batch;
            do {
                batch = replay.poll(3, 512);
                effects.addAll(batch.effects());
                receipts.addAll(batch.receipts());
            } while (batch.throughIndex() < staleCp.index());
            assertEquals(5, effects.size());
            assertEquals(8, receipts.size());
            assertEquals(LogicalStreamState.Status.DUPLICATE, receipts.get(4).decision().status());
            assertEquals(duplicateCp, receipts.get(4).checkpoint());
            assertEquals(originalCp, receipts.get(4).decision().originalCheckpoint());
            assertEquals(LogicalStreamState.Status.DUPLICATE, receipts.get(6).decision().status());
            assertEquals(outcomeCp, receipts.get(6).decision().originalCheckpoint());
            assertEquals(LogicalStreamState.Rejection.STALE_EPOCH, receipts.get(7).decision().rejection());
            assertEquals(second, batch.state().owner());
            assertEquals(0, batch.state().lastSequenceNumber());
            assertEquals(schemaCp, batch.state().schemaCheckpoint());
            assertTrue(replay.poll(3, 512).effects().isEmpty());
            final var reconstructed = new LogicalStreamReplay(replacement, group, stream, 4, 16384).poll(100, 65536);
            assertEquals(batch.state(), reconstructed.state());
            assertEquals(receipts, reconstructed.receipts());
            final var assignment = new LogicalStreamRecord(stream, group, replacement.groupActivation(group).checkpoint(), 4, 16384);
            final LogCheckpoint recoveredOriginal;
            try (var session = new LogicalOwnershipSession(replacement, assignment, second, ignored -> {})) {
                LogicalOwnershipSession.Progress progress;
                do {
                    progress = session.advance(3, 512);
                } while (progress.throughIndex() < staleCp.index());
                assertEquals(LogicalOwnershipSession.Stage.REPLAYING, session.stage());
                assertTrue(session.ownership().isEmpty());
                final var found = session.knownClientOutcome(mutation.requestId(), mutation.firstObservedIndex(), mutation.requestDigest());
                assertEquals(LogicalStreamState.Status.DUPLICATE, found.outcome().orElseThrow().status());
                recoveredOriginal = found.outcome().orElseThrow().originalCheckpoint();
                assertEquals(originalCp, recoveredOriginal);
                assertEquals(
                    outcomeCp,
                    session.knownClientOutcome(outcome.requestId(), outcome.firstObservedIndex(), outcome.requestDigest())
                        .outcome()
                        .orElseThrow()
                        .originalCheckpoint()
                );
                assertEquals(progress.throughIndex(), found.throughIndex());
            }
            final var original = replacement.readCommitted(assignment, recoveredOriginal);
            assertEquals(originalCp, original.checkpoint());
            final var recoveredMutation = LogicalCommand.decode(original.operation().payloadReader().readAllBytes(), 16384);
            assertEquals(mutation.requestId(), recoveredMutation.requestId());
            assertEquals(LogicalCommand.Action.MUTATION, recoveredMutation.action());
            assertArrayEquals(new byte[] { 7 }, recoveredMutation.payloadReader().readAllBytes());
            final var recoveredOutcome = LogicalCommand.decode(
                replacement.readCommitted(assignment, outcomeCp).operation().payloadReader().readAllBytes(),
                16384
            );
            assertEquals(outcome.requestId(), recoveredOutcome.requestId());
            assertEquals(LogicalCommand.Action.OUTCOME_ONLY, recoveredOutcome.action());
            final var retryFrame = LogicalCommand.decode(
                replacement.readCommitted(assignment, duplicateCp).operation().payloadReader().readAllBytes(),
                16384
            );
            assertEquals(LogicalCommand.Action.OUTCOME_ONLY, retryFrame.action());
            assertArrayEquals(new byte[] { 99 }, retryFrame.payloadReader().readAllBytes());
            expectThrows(
                IOException.class,
                () -> replacement.readCommitted(assignment, new LogCheckpoint(stream, originalCp.index(), originalCp.entryTerm() + 1))
            );
            expectThrows(
                IOException.class,
                () -> replacement.readCommitted(assignment, new LogCheckpoint(stream, staleCp.index() + 100, staleCp.entryTerm()))
            );
            final var otherStream = new ShardLogId(stream.deploymentId(), "other-index", 0, UUID.randomUUID());
            expectThrows(
                IOException.class,
                () -> replacement.readCommitted(assignment, new LogCheckpoint(otherStream, originalCp.index(), originalCp.entryTerm()))
            );
        } finally {
            closeAll(journals);
        }
    }

    public void testOwnershipSessionInstallsOnlyAnAppliedAcceptedClaimAndReclaimsAfterLeaderLoss() throws Exception {
        final var fixture = fixture(1);
        final var journals = new ArrayList<RatisJournal>();
        try {
            startAll(fixture, journals, true);
            final var identities = new java.util.IdentityHashMap<RatisJournal, UUID>();
            for (int i = 0; i < journals.size(); i++) {
                identities.put(journals.get(i), fixture.configurations().get(i).localVoterId());
            }
            final UUID group = fixture.configurations().getFirst().groups().getFirst().id();
            final var stream = new ShardLogId(fixture.deployment(), "owned-session", 0, UUID.randomUUID());
            final var first = leader(journals, group);
            final var assignment = new LogicalStreamRecord(stream, group, first.groupActivation(group).checkpoint(), 8, 16384);
            final var applied = new ArrayList<RatisJournal.CommittedOperation>();
            try (var session = new LogicalOwnershipSession(first, assignment, identities.get(first), applied::add)) {
                LogicalOwnershipSession.Progress batch;
                do {
                    batch = session.advance(3, 512);
                } while (batch.throughIndex() < batch.committedIndex());
                final var cp = session.beginClaim(1).toCompletableFuture().get(10, TimeUnit.SECONDS);
                assertTrue(session.ownership().isEmpty());
                while (session.stage() != LogicalOwnershipSession.Stage.OWNED) {
                    session.advance(3, 512);
                }
                assertEquals(cp, session.ownership().orElseThrow().checkpoint());
                assertEquals(1, applied.size());
                assertEquals(cp, applied.getFirst().checkpoint());
            }
            first.close();
            journals.remove(first);
            final var replacement = leader(journals, group);
            applied.clear();
            try (var session = new LogicalOwnershipSession(replacement, assignment, identities.get(replacement), applied::add)) {
                LogicalOwnershipSession.Progress batch;
                do {
                    batch = session.advance(3, 512);
                } while (batch.throughIndex() < batch.committedIndex());
                expectThrows(IllegalArgumentException.class, () -> session.beginClaim(1));
                final var cp = session.beginClaim(2).toCompletableFuture().get(10, TimeUnit.SECONDS);
                while (session.stage() != LogicalOwnershipSession.Stage.OWNED) {
                    session.advance(3, 512);
                }
                assertEquals(cp, session.ownership().orElseThrow().checkpoint());
                assertEquals(identities.get(replacement), session.ownership().orElseThrow().state().owner());
                assertEquals(2, session.ownership().orElseThrow().state().producerEpoch());
                assertEquals(2, session.ownership().orElseThrow().state().primaryTerm());
                assertEquals(2, applied.size());
            }
        } finally {
            closeAll(journals);
        }
    }

    public void testLogicalProposalAdmissionCannotImpersonateAnotherVoter() throws Exception {
        final var fixture = fixture(1);
        final var journals = new ArrayList<RatisJournal>();
        try {
            startAll(fixture, journals, true);
            final UUID group = fixture.configurations().getFirst().groups().getFirst().id();
            final var stream = new ShardLogId(fixture.deployment(), "logical-index", 0, UUID.randomUUID());
            final var local = leader(journals, group);
            final UUID actor = fixture.configurations().get(journals.indexOf(local)).localVoterId();
            final var claim = logicalCommand(LogicalCommand.Action.OWNERSHIP, actor, 0, -1);
            final var forged = logicalCommand(LogicalCommand.Action.OWNERSHIP, UUID.randomUUID(), 0, -1);
            final var before = local.scan(group, -1, Set.of(stream), 100, 65536);
            assertTrue(
                expectThrows(IOException.class, () -> local.append(group, logicalEnvelope(stream, claim, 1, 1, 0))).getMessage()
                    .contains("explicit")
            );
            assertTrue(
                expectThrows(IOException.class, () -> local.appendLogical(group, stream, forged, 1, 1, 0)).getMessage()
                    .contains("local voter")
            );
            final var nativeMachine = ((org.apache.ratis.server.RaftServer.Division) nativeDivisionForStall(local, group))
                .getStateMachine();
            final var forgedRequest = org.apache.ratis.protocol.RaftClientRequest.newBuilder()
                .setClientId(org.apache.ratis.protocol.ClientId.randomId())
                .setServerId(org.apache.ratis.protocol.RaftPeerId.valueOf(actor.toString()))
                .setGroupId(org.apache.ratis.protocol.RaftGroupId.valueOf(group))
                .setCallId(1)
                .setMessage(
                    org.apache.ratis.protocol.Message.valueOf(
                        org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(
                            logicalEnvelope(stream, forged, 1, 1, 0).encode(16384)
                        )
                    )
                )
                .setType(org.apache.ratis.protocol.RaftClientRequest.writeRequestType())
                .build();
            assertTrue(
                expectThrows(IOException.class, () -> nativeMachine.startTransaction(forgedRequest)).getMessage().contains("local voter")
            );
            final var oversized = new LogicalCommand(
                LogicalCommand.Action.SCHEMA,
                UUID.randomUUID(),
                -1,
                new byte[32],
                actor,
                0,
                new byte[16384]
            );
            expectThrows(IOException.class, () -> local.appendLogical(group, stream, oversized, 1, 1, 1));
            final var follower = journals.stream().filter(j -> j != local).findFirst().orElseThrow();
            final UUID followerId = fixture.configurations().get(journals.indexOf(follower)).localVoterId();
            final var followerClaim = logicalCommand(LogicalCommand.Action.OWNERSHIP, followerId, 0, -1);
            expectThrows(IOException.class, () -> follower.appendLogical(group, stream, followerClaim, 1, 1, 0));
            expectThrows(
                IOException.class,
                () -> local.appendLogical(group, JournalGroupRecord.controlIdentity(fixture.deployment(), group), claim, 1, 1, 0)
            );
            final var foreign = new ShardLogId(UUID.randomUUID(), stream.indexUuid(), stream.shardNumber(), stream.activationId());
            expectThrows(IOException.class, () -> local.appendLogical(group, foreign, claim, 1, 1, 0));
            assertEquals(before.operations(), local.scan(group, -1, Set.of(stream), 100, 65536).operations());
            final var cp = local.appendLogical(group, stream, claim, 1, 1, 0).toCompletableFuture().get(10, TimeUnit.SECONDS);
            final var replayed = new LogicalStreamReplay(local, group, stream, 8, 16384).poll(100, 65536);
            assertEquals(1, replayed.effects().size());
            assertEquals(cp, replayed.effects().getFirst().checkpoint());
            assertEquals(actor, replayed.state().owner());
        } finally {
            closeAll(journals);
        }
    }

    private static LogicalCommand logicalCommand(LogicalCommand.Action action, UUID actor, long argument, long observed) {
        return new LogicalCommand(
            action,
            UUID.randomUUID(),
            observed,
            new byte[32],
            actor,
            argument,
            action == LogicalCommand.Action.OWNERSHIP ? new byte[0] : new byte[] { 7 }
        );
    }

    private static OperationEnvelope logicalEnvelope(ShardLogId stream, LogicalCommand command, long epoch, long term, long schema)
        throws IOException {
        return new OperationEnvelope(stream, OperationEnvelope.Kind.LOGICAL, epoch, term, schema, command.encode(16384));
    }

}
