/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.log.JournalGroupRecord;
import org.opensearch.serverless.log.LogCheckpoint;
import org.opensearch.serverless.log.LogicalCommand;
import org.opensearch.serverless.log.LogicalOwnershipSession;
import org.opensearch.serverless.log.LogicalStreamRecord;
import org.opensearch.serverless.log.LogicalStreamState;
import org.opensearch.serverless.log.OperationEnvelope;
import org.opensearch.serverless.log.ShardLogId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class PreparedLogicalMutationTests extends OpenSearchTestCase {
    private static final UUID ACTOR = new UUID(5, 6);

    public void testHeaderAndNativePayloadUseOneStableIdentityAndDoNotAliasInputs() throws Exception {
        final var fixture = new Fixture();
        fixture.schema();
        final byte[] source = "{}".getBytes(StandardCharsets.UTF_8);
        final var mutation = new ResolvedMutation(
            UUID.randomUUID(),
            new Translog.Index("id", 0, 2, 1, source, "route", -1),
            ResolvedMutation.Outcome.CREATED
        );
        final byte[] digest = new byte[32];
        digest[0] = 7;
        final var packet = PreparedLogicalMutation.create(fixture.assignment, fixture.ownership(), mutation, 4, digest);
        source[0] ^= 1;
        digest[0] ^= 1;
        final var command = packet.command();
        assertEquals(LogicalCommand.Action.MUTATION, command.action());
        assertEquals(mutation.operationId(), command.requestId());
        assertEquals(0, command.argument());
        assertEquals(4, command.firstObservedIndex());
        assertEquals(7, command.requestDigest()[0]);
        assertEquals(ACTOR, command.actor());
        final var recovered = ResolvedMutation.decode(command.payloadReader().readAllBytes(), 4096);
        assertEquals(mutation.operationId(), recovered.operationId());
        assertEquals(mutation.operation(), recovered.operation());
        assertEquals(0, mutation.sequenceNumber());
        assertEquals(2, mutation.primaryTerm());
    }

    public void testMissingSchemaRetiredAndForeignActivationCannotBuildDataProposals() throws Exception {
        final var fixture = new Fixture();
        final var mutation = mutation(0, 2);
        expectThrows(
            IOException.class,
            () -> PreparedLogicalMutation.create(fixture.assignment, fixture.ownership(), mutation, 3, new byte[32])
        );
        fixture.schema();
        final var other = new ShardLogId(
            fixture.stream.deploymentId(),
            fixture.stream.indexUuid(),
            fixture.stream.shardNumber(),
            UUID.randomUUID()
        );
        final var foreign = new LogicalStreamRecord(other, fixture.group, fixture.assignment.groupActivation(), 8, 4096);
        expectThrows(IOException.class, () -> PreparedLogicalMutation.create(foreign, fixture.ownership(), mutation, 4, new byte[32]));
        final var retirement = new LogicalCommand(LogicalCommand.Action.RETIRE, UUID.randomUUID(), 4, new byte[32], ACTOR, -1, new byte[0]);
        fixture.apply(5, retirement, 1, 2, 1);
        expectThrows(
            IOException.class,
            () -> PreparedLogicalMutation.create(fixture.assignment, fixture.ownership(), mutation, 5, new byte[32])
        );
    }

    public void testNativeSequenceTermAndStaleProjectionRefuseBeforeEncoding() throws Exception {
        final var fixture = new Fixture();
        fixture.schema();
        expectThrows(
            IOException.class,
            () -> PreparedLogicalMutation.create(fixture.assignment, fixture.ownership(), mutation(1, 2), 4, new byte[32])
        );
        expectThrows(
            IOException.class,
            () -> PreparedLogicalMutation.create(fixture.assignment, fixture.ownership(), mutation(0, 3), 4, new byte[32])
        );
        final var stale = fixture.ownership();
        final var first = PreparedLogicalMutation.create(fixture.assignment, stale, mutation(0, 2), 4, new byte[32]);
        fixture.apply(5, first.command(), 1, 2, 1);
        expectThrows(IOException.class, () -> PreparedLogicalMutation.create(fixture.assignment, stale, mutation(1, 2), 5, new byte[32]));
        assertEquals(
            1,
            PreparedLogicalMutation.create(fixture.assignment, fixture.ownership(), mutation(1, 2), 5, new byte[32]).command().argument()
        );
    }

    public void testCompleteCommandHeaderIsIncludedInBodyAdmission() throws Exception {
        final var fixture = new Fixture();
        fixture.schema();
        final var mutation = mutation(0, 2);
        final int bodyLength = mutation.encode(4096).length;
        final int headerLength = 4096 - LogicalCommand.maximumBodyBytes(4096);
        final var exact = new LogicalStreamRecord(
            fixture.stream,
            fixture.group,
            fixture.assignment.groupActivation(),
            8,
            headerLength + bodyLength
        );
        final var packet = PreparedLogicalMutation.create(exact, fixture.ownership(), mutation, 4, new byte[32]);
        assertEquals(headerLength + bodyLength, packet.command().encode(exact.maximumPayloadBytes()).length);
        final var shortLimit = new LogicalStreamRecord(
            fixture.stream,
            fixture.group,
            fixture.assignment.groupActivation(),
            8,
            exact.maximumPayloadBytes() - 1
        );
        expectThrows(IOException.class, () -> PreparedLogicalMutation.create(shortLimit, fixture.ownership(), mutation, 4, new byte[32]));
        expectThrows(
            IllegalArgumentException.class,
            () -> PreparedLogicalMutation.create(exact, fixture.ownership(), mutation, -2, new byte[32])
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> PreparedLogicalMutation.create(exact, fixture.ownership(), mutation, 4, new byte[31])
        );
    }

    private static ResolvedMutation mutation(long sequence, long term) {
        return new ResolvedMutation(
            UUID.randomUUID(),
            new Translog.Index("id", sequence, term, 1, "{}".getBytes(StandardCharsets.UTF_8), null, -1),
            ResolvedMutation.Outcome.CREATED
        );
    }

    private static final class Fixture {
        private final UUID group = UUID.randomUUID();
        private final ShardLogId stream = new ShardLogId(UUID.randomUUID(), "index-uuid", 0, UUID.randomUUID());
        private final LogicalStreamRecord assignment = new LogicalStreamRecord(
            stream,
            group,
            new LogCheckpoint(JournalGroupRecord.controlIdentity(stream.deploymentId(), group), 2, 1),
            8,
            4096
        );
        private final LogicalStreamState state = new LogicalStreamState(stream, 8, 4096);
        private final LogCheckpoint barrier = new LogCheckpoint(stream, 3, 1);

        private Fixture() throws IOException {
            apply(
                3,
                new LogicalCommand(LogicalCommand.Action.OWNERSHIP, UUID.randomUUID(), 2, new byte[32], ACTOR, 0, new byte[0]),
                1,
                2,
                0
            );
        }

        private void schema() throws IOException {
            apply(
                4,
                new LogicalCommand(LogicalCommand.Action.SCHEMA, UUID.randomUUID(), 3, new byte[32], ACTOR, 0, new byte[] { 7 }),
                1,
                2,
                1
            );
        }

        private LogicalOwnershipSession.Ownership ownership() {
            return new LogicalOwnershipSession.Ownership(barrier, state.view());
        }

        private void apply(long index, LogicalCommand command, long epoch, long term, long schema) throws IOException {
            final var result = state.apply(
                new LogCheckpoint(stream, index, 1),
                new OperationEnvelope(stream, OperationEnvelope.Kind.LOGICAL, epoch, term, schema, command.encode(4096))
            );
            assertEquals(LogicalStreamState.Status.ACCEPTED, result.status());
        }
    }
}
