/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.log.JournalGroupRecord;
import org.opensearch.serverless.log.LogCheckpoint;
import org.opensearch.serverless.log.LogicalCommand;
import org.opensearch.serverless.log.LogicalOwnershipSession;
import org.opensearch.serverless.log.LogicalStreamRecord;
import org.opensearch.serverless.log.LogicalStreamState;
import org.opensearch.serverless.log.OperationEnvelope;
import org.opensearch.serverless.log.ShardLogId;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;

@SuppressFileSystems("*")
public class PreparedLogicalResultOnlyTests extends EngineTestCase {
    private static final UUID ACTOR = new UUID(5, 6);

    public void testOriginalIdentityAndImmutableBodyConsumeNoLogicalDocumentSlot() throws Exception {
        try (var fixture = new Fixture()) {
            fixture.schema();
            final UUID request = UUID.randomUUID();
            final byte[] digest = new byte[32];
            digest[0] = 7;
            final var outcome = outcome();
            final var packet = PreparedLogicalResultOnly.create(
                fixture.assignment,
                fixture.ownership(),
                fixture.resources,
                request,
                outcome,
                4,
                digest
            );
            digest[0] = 9;
            final var command = packet.command();
            assertEquals(LogicalCommand.Action.OUTCOME_ONLY, command.action());
            assertEquals(-1, command.argument());
            assertEquals(request, command.requestId());
            assertEquals(ACTOR, command.actor());
            assertEquals(7, command.requestDigest()[0]);
            assertArrayEquals(outcome.json(), ResolvedResultOnly.decode(command.payloadReader().readAllBytes(), 4096).json());
            fixture.apply(command);
            assertEquals(-1, fixture.state.view().lastSequenceNumber());
            assertEquals(-1, fixture.resources.engine().getProcessedLocalCheckpoint());
            assertEquals(
                LogicalStreamState.Status.DUPLICATE,
                fixture.state.knownOutcome(request, LogicalCommand.Action.MUTATION, 4, command.requestDigest()).orElseThrow().status()
            );
        }
    }

    public void testSpeculativeMutationAssignedNoopAndUnprocessedGapCannotBecomeResultOnly() throws Exception {
        for (int assigned = 0; assigned < 3; assigned++) {
            try (var fixture = new Fixture()) {
                fixture.schema();
                if (assigned == 0) {
                    assertEquals(
                        Engine.Result.Type.SUCCESS,
                        fixture.resources.index(indexForDoc(createParsedDoc("speculative", null))).getResultType()
                    );
                } else {
                    fixture.resources.engine()
                        .noOp(
                            new Engine.NoOp(
                                assigned == 1 ? 0 : 1,
                                primaryTerm.get(),
                                Engine.Operation.Origin.PRIMARY,
                                System.nanoTime(),
                                "assigned private position"
                            )
                        );
                }
                if (assigned == 2) {
                    // Max sequence matches the logical view, but native processing still has a gap.
                    fixture.apply(
                        new LogicalCommand(LogicalCommand.Action.MUTATION, UUID.randomUUID(), 4, new byte[32], ACTOR, 0, new byte[] { 7 })
                    );
                    fixture.apply(
                        new LogicalCommand(LogicalCommand.Action.MUTATION, UUID.randomUUID(), 5, new byte[32], ACTOR, 1, new byte[] { 7 })
                    );
                    assertEquals(1, fixture.resources.engine().getSeqNoStats(-1).getMaxSeqNo());
                    assertEquals(-1, fixture.resources.engine().getProcessedLocalCheckpoint());
                }
                expectThrows(IOException.class, () -> fixture.packet(fixture.assignment, fixture.ownership(), outcome()));
                assertEquals(-1, fixture.serving.getProcessedLocalCheckpoint());
            }
        }
    }

    public void testMissingSchemaForeignGroupShardTermAndClosedContextAreRefused() throws Exception {
        primaryTerm.set(10);
        try (var fixture = new Fixture()) {
            expectThrows(IOException.class, () -> fixture.packet(fixture.assignment, fixture.ownership(), outcome()));
            fixture.schema();
            final UUID otherGroup = UUID.randomUUID();
            final var otherAssignment = new LogicalStreamRecord(
                fixture.stream,
                otherGroup,
                new LogCheckpoint(JournalGroupRecord.controlIdentity(fixture.stream.deploymentId(), otherGroup), 2, 1),
                8,
                4096
            );
            expectThrows(IOException.class, () -> fixture.packet(otherAssignment, fixture.ownership(), outcome()));
            for (var wrongStream : new ShardLogId[] {
                new ShardLogId(fixture.stream.deploymentId(), "foreign-index", fixture.stream.shardNumber(), UUID.randomUUID()),
                new ShardLogId(
                    fixture.stream.deploymentId(),
                    fixture.stream.indexUuid(),
                    fixture.stream.shardNumber() + 1,
                    UUID.randomUUID()
                ) }) {
                final var projection = initialized(wrongStream, primaryTerm.get());
                final var record = new LogicalStreamRecord(wrongStream, fixture.group, fixture.assignment.groupActivation(), 8, 4096);
                expectThrows(
                    IOException.class,
                    () -> fixture.packet(
                        record,
                        new LogicalOwnershipSession.Ownership(new LogCheckpoint(wrongStream, 3, 1), projection.view()),
                        outcome()
                    )
                );
            }
            final var wrongTerm = initialized(fixture.stream, primaryTerm.get() + 1);
            expectThrows(
                IOException.class,
                () -> fixture.packet(
                    fixture.assignment,
                    new LogicalOwnershipSession.Ownership(new LogCheckpoint(fixture.stream, 3, 1), wrongTerm.view()),
                    outcome()
                )
            );
            fixture.resources.close();
            expectThrows(IllegalStateException.class, () -> fixture.packet(fixture.assignment, fixture.ownership(), outcome()));
        }
    }

    public void testWholeCommandHeaderIsAdmittedBeforeResultEncoding() throws Exception {
        try (var fixture = new Fixture()) {
            fixture.schema();
            final var outcome = outcome();
            final int length = outcome.encode(4096).length + 4096 - LogicalCommand.maximumBodyBytes(4096);
            final var exact = new LogicalStreamRecord(fixture.stream, fixture.group, fixture.assignment.groupActivation(), 8, length);
            assertEquals(length, fixture.packet(exact, fixture.ownership(), outcome).command().encode(length).length);
            final var shortRecord = new LogicalStreamRecord(
                fixture.stream,
                fixture.group,
                fixture.assignment.groupActivation(),
                8,
                length - 1
            );
            expectThrows(IOException.class, () -> fixture.packet(shortRecord, fixture.ownership(), outcome));
        }
    }

    private static ResolvedResultOnly outcome() throws IOException {
        return ResolvedResultOnly.rejected(
            RestStatus.CONFLICT,
            "{\"error\":\"conflict\",\"status\":409}".getBytes(StandardCharsets.UTF_8),
            4096
        );
    }

    private static LogicalStreamState initialized(ShardLogId stream, long term) throws IOException {
        final var state = new LogicalStreamState(stream, 8, 4096);
        accept(
            state,
            stream,
            3,
            new LogicalCommand(LogicalCommand.Action.OWNERSHIP, UUID.randomUUID(), 2, new byte[32], ACTOR, 0, new byte[0]),
            term,
            0
        );
        accept(
            state,
            stream,
            4,
            new LogicalCommand(LogicalCommand.Action.SCHEMA, UUID.randomUUID(), 3, new byte[32], ACTOR, 0, new byte[] { 7 }),
            term,
            1
        );
        return state;
    }

    private static void accept(LogicalStreamState state, ShardLogId stream, long index, LogicalCommand command, long term, long schema)
        throws IOException {
        assertEquals(
            LogicalStreamState.Status.ACCEPTED,
            state.apply(
                new LogCheckpoint(stream, index, 1),
                new OperationEnvelope(stream, OperationEnvelope.Kind.LOGICAL, 1, term, schema, command.encode(4096))
            ).status()
        );
    }

    private final class Fixture implements Closeable {
        private final UUID group = UUID.randomUUID();
        private final ShardLogId stream = new ShardLogId(UUID.randomUUID(), shardId.getIndex().getUUID(), shardId.id(), UUID.randomUUID());
        private final LogicalStreamRecord assignment = new LogicalStreamRecord(
            stream,
            group,
            new LogCheckpoint(JournalGroupRecord.controlIdentity(stream.deploymentId(), group), 2, 1),
            8,
            4096
        );
        private final LogicalStreamState state = new LogicalStreamState(stream, 8, 4096);
        private final Store store;
        private final CommittedServingEngine serving;
        private final PrivatePreparationResources resources;

        private Fixture() throws IOException {
            store = createStore(newFSDirectory(createTempDir()));
            final var configuration = config(defaultSettings, store, createTempDir(), newMergePolicy(), null);
            store.createEmpty(configuration.getIndexSettings().getIndexVersionCreated().luceneVersion);
            store.associateIndexWithNewTranslog(
                Translog.createEmptyTranslog(configuration.getTranslogConfig().getTranslogPath(), -1, shardId, primaryTerm.get())
            );
            serving = new CommittedServingEngine(configuration);
            serving.translogManager()
                .recoverFromTranslog(createTranslogHandler(configuration.getIndexSettings(), serving), -1, Long.MAX_VALUE);
            resources = PrivatePreparationResources.openEmpty(
                new PreparationEngineAdmission(Set.of(group), 1, 64L << 20, 64L << 20),
                group,
                serving,
                createTempDir(),
                64L << 20,
                4L << 20
            );
            accept(
                state,
                stream,
                3,
                new LogicalCommand(LogicalCommand.Action.OWNERSHIP, UUID.randomUUID(), 2, new byte[32], ACTOR, 0, new byte[0]),
                primaryTerm.get(),
                0
            );
        }

        private void schema() throws IOException {
            accept(
                state,
                stream,
                4,
                new LogicalCommand(LogicalCommand.Action.SCHEMA, UUID.randomUUID(), 3, new byte[32], ACTOR, 0, new byte[] { 7 }),
                primaryTerm.get(),
                1
            );
        }

        private void apply(LogicalCommand command) throws IOException {
            accept(state, stream, state.view().lastIndex() + 1, command, primaryTerm.get(), 1);
        }

        private LogicalOwnershipSession.Ownership ownership() {
            return new LogicalOwnershipSession.Ownership(new LogCheckpoint(stream, 3, 1), state.view());
        }

        private PreparedLogicalResultOnly packet(
            LogicalStreamRecord record,
            LogicalOwnershipSession.Ownership ownership,
            ResolvedResultOnly outcome
        ) throws IOException {
            return PreparedLogicalResultOnly.create(record, ownership, resources, UUID.randomUUID(), outcome, 4, new byte[32]);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(resources, serving, store);
        }
    }
}
