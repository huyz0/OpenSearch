/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.exec.EngineBackedIndexerFactory;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.log.LogCheckpoint;
import org.opensearch.serverless.log.LogicalCommand;
import org.opensearch.serverless.log.OperationEnvelope;
import org.opensearch.serverless.log.RatisJournal;
import org.opensearch.serverless.log.ShardLogId;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.UUID;

public class CommittedLogicalApplierTests extends IndexShardTestCase {
    private static final UUID OWNER = new UUID(5, 6);
    private final Map<Store, CommittedServingEngine> engines = new IdentityHashMap<>();

    public void testMismatchedResolvedIdentitySequenceAndTermFailBeforeNativeMutation() throws Exception {
        for (int invalid = 0; invalid < 3; invalid++) {
            final var serving = newServing();
            try {
                final var fixture = new Fixture(serving);
                fixture.activate();
                final var id = UUID.randomUUID();
                final var mutation = new ResolvedMutation(
                    id,
                    new Translog.Index(
                        "id",
                        invalid == 1 ? 1 : 0,
                        invalid == 2 ? fixture.term + 1 : fixture.term,
                        1,
                        "{}".getBytes(StandardCharsets.UTF_8),
                        null,
                        -1
                    ),
                    ResolvedMutation.Outcome.CREATED
                );
                final var command = new LogicalCommand(
                    LogicalCommand.Action.MUTATION,
                    invalid == 0 ? UUID.randomUUID() : id,
                    1,
                    new byte[32],
                    OWNER,
                    0,
                    mutation.encode(4000)
                );
                expectThrows(IOException.class, () -> fixture.applier.apply(fixture.frame(command, 1)));
                assertEquals(SequenceNumbers.NO_OPS_PERFORMED, fixture.engine.getSeqNoStats(-1).getMaxSeqNo());
                serving.refresh("invalid logical identity");
                assertDocCount(serving, 0);
                assertEquals(-1, fixture.applier.lastSequenceNumber());
                expectThrows(IOException.class, () -> fixture.applier.apply(fixture.frame(command, 1)));
            } finally {
                closeShards(serving);
            }
        }
    }

    public void testOutcomeOnlyConsumesNoSequenceWhileResolvedNoopDoes() throws Exception {
        final var serving = newServing();
        try {
            final var fixture = new Fixture(serving);
            fixture.activate();
            final var result = new LogicalCommand(
                LogicalCommand.Action.OUTCOME_ONLY,
                UUID.randomUUID(),
                1,
                new byte[32],
                OWNER,
                -1,
                new byte[] { 7 }
            );
            assertTrue(fixture.applier.apply(fixture.frame(result, 1)).isEmpty());
            assertEquals(-1, fixture.applier.lastSequenceNumber());
            final var noop = new ResolvedMutation(
                UUID.randomUUID(),
                new Translog.NoOp(0, fixture.term, "resolved gap"),
                ResolvedMutation.Outcome.NO_OP
            );
            final var command = new LogicalCommand(
                LogicalCommand.Action.MUTATION,
                noop.operationId(),
                2,
                new byte[32],
                OWNER,
                0,
                noop.encode(4000)
            );
            assertEquals(
                ResolvedMutation.Outcome.NO_OP,
                fixture.applier.apply(fixture.frame(command, 1)).orElseThrow().mutation().outcome()
            );
            assertEquals(0, fixture.applier.lastSequenceNumber());
            assertEquals(0, serving.getProcessedLocalCheckpoint());
            serving.refresh("logical noop application");
            assertDocCount(serving, 0);
        } finally {
            closeShards(serving);
        }
    }

    public void testUninstalledDefinitionAndRetirementCannotAdmitDocumentEffects() throws Exception {
        for (boolean retirement : new boolean[] { false, true }) {
            final var serving = newServing();
            try {
                final var fixture = new Fixture(serving);
                if (retirement) {
                    fixture.activate();
                    fixture.applier.apply(
                        fixture.frame(
                            new LogicalCommand(LogicalCommand.Action.RETIRE, UUID.randomUUID(), 1, new byte[32], OWNER, -1, new byte[0]),
                            1
                        )
                    );
                    final var noop = new ResolvedMutation(
                        UUID.randomUUID(),
                        new Translog.NoOp(0, fixture.term, "too late"),
                        ResolvedMutation.Outcome.NO_OP
                    );
                    final var command = new LogicalCommand(
                        LogicalCommand.Action.MUTATION,
                        noop.operationId(),
                        2,
                        new byte[32],
                        OWNER,
                        0,
                        noop.encode(4000)
                    );
                    expectThrows(IOException.class, () -> fixture.applier.apply(fixture.frame(command, 1)));
                } else {
                    fixture.owner();
                    final var schema = new LogicalCommand(
                        LogicalCommand.Action.SCHEMA,
                        UUID.randomUUID(),
                        0,
                        new byte[32],
                        OWNER,
                        0,
                        new byte[] { 99 }
                    );
                    expectThrows(IOException.class, () -> fixture.applier.apply(fixture.frame(schema, 1)));
                }
                assertEquals(-1, fixture.applier.lastSequenceNumber());
                assertEquals(SequenceNumbers.NO_OPS_PERFORMED, fixture.engine.getSeqNoStats(-1).getMaxSeqNo());
            } finally {
                closeShards(serving);
            }
        }
    }

    public void testUnversionedNativeMapperReplacementStopsBeforeAnotherSequence() throws Exception {
        final var serving = newServing();
        try {
            final var fixture = new Fixture(serving);
            fixture.activate();
            final long mappingVersion = serving.indexSettings().getIndexMetadata().getMappingVersion();
            updateMappings(
                serving,
                IndexMetadata.builder(serving.indexSettings().getIndexMetadata())
                    .putMapping("{\"properties\":{\"added\":{\"type\":\"long\"}}}")
                    .mappingVersion(mappingVersion)
                    .build()
            );
            assertEquals(mappingVersion, serving.indexSettings().getIndexMetadata().getMappingVersion());
            final var noop = new ResolvedMutation(
                UUID.randomUUID(),
                new Translog.NoOp(0, fixture.term, "after mapper replacement"),
                ResolvedMutation.Outcome.NO_OP
            );
            final var command = new LogicalCommand(
                LogicalCommand.Action.MUTATION,
                noop.operationId(),
                1,
                new byte[32],
                OWNER,
                0,
                noop.encode(4000)
            );
            expectThrows(IOException.class, () -> fixture.applier.apply(fixture.frame(command, 1)));
            assertEquals(SequenceNumbers.NO_OPS_PERFORMED, fixture.engine.getSeqNoStats(-1).getMaxSeqNo());
        } finally {
            closeShards(serving);
        }
    }

    public void testForeignShardStoreAndNonemptyUnverifiedBaseAreRefused() throws Exception {
        final var serving = newServing();
        final var other = newServing();
        try {
            final var fixture = new Fixture(serving);
            expectThrows(
                IllegalStateException.class,
                () -> new CommittedLogicalApplier(serving, engines.get(other.store()), fixture.stream, 1, fixture.definition, 4096)
            );
            final var foreign = new ShardLogId(
                fixture.stream.deploymentId(),
                "another-index",
                fixture.stream.shardNumber(),
                UUID.randomUUID()
            );
            expectThrows(
                IllegalArgumentException.class,
                () -> new CommittedLogicalApplier(serving, fixture.engine, foreign, 1, fixture.definition, 4096)
            );
            serving.applyTranslogOperation(
                new Translog.NoOp(0, fixture.term, "unverified preexisting state"),
                Engine.Operation.Origin.PRIMARY
            );
            expectThrows(
                IllegalArgumentException.class,
                () -> new CommittedLogicalApplier(serving, fixture.engine, fixture.stream, 1, fixture.definition, 4096)
            );
        } finally {
            closeShards(serving, other);
        }
    }

    private IndexShard newServing() throws IOException {
        return newStartedShard(true, Settings.EMPTY, new EngineBackedIndexerFactory(config -> {
            final var engine = new CommittedServingEngine(config);
            engines.put(config.getStore(), engine);
            return engine;
        }));
    }

    private final class Fixture {
        private final CommittedServingEngine engine;
        private final ShardLogId stream;
        private final byte[] definition;
        private final long term;
        private final CommittedLogicalApplier applier;
        private int index;

        private Fixture(IndexShard serving) throws IOException {
            engine = engines.get(serving.store());
            stream = new ShardLogId(UUID.randomUUID(), serving.shardId().getIndex().getUUID(), serving.shardId().id(), UUID.randomUUID());
            definition = BytesReference.toBytes(serving.mapperService().documentMapper().mappingSource().uncompressed());
            term = serving.getOperationPrimaryTerm();
            applier = new CommittedLogicalApplier(serving, engine, stream, 1, definition, 4096);
        }

        private void owner() throws IOException {
            applier.apply(
                frame(new LogicalCommand(LogicalCommand.Action.OWNERSHIP, UUID.randomUUID(), -1, new byte[32], OWNER, 0, new byte[0]), 0)
            );
        }

        private void activate() throws IOException {
            owner();
            applier.apply(
                frame(new LogicalCommand(LogicalCommand.Action.SCHEMA, UUID.randomUUID(), 0, new byte[32], OWNER, 0, definition), 1)
            );
        }

        private RatisJournal.CommittedOperation frame(LogicalCommand command, long schema) throws IOException {
            return new RatisJournal.CommittedOperation(
                new OperationEnvelope(stream, OperationEnvelope.Kind.LOGICAL, 1, term, schema, command.encode(4096)),
                new LogCheckpoint(stream, index++, 1)
            );
        }
    }
}
