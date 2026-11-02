/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.exec.EngineBackedIndexerFactory;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Local native preparation boundaries; no committed ownership/schema or launcher qualification. */
@SuppressFileSystems("*")
public class PrivatePreparationMutationTests extends IndexShardTestCase {
    private final Map<Store, CommittedServingEngine> engines = new IdentityHashMap<>();

    public void testDynamicMappingCannotConsumePrivateSequenceBeforeSchemaIsAvailable() throws Exception {
        final var serving = serving();
        try (var preparation = preparation(serving)) {
            final var first = index(preparation, serving, "{\"new_field\":1}");
            assertNotNull(first.parsedDoc().dynamicMappingsUpdate());
            final var required = preparation.index(first);
            assertEquals(Engine.Result.Type.MAPPING_UPDATE_REQUIRED, required.getResultType());
            assertNotNull(required.getRequiredMappingUpdate());
            assertEquals(SequenceNumbers.UNASSIGNED_SEQ_NO, required.getSeqNo());
            assertEquals(-1, preparation.engine().getSeqNoStats(-1).getMaxSeqNo());
            assertEquals(-1, serving.getProcessedLocalCheckpoint());
            assertPrivateCount(preparation, 0);

            // A quiescent local schema update only. A runtime controller still needs to commit
            // its schema barrier before installing a mapping or admitting this retry.
            updateMappings(
                serving,
                IndexMetadata.builder(serving.indexSettings().getIndexMetadata())
                    .putMapping("{\"properties\":{\"new_field\":{\"type\":\"long\"}}}")
                    .build()
            );
            final var retry = index(preparation, serving, "{\"new_field\":1}");
            assertNull(retry.parsedDoc().dynamicMappingsUpdate());
            final var accepted = preparation.index(retry);
            assertEquals(Engine.Result.Type.SUCCESS, accepted.getResultType());
            assertEquals(0, accepted.getSeqNo());
            final var resolved = new ResolvedMutation(
                UUID.randomUUID(),
                new Translog.Index(retry, accepted),
                ResolvedMutation.Outcome.CREATED
            );
            assertEquals("route", ((Translog.Index) resolved.operation()).routing());
            assertPrivateCount(preparation, 1);
            assertEquals(-1, serving.getProcessedLocalCheckpoint());
            try (var reader = serving.acquireSearcher("private mapping isolation")) {
                assertEquals(0, reader.getIndexReader().numDocs());
            }
        } finally {
            closeShards(serving);
        }
    }

    public void testNativeConditionalFailuresAndMissingDeletesKeepTheirOriginalResults() throws Exception {
        final var serving = serving();
        try (var preparation = preparation(serving)) {
            final var inserted = preparation.index(index(preparation, serving, "{}"));
            assertTrue(inserted.isCreated());
            final var replaced = preparation.index(index(preparation, serving, "{}"));
            assertFalse(replaced.isCreated());
            assertEquals(2, replaced.getVersion());
            assertEquals(1, replaced.getSeqNo());
            final var engine = preparation.engine();
            final long term = serving.getOperationPrimaryTerm();
            final var stale = preparation.delete(
                engine.prepareDelete(
                    "id",
                    "route",
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    term,
                    Versions.MATCH_ANY,
                    VersionType.INTERNAL,
                    Engine.Operation.Origin.PRIMARY,
                    inserted.getSeqNo(),
                    term
                )
            );
            assertEquals(Engine.Result.Type.FAILURE, stale.getResultType());
            assertNotNull(stale.getFailure());
            assertEquals(SequenceNumbers.UNASSIGNED_SEQ_NO, stale.getSeqNo());
            assertEquals(1, engine.getSeqNoStats(-1).getMaxSeqNo());
            final var delete = engine.prepareDelete(
                "id",
                "route",
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                term,
                Versions.MATCH_ANY,
                VersionType.INTERNAL,
                Engine.Operation.Origin.PRIMARY,
                replaced.getSeqNo(),
                term
            );
            final var removed = preparation.delete(delete);
            assertTrue(removed.isFound());
            assertEquals(2, removed.getSeqNo());
            final var resolved = new ResolvedMutation(
                UUID.randomUUID(),
                new Translog.Delete(delete, removed),
                ResolvedMutation.Outcome.DELETED
            );
            assertEquals("route", ((Translog.Delete) resolved.operation()).routing());
            final var missing = preparation.delete(
                engine.prepareDelete(
                    "id",
                    "route",
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    term,
                    Versions.MATCH_ANY,
                    VersionType.INTERNAL,
                    Engine.Operation.Origin.PRIMARY,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    0
                )
            );
            assertFalse(missing.isFound());
            assertEquals(Engine.Result.Type.SUCCESS, missing.getResultType());
            assertEquals(3, missing.getSeqNo());
            assertEquals(4, missing.getVersion());
            assertPrivateCount(preparation, 0);
            assertEquals(-1, serving.getProcessedLocalCheckpoint());
        } finally {
            closeShards(serving);
        }
    }

    public void testAssignedReplicaAndWrongTermOperationsCannotMutatePrivateHistory() throws Exception {
        final var serving = serving();
        try (var preparation = preparation(serving)) {
            final var engine = preparation.engine();
            final var original = index(preparation, serving, "{}");
            for (var origin : new Engine.Operation.Origin[] { Engine.Operation.Origin.PRIMARY, Engine.Operation.Origin.REPLICA }) {
                final var assigned = new Engine.Index(
                    original.uid(),
                    original.parsedDoc(),
                    0,
                    original.primaryTerm(),
                    1,
                    origin == Engine.Operation.Origin.PRIMARY ? VersionType.INTERNAL : null,
                    origin,
                    original.startTime(),
                    -1,
                    false,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    0
                );
                expectThrows(IllegalArgumentException.class, () -> preparation.index(assigned));
            }
            final var wrongTerm = engine.prepareDelete(
                "id",
                "route",
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                serving.getOperationPrimaryTerm() + 1,
                Versions.MATCH_ANY,
                VersionType.INTERNAL,
                Engine.Operation.Origin.PRIMARY,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                0
            );
            expectThrows(IllegalArgumentException.class, () -> preparation.delete(wrongTerm));
            assertEquals(-1, engine.getSeqNoStats(-1).getMaxSeqNo());
            assertEquals(0, preparation.index(original).getSeqNo());
            assertPrivateCount(preparation, 1);
        } finally {
            closeShards(serving);
        }
    }

    public void testExternalVersionsRemainNativeAcrossDeleteAndRecreation() throws Exception {
        final var serving = serving();
        try (var preparation = preparation(serving)) {
            final var created = preparation.index(external(preparation, serving, 5, VersionType.EXTERNAL));
            assertTrue(created.isCreated());
            assertEquals(5, created.getVersion());
            assertEquals(0, created.getSeqNo());
            for (long rejectedVersion : new long[] { 4, 5 }) {
                final var rejected = preparation.index(external(preparation, serving, rejectedVersion, VersionType.EXTERNAL));
                assertEquals(Engine.Result.Type.FAILURE, rejected.getResultType());
                assertEquals(SequenceNumbers.UNASSIGNED_SEQ_NO, rejected.getSeqNo());
                assertEquals(0, preparation.engine().getSeqNoStats(-1).getMaxSeqNo());
            }
            final var equal = preparation.index(external(preparation, serving, 5, VersionType.EXTERNAL_GTE));
            assertFalse(equal.isCreated());
            assertEquals(5, equal.getVersion());
            assertEquals(1, equal.getSeqNo());
            final var removed = preparation.delete(
                preparation.engine()
                    .prepareDelete(
                        "id",
                        "route",
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        serving.getOperationPrimaryTerm(),
                        7,
                        VersionType.EXTERNAL,
                        Engine.Operation.Origin.PRIMARY,
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        0
                    )
            );
            assertTrue(removed.isFound());
            assertEquals(7, removed.getVersion());
            assertEquals(2, removed.getSeqNo());
            final var recreated = preparation.index(external(preparation, serving, 8, VersionType.EXTERNAL));
            assertTrue(recreated.isCreated());
            assertEquals(8, recreated.getVersion());
            assertEquals(3, recreated.getSeqNo());
            assertPrivateCount(preparation, 1);
            assertEquals(-1, serving.getProcessedLocalCheckpoint());
        } finally {
            closeShards(serving);
        }
    }

    private static Engine.Index external(PrivatePreparationResources preparation, IndexShard serving, long version, VersionType type) {
        final var parsed = index(preparation, serving, "{}");
        return new Engine.Index(
            parsed.uid(),
            parsed.parsedDoc(),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            parsed.primaryTerm(),
            version,
            type,
            Engine.Operation.Origin.PRIMARY,
            parsed.startTime(),
            -1,
            false,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0
        );
    }

    private IndexShard serving() throws IOException {
        return newStartedShard(true, Settings.EMPTY, new EngineBackedIndexerFactory(config -> {
            final var engine = new CommittedServingEngine(config);
            engines.put(config.getStore(), engine);
            return engine;
        }));
    }

    private PrivatePreparationResources preparation(IndexShard serving) throws IOException {
        final var group = UUID.randomUUID();
        final long bytes = 64L * 1024 * 1024;
        return PrivatePreparationResources.openEmpty(
            new PreparationEngineAdmission(Set.of(group), 1, bytes, bytes),
            group,
            engines.get(serving.store()),
            createTempDir(),
            bytes,
            4L * 1024 * 1024
        );
    }

    private static Engine.Index index(PrivatePreparationResources preparation, IndexShard serving, String source) {
        final var engine = preparation.engine();
        return engine.prepareIndex(
            engine.config().getDocumentMapperForTypeSupplier().get(),
            new SourceToParse(serving.shardId().getIndexName(), "id", new BytesArray(source), MediaTypeRegistry.JSON, "route"),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            serving.getOperationPrimaryTerm(),
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            -1,
            false,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0
        );
    }

    private static void assertPrivateCount(PrivatePreparationResources preparation, int expected) throws IOException {
        preparation.engine().refresh("private preparation boundary");
        try (var reader = preparation.engine().acquireSearcher("private preparation boundary")) {
            assertEquals(expected, reader.getIndexReader().numDocs());
        }
    }
}
