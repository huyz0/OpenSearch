/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
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
import org.opensearch.index.shard.IndexShardTestCase;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class PrivatePreparationMappingRebuildTests extends IndexShardTestCase {
    public void testRebuildPreservesOldIndexedFieldsUnderTheNewMapper() throws Exception {
        final var servingEngine = new AtomicReference<CommittedServingEngine>();
        final var serving = newStartedShard(true, Settings.EMPTY, new EngineBackedIndexerFactory(configuration -> {
            final var engine = new CommittedServingEngine(configuration);
            servingEngine.set(engine);
            return engine;
        }));
        try {
            updateMappings(
                serving,
                IndexMetadata.builder(serving.indexSettings().getIndexMetadata())
                    .putMapping("{\"dynamic\":false,\"properties\":{}}")
                    .mappingVersion(2)
                    .build()
            );
            final var source = servingEngine.get();
            final long term = serving.getOperationPrimaryTerm();
            final var old = source.prepareIndex(
                source.config().getDocumentMapperForTypeSupplier().get(),
                new SourceToParse(serving.shardId().getIndexName(), "old", new BytesArray("{\"added\":\"value\"}"), MediaTypeRegistry.JSON),
                0,
                term,
                1,
                VersionType.EXTERNAL,
                Engine.Operation.Origin.PRIMARY,
                -1,
                false,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                0
            );
            assertNull(old.parsedDoc().dynamicMappingsUpdate());
            assertEquals(Engine.Result.Type.SUCCESS, source.index(old).getResultType());
            source.refresh("old schema committed file state");
            assertEquals(0, matches(source));
            updateMappings(
                serving,
                IndexMetadata.builder(serving.indexSettings().getIndexMetadata())
                    .putMapping("{\"dynamic\":false,\"properties\":{\"added\":{\"type\":\"keyword\"}}}")
                    .mappingVersion(3)
                    .build()
            );
            final UUID group = UUID.randomUUID();
            final var admission = new PreparationEngineAdmission(Set.of(group), 1, 64L * 1024 * 1024, 64L * 1024 * 1024);
            try (
                var rebuilt = PrivatePreparationResources.openCommitted(
                    admission,
                    group,
                    source,
                    createTempDir(),
                    64L * 1024 * 1024,
                    4L * 1024 * 1024,
                    0,
                    term,
                    new PrivatePreparationResources.RebuildLimits(10_000, 1024 * 1024, 1L << 30, 10_000, 4L * 1024 * 1024)
                )
            ) {
                final var privateEngine = rebuilt.engine();
                assertEquals(0, matches(privateEngine));
                // A full source replay under schema 3 would have incorrectly indexed the old field.
                final var next = privateEngine.prepareIndex(
                    privateEngine.config().getDocumentMapperForTypeSupplier().get(),
                    new SourceToParse(
                        serving.shardId().getIndexName(),
                        "new",
                        new BytesArray("{\"added\":\"value\"}"),
                        MediaTypeRegistry.JSON
                    ),
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    term,
                    Versions.MATCH_ANY,
                    VersionType.INTERNAL,
                    Engine.Operation.Origin.PRIMARY,
                    -1,
                    false,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    0
                );
                final var result = rebuilt.index(next);
                assertEquals(Engine.Result.Type.SUCCESS, result.getResultType());
                assertEquals(1, result.getSeqNo());
                privateEngine.refresh("private current-schema insert");
                assertEquals(1, matches(privateEngine));
                try (var reader = privateEngine.acquireSearcher("private old and new files", Engine.SearcherScope.INTERNAL)) {
                    assertEquals(2, reader.getIndexReader().numDocs());
                }
                assertEquals(0, matches(source));
                assertEquals(0, source.getProcessedLocalCheckpoint());
            }
            assertEquals(new PreparationEngineAdmission.Stats(0, 0, 0), admission.stats());
        } finally {
            closeShards(serving);
        }
    }

    private static int matches(Engine engine) throws Exception {
        try (var searcher = engine.acquireSearcher("mapping-sensitive recovery check", Engine.SearcherScope.INTERNAL)) {
            return searcher.count(new TermQuery(new Term("added", "value")));
        }
    }
}
