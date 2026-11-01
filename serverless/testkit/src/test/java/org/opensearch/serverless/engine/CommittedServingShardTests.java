/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.exec.EngineBackedIndexerFactory;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.shard.IndexingOperationListener;
import org.opensearch.index.translog.Translog;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Exercises resolved primary replay through shard parsing, listeners, GET and search. */
public class CommittedServingShardTests extends IndexShardTestCase {
    public void testResolvedPrimaryReplayRetainsIdentityRoutingAndPublicListeners() throws Exception {
        final List<Engine.Operation> applied = new ArrayList<>();
        final IndexingOperationListener listener = new IndexingOperationListener() {
            @Override
            public void postIndex(ShardId shardId, Engine.Index operation, Engine.IndexResult result) {
                if (result.getResultType() == Engine.Result.Type.SUCCESS) {
                    applied.add(operation);
                }
            }

            @Override
            public void postDelete(ShardId shardId, Engine.Delete operation, Engine.DeleteResult result) {
                if (result.getResultType() == Engine.Result.Type.SUCCESS) {
                    applied.add(operation);
                }
            }
        };
        final IndexShard serving = newStartedShard(
            primary -> newShard(
                TestShardRouting.newShardRouting(
                    new ShardId("index", "_na_", 0),
                    randomAlphaOfLength(10),
                    primary,
                    ShardRoutingState.INITIALIZING,
                    RecoverySource.EmptyStoreRecoverySource.INSTANCE
                ),
                Settings.EMPTY,
                new EngineBackedIndexerFactory(CommittedServingEngine::new),
                listener
            ),
            true
        );
        try {
            updateMappings(
                serving,
                IndexMetadata.builder(serving.indexSettings().getIndexMetadata())
                    .putMapping("{\"properties\":{\"n\":{\"type\":\"long\"}}}")
                    .build()
            );
            final long term = serving.getOperationPrimaryTerm();
            final Translog.Index insert = resolvedIndex(0, term, 1, "{\"n\":1}");
            assertIdentity(serving.applyTranslogOperation(insert, Engine.Operation.Origin.PRIMARY), 0, term, 1);
            assertVisible(serving, 0, term, 1, "{\"n\":1}");
            assertEquals(1, applied.size());
            assertEquals("route", ((Engine.Index) applied.get(0)).routing());
            assertEquals(Engine.Operation.Origin.PRIMARY, applied.get(0).origin());

            // Redelivery uses the resolved identity rather than re-running external version checks.
            assertIdentity(serving.applyTranslogOperation(insert, Engine.Operation.Origin.PRIMARY), 0, term, 1);
            assertVisible(serving, 0, term, 1, "{\"n\":1}");
            assertIdentity(
                serving.applyTranslogOperation(resolvedIndex(1, term, 2, "{\"n\":2}"), Engine.Operation.Origin.PRIMARY),
                1,
                term,
                2
            );
            assertVisible(serving, 1, term, 2, "{\"n\":2}");
            assertIdentity(
                serving.applyTranslogOperation(new Translog.Delete("id", 2, term, 3, "route"), Engine.Operation.Origin.PRIMARY),
                2,
                term,
                3
            );
            assertEquals("route", ((Engine.Delete) applied.get(applied.size() - 1)).routing());
            assertEquals(Engine.Operation.Origin.PRIMARY, applied.get(applied.size() - 1).origin());
            serving.refresh("committed delete");
            assertDocCount(serving, 0);
            assertFalse(
                serving.getService()
                    .get("id", null, true, Versions.MATCH_ANY, VersionType.INTERNAL, FetchSourceContext.FETCH_SOURCE)
                    .isExists()
            );
            assertEquals(2, serving.getProcessedLocalCheckpoint());
            serving.sync();
            assertEquals(2, serving.seqNoStats().getLocalCheckpoint());
        } finally {
            closeShards(serving);
        }
    }

    public void testOrdinaryShardPrimaryWritesCannotAllocateServingIdentities() throws Exception {
        final IndexShard serving = newStartedShard(true, Settings.EMPTY, new EngineBackedIndexerFactory(CommittedServingEngine::new));
        try {
            expectThrows(IllegalStateException.class, () -> indexDoc(serving, "_doc", "id", "{}"));
            expectThrows(
                IllegalStateException.class,
                () -> serving.applyDeleteOperationOnPrimary(
                    Versions.MATCH_ANY,
                    "id",
                    null,
                    VersionType.INTERNAL,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    0
                )
            );
            serving.refresh("rejected speculative writes");
            assertDocCount(serving, 0);
            assertEquals(SequenceNumbers.NO_OPS_PERFORMED, serving.seqNoStats().getMaxSeqNo());
        } finally {
            closeShards(serving);
        }
    }

    public void testLocalRestartRecoversAppliedIdentityWithoutAllocatingMissingSlots() throws Exception {
        IndexShard serving = newStartedShard(true, Settings.EMPTY, new EngineBackedIndexerFactory(CommittedServingEngine::new));
        try {
            final long term = serving.getOperationPrimaryTerm();
            assertIdentity(serving.applyTranslogOperation(resolvedIndex(0, term, 1, "{}"), Engine.Operation.Origin.PRIMARY), 0, term, 1);
            assertIdentity(serving.applyTranslogOperation(resolvedIndex(2, term, 2, "{}"), Engine.Operation.Origin.PRIMARY), 2, term, 2);
            assertEquals(0, serving.getProcessedLocalCheckpoint());
            serving = reinitShard(serving);
            recoverShardFromStore(serving);
            assertVisible(serving, 2, term, 2, "{}");
            assertEquals(0, serving.getProcessedLocalCheckpoint());
            assertEquals(2, serving.seqNoStats().getMaxSeqNo());
            final Engine.Result gap = serving.applyTranslogOperation(
                new Translog.NoOp(1, term, "committed missing slot"),
                Engine.Operation.Origin.PRIMARY
            );
            assertEquals(Engine.Result.Type.SUCCESS, gap.getResultType());
            assertEquals(2, serving.getProcessedLocalCheckpoint());
            serving.sync();
            assertEquals(2, serving.seqNoStats().getLocalCheckpoint());
        } finally {
            closeShards(serving);
        }
    }

    private static Translog.Index resolvedIndex(long seqNo, long term, long version, String source) {
        return new Translog.Index(
            "id",
            seqNo,
            term,
            version,
            source.getBytes(StandardCharsets.UTF_8),
            "route",
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP
        );
    }

    private static void assertIdentity(Engine.Result actual, long seqNo, long term, long version) {
        assertEquals(Engine.Result.Type.SUCCESS, actual.getResultType());
        assertEquals(seqNo, actual.getSeqNo());
        assertEquals(term, actual.getTerm());
        assertEquals(version, actual.getVersion());
    }

    private void assertVisible(IndexShard serving, long seqNo, long term, long version, String source) throws Exception {
        serving.refresh("committed primary replay");
        assertDocCount(serving, 1);
        final var actual = serving.getService()
            .get("id", null, true, Versions.MATCH_ANY, VersionType.INTERNAL, FetchSourceContext.FETCH_SOURCE);
        assertTrue(actual.isExists());
        assertEquals(source, actual.sourceAsString());
        assertEquals(seqNo, actual.getSeqNo());
        assertEquals(term, actual.getPrimaryTerm());
        assertEquals(version, actual.getVersion());
    }
}
