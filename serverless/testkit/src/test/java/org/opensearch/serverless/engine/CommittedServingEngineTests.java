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
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.serverless.store.WalRecord;

/**
 * M2 engine-boundary spike: the primary prepares operations while a separate native replica engine
 * exposes only the manually committed prefix. This proves neither quorum integration nor production
 * routing. Conditional preparation must wait for previous dependent operations to commit.
 */
public class CommittedServingEngineTests extends IndexShardTestCase {

    public void testSpeculativeRefreshCannotExposeUncommittedInsertUpdateOrDelete() throws Exception {
        final IndexShard preparation = newStartedShard(true);
        IndexShard serving = null;
        try {
            updateMappings(
                preparation,
                IndexMetadata.builder(preparation.indexSettings().getIndexMetadata())
                    .putMapping("{\"properties\":{\"n\":{\"type\":\"long\"}}}")
                    .build()
            );
            serving = newShard(preparation.shardId(), false, "committed-serving", preparation.indexSettings().getIndexMetadata(), null);
            recoverReplica(serving, preparation, true);

            final Engine.IndexResult first = indexDoc(preparation, "_doc", "id", "{\"n\":1}");
            preparation.refresh("speculative-insert");
            serving.refresh("committed-insert-not-yet-applied");
            assertFalse(get(serving).isExists());
            assertDocCount(serving, 0);
            apply(serving, new WalRecord("id", "{\"n\":1}", first.getSeqNo(), first.getTerm(), first.getVersion()));
            serving.refresh("committed-insert");
            assertIdentity(serving, first, "{\"n\":1}");
            assertDocCount(serving, 1);

            final Engine.IndexResult second = indexDoc(preparation, "_doc", "id", "{\"n\":2}");
            preparation.refresh("speculative-update");
            serving.refresh("committed-update-not-yet-applied");
            assertIdentity(serving, first, "{\"n\":1}");
            apply(serving, new WalRecord("id", "{\"n\":2}", second.getSeqNo(), second.getTerm(), second.getVersion()));
            serving.refresh("committed-update");
            assertIdentity(serving, second, "{\"n\":2}");
            assertDocCount(serving, 1);

            final Engine.DeleteResult removed = preparation.applyDeleteOperationOnPrimary(
                Versions.MATCH_ANY,
                "id",
                null,
                VersionType.INTERNAL,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                0
            );
            preparation.refresh("speculative-delete");
            serving.refresh("committed-delete-not-yet-applied");
            assertIdentity(serving, second, "{\"n\":2}");
            apply(serving, WalRecord.deletion("id", removed.getSeqNo(), removed.getTerm(), removed.getVersion()));
            serving.refresh("committed-delete");
            assertFalse(get(serving).isExists());
            assertDocCount(serving, 0);
        } finally {
            if (serving != null) {
                closeShards(serving);
            }
            closeShards(preparation);
        }
    }

    private static org.opensearch.index.get.GetResult get(IndexShard serving) {
        return serving.getService().get("id", null, true, Versions.MATCH_ANY, VersionType.INTERNAL, FetchSourceContext.FETCH_SOURCE);
    }

    private static void assertIdentity(IndexShard serving, Engine.IndexResult expected, String source) {
        final var actual = get(serving);
        assertTrue(actual.isExists());
        assertEquals(source, actual.sourceAsString());
        assertEquals(expected.getSeqNo(), actual.getSeqNo());
        assertEquals(expected.getTerm(), actual.getPrimaryTerm());
        assertEquals(expected.getVersion(), actual.getVersion());
    }

    private static void apply(IndexShard serving, WalRecord record) throws Exception {
        // Conservatively disable unsafe append-only optimizations for tail updates/deletes.
        serving.advanceMaxSeqNoOfUpdatesOrDeletes(record.seqNo());
        final Engine.Result result = record.isDeletion()
            ? serving.applyDeleteOperationOnReplica(record.seqNo(), record.primaryTerm(), record.version(), record.id(), null)
            : serving.applyIndexOperationOnReplica(
                record.id(),
                record.seqNo(),
                record.primaryTerm(),
                record.version(),
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                true,
                new SourceToParse(serving.shardId().getIndexName(), record.id(), new BytesArray(record.source()), MediaTypeRegistry.JSON)
            );
        assertEquals(Engine.Result.Type.SUCCESS, result.getResultType());
    }
}
