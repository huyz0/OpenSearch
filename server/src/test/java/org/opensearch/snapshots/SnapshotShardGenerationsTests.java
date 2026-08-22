/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.ShardGenerations;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * A7.7. {@code SnapshotsService.buildShardsGenerationFromRepositoryData} used to take the routing
 * table and dereference each index's entry unguarded, which made "every index in metadata has a
 * routing entry" a precondition of taking a snapshot.
 *
 * <p>That precondition was never checked anywhere: {@code createSnapshotV2} builds its index list
 * straight from {@code metadata().indices().keySet()}, so an index present in metadata and absent
 * from routing reached the loop and threw {@link NullPointerException} on an ordinary
 * create-snapshot call.
 *
 * <p>The fix was to stop taking the routing table at all, because the only thing read out of it was
 * {@code shard(i).shardId().id()}, which is {@code i}. These tests pin both halves: the generations
 * are still correct, and they are now derivable from metadata alone.
 */
public class SnapshotShardGenerationsTests extends OpenSearchTestCase {

    private static final String INDEX = "idx";

    /**
     * The case that used to throw. Nothing here provides a routing table, which is the point: if the
     * method still needed one, this could not compile, and before the fix the equivalent call with an
     * empty routing table threw NullPointerException.
     */
    public void testGenerationsAreDerivedFromMetadataAloneForAnIndexWithNoRoutingEntry() {
        Metadata metadata = metadataWith(3);
        IndexId indexId = new IndexId(INDEX, "idx-uuid");

        ShardGenerations generations = SnapshotsService.buildShardsGenerationFromRepositoryData(
            metadata,
            List.of(indexId),
            RepositoryData.EMPTY
        );

        assertEquals("one entry per shard in metadata", 3, generations.totalShards());
        for (int i = 0; i < 3; i++) {
            assertEquals(
                "an index the repository has never seen gets the new-shard generation",
                ShardGenerations.NEW_SHARD_GEN,
                generations.getShardGen(indexId, i)
            );
        }
    }

    /** Shard count comes from metadata, so an index of any size is covered completely. */
    public void testEveryShardOfTheIndexIsCovered() {
        ShardGenerations generations = SnapshotsService.buildShardsGenerationFromRepositoryData(
            metadataWith(17),
            List.of(new IndexId(INDEX, "idx-uuid")),
            RepositoryData.EMPTY
        );

        assertEquals(17, generations.totalShards());
    }

    private static Metadata metadataWith(int shards) {
        IndexMetadata indexMetadata = IndexMetadata.builder(INDEX)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "idx-uuid")
            )
            .numberOfShards(shards)
            .numberOfReplicas(0)
            .build();
        return Metadata.builder().put(indexMetadata, false).build();
    }
}
