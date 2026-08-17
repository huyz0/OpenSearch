/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * An index can be present in metadata and absent from the routing table. The shrink validation path
 * dereferenced the routing lookup without checking, so that combination threw a
 * {@link NullPointerException} instead of the error the caller is meant to see.
 *
 * <p>The fix deliberately does not add a new error: no routing entry means no started shards, so no
 * node holds a full copy, which is the condition the existing message already describes.
 */
public class ResizeWithoutRoutingTableTests extends OpenSearchTestCase {

    public void testShrinkValidationRejectsAnIndexWithNoRoutingTable() {
        ClusterState withRouting = stateWith("source-index", 4);
        ClusterState withoutRouting = ClusterState.builder(withRouting).routingTable(RoutingTable.builder().build()).build();

        assertTrue("the index must still be in metadata", withoutRouting.metadata().hasIndex("source-index"));
        assertNull("...and absent from routing", withoutRouting.routingTable().index("source-index"));

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> MetadataCreateIndexService.validateShrinkIndex(withoutRouting, "source-index", "target-index", targetSettings())
        );
        assertTrue(e.getMessage(), e.getMessage().contains("must have all shards allocated on the same node"));
    }

    /**
     * An open source index whose shards are merely unallocated produces the same error, which is what
     * establishes that the guard reuses the existing path rather than inventing a second one. Note the
     * routing table here is present but empty of started shards.
     */
    public void testShrinkValidationGivesTheSameErrorWithUnallocatedShards() {
        ClusterState withRouting = openStateWith("source-index", 4);
        assertNotNull("an open index does get a routing entry", withRouting.routingTable().index("source-index"));

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> MetadataCreateIndexService.validateShrinkIndex(withRouting, "source-index", "target-index", targetSettings())
        );
        assertTrue(e.getMessage(), e.getMessage().contains("must have all shards allocated on the same node"));
    }

    private static ClusterState openStateWith(String indexName, int shards) {
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexMetadata.SETTING_BLOCKS_WRITE, true)
            )
            .numberOfShards(shards)
            .numberOfReplicas(0)
            .build();
        Metadata metadata = Metadata.builder().put(indexMetadata, false).build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(RoutingTable.builder().addAsNew(metadata.index(indexName)).build())
            .blocks(org.opensearch.cluster.block.ClusterBlocks.builder().addIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK).build())
            .build();
    }

    private static Settings targetSettings() {
        return Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 2).build();
    }

    private static ClusterState stateWith(String indexName, int shards) {
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(shards)
            .numberOfReplicas(0)
            .state(IndexMetadata.State.CLOSE)
            .build();
        Metadata metadata = Metadata.builder().put(indexMetadata, false).build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(RoutingTable.builder().addAsNew(metadata.index(indexName)).build())
            .blocks(
                org.opensearch.cluster.block.ClusterBlocks.builder()
                    .addIndexBlock(indexName, MetadataIndexStateService.INDEX_CLOSED_BLOCK)
                    .build()
            )
            .build();
    }
}
