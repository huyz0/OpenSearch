/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.upgrade.post;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.settings.Settings;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.ShardRoutingState;

import java.util.Set;

/**
 * An index can be present in metadata and absent from the routing table.
 * {@code indicesWithMissingPrimaries} dereferenced the routing lookup without checking, so that
 * combination threw a {@link NullPointerException} out of the upgrade request instead of answering the
 * question it was asked.
 */
public class TransportUpgradeActionTests extends OpenSearchAllocationTestCase {

    public void testIndexWithoutRoutingTableCountsAsMissingPrimaries() {
        ClusterState withRouting = stateWith("upgrade-index");
        ClusterState withoutRouting = ClusterState.builder(withRouting).routingTable(RoutingTable.builder().build()).build();

        assertTrue("the index must still be in metadata", withoutRouting.metadata().hasIndex("upgrade-index"));
        assertNull("...and absent from routing", withoutRouting.routingTable().index("upgrade-index"));

        assertEquals(
            Set.of("upgrade-index"),
            TransportUpgradeAction.indicesWithMissingPrimaries(withoutRouting, new String[] { "upgrade-index" })
        );
    }

    /**
     * The counterpart: an index whose primaries are all active is not reported. Without this the test
     * above would pass even if the method reported every index unconditionally.
     */
    public void testIndexWithActivePrimariesIsNotReported() {
        ClusterState state = stateWithStartedPrimaries("healthy-index");
        assertTrue(state.routingTable().index("healthy-index").allPrimaryShardsActive());

        assertEquals(Set.of(), TransportUpgradeAction.indicesWithMissingPrimaries(state, new String[] { "healthy-index" }));
    }

    private static ClusterState stateWith(String indexName) {
        Metadata metadata = Metadata.builder().put(indexMetadata(indexName), false).build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(RoutingTable.builder().addAsNew(metadata.index(indexName)).build())
            .build();
    }

    private ClusterState stateWithStartedPrimaries(String indexName) {
        Metadata metadata = Metadata.builder().put(indexMetadata(indexName), false).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(newNode("node-0")).add(newNode("node-1")))
            .build();
        // Walk the shards to STARTED so allPrimaryShardsActive() is true.
        AllocationService service = createAllocationService(Settings.EMPTY);
        state = service.reroute(state, "test");
        while (state.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).isEmpty() == false) {
            state = startInitializingShardsAndReroute(service, state);
        }
        return state;
    }

    private static IndexMetadata indexMetadata(String indexName) {
        return IndexMetadata.builder(indexName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();
    }
}
