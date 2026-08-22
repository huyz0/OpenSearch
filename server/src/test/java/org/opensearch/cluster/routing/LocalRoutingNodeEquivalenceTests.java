/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.common.settings.Settings;

import java.util.HashSet;
import java.util.Set;

/**
 * {@link RoutingNodes#localRoutingNode} exists so a data node can read its own shards without
 * building the cluster-wide {@link RoutingNodes} inverse index. Because it is on the shard-lifecycle
 * path used by {@code IndicesClusterStateService}, it must return exactly what
 * {@code clusterState.getRoutingNodes().node(nodeId)} returns -- these tests pin that equivalence
 * across the states that path actually sees, rather than trusting the two implementations to agree.
 */
public class LocalRoutingNodeEquivalenceTests extends OpenSearchAllocationTestCase {

    public void testMatchesRoutingNodesForUnassignedStartedAndRelocatingStates() {
        AllocationService service = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", Integer.MAX_VALUE)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", Integer.MAX_VALUE)
                .build()
        );

        // Everything unassigned: no node should report any shard.
        ClusterState state = buildState(6, 4);
        assertEquivalentForAllNodes(state);

        // After allocation: shards INITIALIZING on their target nodes.
        state = service.reroute(state, "test");
        assertEquivalentForAllNodes(state);

        // After start: shards STARTED.
        state = startInitializingShardsAndReroute(service, state);
        while (state.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).isEmpty() == false) {
            state = startInitializingShardsAndReroute(service, state);
        }
        assertEquivalentForAllNodes(state);

        // Add a node so the balancer relocates shards onto it -- exercises the relocation-target
        // branch, where the source shard must contribute its target-relocating counterpart to the
        // destination node.
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(state.nodes()).add(newNode("node-extra"));
        state = service.reroute(ClusterState.builder(state).nodes(nodes).build(), "added node");
        assertTrue(
            "expected the added node to trigger relocations, otherwise this case is untested",
            state.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).isEmpty() == false
        );
        assertEquivalentForAllNodes(state);
    }

    /** A data node hosting nothing must yield an empty RoutingNode, never null. */
    public void testDataNodeWithNoShardsYieldsEmptyNotNull() {
        ClusterState state = buildState(0, 2);
        for (DiscoveryNode node : state.nodes().getDataNodes().values()) {
            RoutingNode local = RoutingNodes.localRoutingNode(state, node.getId());
            assertNotNull("a data node with no shards must produce an empty RoutingNode, not null", local);
            assertEquals(0, local.size());
        }
    }

    /** Unknown and null node ids must be null, matching a miss in RoutingNodes' map. */
    public void testUnknownNodeIsNull() {
        ClusterState state = buildState(2, 2);
        assertNull(RoutingNodes.localRoutingNode(state, "no-such-node"));
        assertNull(RoutingNodes.localRoutingNode(state, null));
        assertNull(state.getRoutingNodes().node("no-such-node"));
    }

    private void assertEquivalentForAllNodes(ClusterState state) {
        RoutingNodes routingNodes = state.getRoutingNodes();
        for (DiscoveryNode node : state.nodes().getNodes().values()) {
            String nodeId = node.getId();
            RoutingNode expected = routingNodes.node(nodeId);
            RoutingNode actual = RoutingNodes.localRoutingNode(state, nodeId);

            if (expected == null) {
                assertNull("expected null for " + nodeId + " to match RoutingNodes", actual);
                continue;
            }
            assertNotNull("expected non-null for " + nodeId + " to match RoutingNodes", actual);
            assertEquals("shard count mismatch on " + nodeId, expected.size(), actual.size());
            assertEquals("shard set mismatch on " + nodeId, shardSet(expected), shardSet(actual));

            // getByShardId is used directly by IndicesClusterStateService, so check it too.
            for (ShardRouting shard : expected) {
                assertEquals("getByShardId mismatch on " + nodeId, shard, actual.getByShardId(shard.shardId()));
            }
        }
    }

    private static Set<ShardRouting> shardSet(RoutingNode node) {
        Set<ShardRouting> out = new HashSet<>();
        for (ShardRouting shard : node) {
            out.add(shard);
        }
        return out;
    }

    private ClusterState buildState(int indexCount, int nodeCount) {
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        for (int i = 0; i < indexCount; i++) {
            IndexMetadata indexMetadata = IndexMetadata.builder("index-" + i)
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(2)
                .numberOfReplicas(1)
                .build();
            metadata.put(indexMetadata, false);
            routingTable.addAsNew(indexMetadata);
        }
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < nodeCount; i++) {
            nodes.add(newNode("node-" + i));
        }
        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable.build()).nodes(nodes).build();
    }
}
