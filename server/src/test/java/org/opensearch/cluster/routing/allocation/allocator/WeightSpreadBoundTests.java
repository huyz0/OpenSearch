/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.allocator;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.common.settings.Settings;

/**
 * {@code LocalShardsBalancer.balanceByWeights} skips an index outright when its weight spread across
 * all nodes is already under the balance threshold, avoiding a per-node decider scan for indices
 * that provably cannot move. The risk in that optimization is over-skipping: an index that genuinely
 * needs rebalancing being passed over and left unbalanced.
 *
 * <p>This is the targeted regression guard for that. A cluster is grown by adding an empty node,
 * which makes every index unbalanced at once, and the balancer must still even them out -- an index
 * left concentrated on the original nodes would mean the skip fired when it should not have.
 *
 * <p>Broader balance behaviour is already covered by {@code BalanceConfigurationTests},
 * {@code IndexBalanceTests} and {@code ClusterRebalanceRoutingTests}; this adds the specific shape
 * the skip could break.
 */
public class WeightSpreadBoundTests extends OpenSearchAllocationTestCase {

    public void testIndicesNeedingRebalanceAreNotSkipped() {
        AllocationService service = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", Integer.MAX_VALUE)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", Integer.MAX_VALUE)
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", Integer.MAX_VALUE)
                .build()
        );

        ClusterState state = settle(service, buildState(3));
        assertEquals("expected the cluster to start on three nodes", 3, state.nodes().getDataNodes().size());

        // Add a fourth, empty node. Every index is now unbalanced, so none may be skipped.
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(state.nodes()).add(newNode("node-3"));
        state = settle(service, ClusterState.builder(state).nodes(nodes).build());

        RoutingNode added = state.getRoutingNodes().node("node-3");
        assertNotNull(added);
        assertTrue("the added node must receive shards, otherwise balancing was skipped entirely", added.size() > 0);

        // Every index must have placed at least one shard on the new node. An index that was wrongly
        // skipped would remain entirely on the original three.
        for (String index : state.metadata().indices().keySet()) {
            int onAddedNode = 0;
            for (ShardRouting shard : added) {
                if (shard.getIndexName().equals(index)) {
                    onAddedNode++;
                }
            }
            assertTrue(
                "index [" + index + "] was not rebalanced onto the added node -- the weight-spread skip over-skipped",
                onAddedNode > 0
            );
        }
    }

    /** An already-balanced cluster must stay balanced, i.e. the skip must not itself cause churn. */
    public void testBalancedClusterStaysPut() {
        AllocationService service = createAllocationService(Settings.EMPTY);
        ClusterState settled = settle(service, buildState(3));

        ClusterState afterReroute = service.reroute(settled, "no-op");
        assertSame("a settled cluster must not be perturbed by a further reroute", settled, afterReroute);
    }

    private ClusterState settle(AllocationService service, ClusterState state) {
        state = service.reroute(state, "test");
        int guard = 0;
        while (state.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).isEmpty() == false) {
            state = startInitializingShardsAndReroute(service, state);
            if (++guard > 100) {
                fail("cluster did not settle");
            }
        }
        return state;
    }

    private ClusterState buildState(int nodeCount) {
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        // Several indices with enough shards each that a fourth node must receive some of every one.
        for (int i = 0; i < 4; i++) {
            IndexMetadata indexMetadata = IndexMetadata.builder("index-" + i)
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(8)
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
