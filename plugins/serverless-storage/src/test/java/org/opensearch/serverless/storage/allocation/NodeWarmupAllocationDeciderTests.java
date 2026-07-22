/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.allocation;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.nodecapacity.NodeWarmupCoordinator;

import java.util.Map;

public class NodeWarmupAllocationDeciderTests extends OpenSearchAllocationTestCase {

    private static final String SERVERLESS_INDEX = "serverless-idx";

    private ClusterState buildClusterState(String warmingNames) {
        Settings serverlessIndexSettings = settings(Version.CURRENT).put(
            ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(),
            true
        ).build();
        IndexMetadata serverlessIndex = IndexMetadata.builder(SERVERLESS_INDEX)
            .settings(serverlessIndexSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        // newNode(id, attrs) leaves the node's *name* empty; the decider keys warming state by name
        // (matching DrainCoordinator's convention, which mirrors core's own exclude._name semantics),
        // so tests must set both explicitly via the (name, id, attrs) overload.
        DiscoveryNode warmingNode = newNode(
            "warming-node",
            "warming-node",
            Map.of(ReaderShardPlacementAllocationDecider.READER_NODE_ATTRIBUTE, "true")
        );
        DiscoveryNode warmNode = newNode(
            "warm-node",
            "warm-node",
            Map.of(ReaderShardPlacementAllocationDecider.READER_NODE_ATTRIBUTE, "true")
        );

        Settings.Builder transientSettings = Settings.builder();
        if (warmingNames != null) {
            transientSettings.put(NodeWarmupCoordinator.WARMING_NAMES_SETTING_KEY, warmingNames);
        }

        Metadata metadata = Metadata.builder().put(serverlessIndex, false).transientSettings(transientSettings.build()).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(serverlessIndex).build();
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(warmingNode).add(warmNode).localNodeId("warm-node").build();

        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).nodes(discoveryNodes).build();
    }

    private RoutingAllocation newAllocation(ClusterState state) {
        return new RoutingAllocation(null, state.getRoutingNodes(), state, null, null, 0);
    }

    private ShardRouting readerShard(ClusterState state) {
        ShardId shardId = new ShardId(new Index(SERVERLESS_INDEX, state.metadata().index(SERVERLESS_INDEX).getIndexUUID()), 0);
        return TestShardRouting.newShardRouting(
            shardId,
            null,
            false,
            true,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );
    }

    public void testReaderShardCannotAllocateToAWarmingNode() {
        ClusterState state = buildClusterState("warming-node");
        RoutingAllocation allocation = newAllocation(state);
        NodeWarmupAllocationDecider decider = new NodeWarmupAllocationDecider();

        RoutingNode warmingRoutingNode = state.getRoutingNodes().node("warming-node");
        assertEquals(Decision.Type.NO, decider.canAllocate(readerShard(state), warmingRoutingNode, allocation).type());
    }

    public void testReaderShardCanAllocateToANonWarmingNode() {
        ClusterState state = buildClusterState("warming-node");
        RoutingAllocation allocation = newAllocation(state);
        NodeWarmupAllocationDecider decider = new NodeWarmupAllocationDecider();

        RoutingNode warmRoutingNode = state.getRoutingNodes().node("warm-node");
        assertEquals(Decision.Type.YES, decider.canAllocate(readerShard(state), warmRoutingNode, allocation).type());
    }

    public void testNoWarmingNamesMeansEveryNodeIsEligible() {
        ClusterState state = buildClusterState(null);
        RoutingAllocation allocation = newAllocation(state);
        NodeWarmupAllocationDecider decider = new NodeWarmupAllocationDecider();

        RoutingNode warmingRoutingNode = state.getRoutingNodes().node("warming-node");
        assertEquals(Decision.Type.YES, decider.canAllocate(readerShard(state), warmingRoutingNode, allocation).type());
    }

    public void testWriterShardIsUnaffectedByWarmingEvenOnAWarmingNode() {
        ClusterState state = buildClusterState("warming-node");
        RoutingAllocation allocation = newAllocation(state);
        NodeWarmupAllocationDecider decider = new NodeWarmupAllocationDecider();

        ShardId shardId = new ShardId(new Index(SERVERLESS_INDEX, state.metadata().index(SERVERLESS_INDEX).getIndexUUID()), 0);
        ShardRouting writerShard = TestShardRouting.newShardRouting(shardId, "warming-node", true, ShardRoutingState.STARTED);

        RoutingNode warmingRoutingNode = state.getRoutingNodes().node("warming-node");
        assertEquals(Decision.Type.YES, decider.canAllocate(writerShard, warmingRoutingNode, allocation).type());
    }

    public void testCanRemainAlwaysAllowedRegardlessOfWarming() {
        // Phase 3 gates only new placement; a node that starts warming after a shard already landed
        // is not evicted (see the decider's own javadoc on canRemain).
        ClusterState state = buildClusterState("warming-node");
        RoutingAllocation allocation = newAllocation(state);
        NodeWarmupAllocationDecider decider = new NodeWarmupAllocationDecider();

        RoutingNode warmingRoutingNode = state.getRoutingNodes().node("warming-node");
        assertEquals(Decision.Type.YES, decider.canRemain(readerShard(state), warmingRoutingNode, allocation).type());
    }
}
