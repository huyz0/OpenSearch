/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.allocation;

import org.opensearch.Version;
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

import java.util.Map;

public class ReaderShardPlacementAllocationDeciderTests extends OpenSearchAllocationTestCase {

    private static final String SERVERLESS_INDEX = "serverless-idx";
    private static final String CLASSIC_INDEX = "classic-idx";

    private ClusterState buildClusterState() {
        Settings serverlessIndexSettings = settings(Version.CURRENT).put(
            ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(),
            true
        ).build();
        IndexMetadata serverlessIndex = IndexMetadata.builder(SERVERLESS_INDEX)
            .settings(serverlessIndexSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexMetadata classicIndex = IndexMetadata.builder(CLASSIC_INDEX)
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        DiscoveryNode readerCapableNode = newNode(
            "reader-node",
            Map.of(ReaderShardPlacementAllocationDecider.READER_NODE_ATTRIBUTE, "true")
        );
        DiscoveryNode ordinaryNode = newNode("ordinary-node", Map.of());

        Metadata metadata = Metadata.builder().put(serverlessIndex, false).put(classicIndex, false).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(serverlessIndex).addAsNew(classicIndex).build();
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(readerCapableNode)
            .add(ordinaryNode)
            .localNodeId("reader-node")
            .build();

        return ClusterState.builder(org.opensearch.cluster.ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(discoveryNodes)
            .build();
    }

    private RoutingAllocation newAllocation(ClusterState state) {
        return new RoutingAllocation(null, state.getRoutingNodes(), state, null, null, 0);
    }

    public void testReaderShardOfServerlessIndexCanOnlyBeAllocatedToAReaderCapableNode() {
        ClusterState state = buildClusterState();
        RoutingAllocation allocation = newAllocation(state);
        ReaderShardPlacementAllocationDecider decider = new ReaderShardPlacementAllocationDecider();

        ShardId shardId = new ShardId(new Index(SERVERLESS_INDEX, state.metadata().index(SERVERLESS_INDEX).getIndexUUID()), 0);
        ShardRouting readerShard = TestShardRouting.newShardRouting(
            shardId,
            null,
            false,
            true,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );

        RoutingNode readerCapableRoutingNode = state.getRoutingNodes().node("reader-node");
        RoutingNode ordinaryRoutingNode = state.getRoutingNodes().node("ordinary-node");

        assertEquals(Decision.Type.YES, decider.canAllocate(readerShard, readerCapableRoutingNode, allocation).type());
        assertEquals(Decision.Type.NO, decider.canAllocate(readerShard, ordinaryRoutingNode, allocation).type());
    }

    public void testWriterShardOfServerlessIndexIsUnrestrictedOnAnOrdinaryNode() {
        ClusterState state = buildClusterState();
        RoutingAllocation allocation = newAllocation(state);
        ReaderShardPlacementAllocationDecider decider = new ReaderShardPlacementAllocationDecider();

        ShardId shardId = new ShardId(new Index(SERVERLESS_INDEX, state.metadata().index(SERVERLESS_INDEX).getIndexUUID()), 0);
        ShardRouting writerShard = TestShardRouting.newShardRouting(shardId, "ordinary-node", true, ShardRoutingState.STARTED);

        RoutingNode ordinaryRoutingNode = state.getRoutingNodes().node("ordinary-node");
        assertEquals(Decision.Type.YES, decider.canAllocate(writerShard, ordinaryRoutingNode, allocation).type());
    }

    public void testWriterShardOfServerlessIndexIsForbiddenFromAReaderDesignatedNode() {
        // The "vice versa" half of the node-role split (rfc-serverless-opensearch.md &sect;10):
        // reader-designated capacity must never be silently consumed by an ordinary writer shard.
        ClusterState state = buildClusterState();
        RoutingAllocation allocation = newAllocation(state);
        ReaderShardPlacementAllocationDecider decider = new ReaderShardPlacementAllocationDecider();

        ShardId shardId = new ShardId(new Index(SERVERLESS_INDEX, state.metadata().index(SERVERLESS_INDEX).getIndexUUID()), 0);
        ShardRouting writerShard = TestShardRouting.newShardRouting(shardId, "reader-node", true, ShardRoutingState.STARTED);

        RoutingNode readerCapableRoutingNode = state.getRoutingNodes().node("reader-node");
        assertEquals(Decision.Type.NO, decider.canAllocate(writerShard, readerCapableRoutingNode, allocation).type());
    }

    public void testWriterShardOfANonServerlessIndexIsUnrestrictedEvenOnAReaderDesignatedNode() {
        ClusterState state = buildClusterState();
        RoutingAllocation allocation = newAllocation(state);
        ReaderShardPlacementAllocationDecider decider = new ReaderShardPlacementAllocationDecider();

        ShardId shardId = new ShardId(new Index(CLASSIC_INDEX, state.metadata().index(CLASSIC_INDEX).getIndexUUID()), 0);
        ShardRouting writerShard = TestShardRouting.newShardRouting(shardId, "reader-node", true, ShardRoutingState.STARTED);

        RoutingNode readerCapableRoutingNode = state.getRoutingNodes().node("reader-node");
        assertEquals(Decision.Type.YES, decider.canAllocate(writerShard, readerCapableRoutingNode, allocation).type());
    }

    public void testReaderShardOfANonServerlessIndexHasNoOpinionFromThisDecider() {
        ClusterState state = buildClusterState();
        RoutingAllocation allocation = newAllocation(state);
        ReaderShardPlacementAllocationDecider decider = new ReaderShardPlacementAllocationDecider();

        ShardId shardId = new ShardId(new Index(CLASSIC_INDEX, state.metadata().index(CLASSIC_INDEX).getIndexUUID()), 0);
        ShardRouting readerShard = TestShardRouting.newShardRouting(
            shardId,
            null,
            false,
            true,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );

        RoutingNode ordinaryRoutingNode = state.getRoutingNodes().node("ordinary-node");
        assertEquals(Decision.Type.YES, decider.canAllocate(readerShard, ordinaryRoutingNode, allocation).type());
    }
}
