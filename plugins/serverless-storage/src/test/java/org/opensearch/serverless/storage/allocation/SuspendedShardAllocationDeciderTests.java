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

public class SuspendedShardAllocationDeciderTests extends OpenSearchAllocationTestCase {

    private static final String SERVERLESS_INDEX = "suspend-decider-idx";
    private static final String CLASSIC_INDEX = "suspend-decider-classic-idx";

    private ClusterState buildClusterState(boolean shard0Suspended) {
        Settings serverlessIndexSettings = settings(Version.CURRENT).put(
            ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(),
            true
        ).build();
        IndexMetadata.Builder serverlessIndexBuilder = IndexMetadata.builder(SERVERLESS_INDEX)
            .settings(serverlessIndexSettings)
            .numberOfShards(1)
            .numberOfReplicas(0);
        IndexMetadata serverlessIndex = serverlessIndexBuilder.build();
        if (shard0Suspended) {
            serverlessIndex = SuspendedShardsMetadata.withShardSuspended(serverlessIndex, 0);
        }
        IndexMetadata classicIndex = IndexMetadata.builder(CLASSIC_INDEX)
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        DiscoveryNode node = newNode("node-1", Map.of());

        Metadata metadata = Metadata.builder().put(serverlessIndex, false).put(classicIndex, false).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(serverlessIndex).addAsNew(classicIndex).build();
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(node).localNodeId("node-1").build();

        return ClusterState.builder(org.opensearch.cluster.ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(discoveryNodes)
            .build();
    }

    private RoutingAllocation newAllocation(ClusterState state) {
        return new RoutingAllocation(null, state.getRoutingNodes(), state, null, null, 0);
    }

    public void testSuspendedWriterShardCannotBeAllocated() {
        ClusterState state = buildClusterState(true);
        RoutingAllocation allocation = newAllocation(state);
        SuspendedShardAllocationDecider decider = new SuspendedShardAllocationDecider();

        ShardId shardId = new ShardId(new Index(SERVERLESS_INDEX, state.metadata().index(SERVERLESS_INDEX).getIndexUUID()), 0);
        ShardRouting writerShard = TestShardRouting.newShardRouting(
            shardId,
            null,
            true,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );

        RoutingNode routingNode = state.getRoutingNodes().node("node-1");
        assertEquals(Decision.Type.NO, decider.canAllocate(writerShard, routingNode, allocation).type());
    }

    public void testSuspendedWriterShardCannotRemain() {
        ClusterState state = buildClusterState(true);
        RoutingAllocation allocation = newAllocation(state);
        SuspendedShardAllocationDecider decider = new SuspendedShardAllocationDecider();

        ShardId shardId = new ShardId(new Index(SERVERLESS_INDEX, state.metadata().index(SERVERLESS_INDEX).getIndexUUID()), 0);
        ShardRouting writerShard = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);

        RoutingNode routingNode = state.getRoutingNodes().node("node-1");
        assertEquals(
            "an already-started writer shard must be evicted once its index is marked suspended",
            Decision.Type.NO,
            decider.canRemain(writerShard, routingNode, allocation).type()
        );
    }

    public void testUnsuspendedWriterShardIsUnaffected() {
        ClusterState state = buildClusterState(false);
        RoutingAllocation allocation = newAllocation(state);
        SuspendedShardAllocationDecider decider = new SuspendedShardAllocationDecider();

        ShardId shardId = new ShardId(new Index(SERVERLESS_INDEX, state.metadata().index(SERVERLESS_INDEX).getIndexUUID()), 0);
        ShardRouting writerShard = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);

        RoutingNode routingNode = state.getRoutingNodes().node("node-1");
        assertEquals(Decision.Type.YES, decider.canRemain(writerShard, routingNode, allocation).type());
    }

    public void testReaderShardIsNeverAffectedBySuspension() {
        // Reader-shard scale-to-zero is deliberately out of scope for this decider (see its own javadoc).
        ClusterState state = buildClusterState(true);
        RoutingAllocation allocation = newAllocation(state);
        SuspendedShardAllocationDecider decider = new SuspendedShardAllocationDecider();

        ShardId shardId = new ShardId(new Index(SERVERLESS_INDEX, state.metadata().index(SERVERLESS_INDEX).getIndexUUID()), 0);
        ShardRouting readerShard = TestShardRouting.newShardRouting(
            shardId,
            null,
            false,
            true,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );

        RoutingNode routingNode = state.getRoutingNodes().node("node-1");
        assertEquals(Decision.Type.YES, decider.canAllocate(readerShard, routingNode, allocation).type());
    }

    public void testNonServerlessIndexIsUnaffected() {
        ClusterState state = buildClusterState(true);
        RoutingAllocation allocation = newAllocation(state);
        SuspendedShardAllocationDecider decider = new SuspendedShardAllocationDecider();

        ShardId shardId = new ShardId(new Index(CLASSIC_INDEX, state.metadata().index(CLASSIC_INDEX).getIndexUUID()), 0);
        ShardRouting writerShard = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);

        RoutingNode routingNode = state.getRoutingNodes().node("node-1");
        assertEquals(Decision.Type.YES, decider.canRemain(writerShard, routingNode, allocation).type());
    }
}
