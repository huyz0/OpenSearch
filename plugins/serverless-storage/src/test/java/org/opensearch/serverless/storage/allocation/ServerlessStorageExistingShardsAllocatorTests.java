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
import org.opensearch.cluster.routing.RoutingChangesObserver;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.AllocationDecision;
import org.opensearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;

/**
 * Proves the sidestep rfc-serverless-opensearch.md &sect;7.1.2 describes: this allocator never
 * consults local shard-data presence at all -- only ordinary {@link AllocationDeciders}, exactly
 * like every other unassigned-shard allocation elsewhere in core -- so it allocates immediately
 * even when (unlike {@code PrimaryShardAllocator}) there is no node anywhere with a locally
 * persisted copy, which is every serverless-storage shard's permanent situation.
 */
public class ServerlessStorageExistingShardsAllocatorTests extends OpenSearchAllocationTestCase {

    private static final String INDEX_NAME = "serverless-idx";

    private ClusterState buildClusterState() {
        IndexMetadata indexMetadata = IndexMetadata.builder(INDEX_NAME)
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        DiscoveryNode node1 = newNode("node1");
        DiscoveryNode node2 = newNode("node2");

        Metadata metadata = Metadata.builder().put(indexMetadata, false).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetadata).build();
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(node1).add(node2).localNodeId("node1").build();

        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).nodes(discoveryNodes).build();
    }

    private ShardRouting unassignedShard(ClusterState state) {
        ShardId shardId = new ShardId(new Index(INDEX_NAME, state.metadata().index(INDEX_NAME).getIndexUUID()), 0);
        return TestShardRouting.newShardRouting(
            shardId,
            null,
            true,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE
        );
    }

    public void testAllocatesImmediatelyWithNoLocalDataAnywhere() {
        // Unlike PrimaryShardAllocator (which would find zero candidates here and leave the shard
        // permanently unassigned with NO_VALID_SHARD_COPY), this allocator has no data-presence
        // question to ask at all -- only whether ordinary deciders approve a node.
        ClusterState state = buildClusterState();
        RoutingAllocation allocation = newRoutingAllocation(yesAllocationDeciders(), state);
        ServerlessStorageExistingShardsAllocator allocator = new ServerlessStorageExistingShardsAllocator();
        ShardRouting shard = unassignedShard(state);

        RecordingHandler handler = new RecordingHandler();
        allocator.allocateUnassigned(shard, allocation, handler);

        assertTrue("a decider-approved node must have been initialized", handler.initializedNodeId != null);
        assertFalse("no shard should have been left ignored/unassigned", handler.ignored);
    }

    public void testRemovesAndIgnoresWhenEveryDeciderSaysNo() {
        ClusterState state = buildClusterState();
        RoutingAllocation allocation = newRoutingAllocation(noAllocationDeciders(), state);
        ServerlessStorageExistingShardsAllocator allocator = new ServerlessStorageExistingShardsAllocator();
        ShardRouting shard = unassignedShard(state);

        RecordingHandler handler = new RecordingHandler();
        allocator.allocateUnassigned(shard, allocation, handler);

        assertTrue("no node should have been initialized", handler.initializedNodeId == null);
        assertTrue("the shard should have been removed and ignored", handler.ignored);
        assertEquals(UnassignedInfo.AllocationStatus.DECIDERS_NO, handler.ignoredStatus);
    }

    public void testExplainReturnsYesWhenADeciderApprovedNodeExists() {
        ClusterState state = buildClusterState();
        RoutingAllocation allocation = newRoutingAllocation(yesAllocationDeciders(), state);
        ServerlessStorageExistingShardsAllocator allocator = new ServerlessStorageExistingShardsAllocator();
        ShardRouting shard = unassignedShard(state);

        AllocateUnassignedDecision decision = allocator.explainUnassignedShardAllocation(shard, allocation);
        assertEquals(AllocationDecision.YES, decision.getAllocationDecision());
    }

    public void testExplainReturnsNoWhenNoDeciderApprovedNodeExists() {
        ClusterState state = buildClusterState();
        RoutingAllocation allocation = newRoutingAllocation(noAllocationDeciders(), state);
        ServerlessStorageExistingShardsAllocator allocator = new ServerlessStorageExistingShardsAllocator();
        ShardRouting shard = unassignedShard(state);

        AllocateUnassignedDecision decision = allocator.explainUnassignedShardAllocation(shard, allocation);
        assertEquals(AllocationDecision.NO, decision.getAllocationDecision());
    }

    public void testNeverTracksAnyInFlightFetches() {
        assertEquals(0, new ServerlessStorageExistingShardsAllocator().getNumberOfInFlightFetches());
    }

    public void testRegisteredUnderItsOwnNameInThePlugin() {
        assertEquals("serverless_storage", ServerlessStorageExistingShardsAllocator.NAME);
    }

    private static final class RecordingHandler implements ExistingShardsAllocator.UnassignedAllocationHandler {
        private String initializedNodeId;
        private boolean ignored;
        private UnassignedInfo.AllocationStatus ignoredStatus;

        @Override
        public ShardRouting initialize(
            String nodeId,
            String existingAllocationId,
            long expectedShardSize,
            RoutingChangesObserver routingChangesObserver
        ) {
            this.initializedNodeId = nodeId;
            return null;
        }

        @Override
        public void removeAndIgnore(UnassignedInfo.AllocationStatus attempt, RoutingChangesObserver changes) {
            this.ignored = true;
            this.ignoredStatus = attempt;
        }

        @Override
        public ShardRouting updateUnassigned(UnassignedInfo unassignedInfo, RecoverySource recoverySource, RoutingChangesObserver changes) {
            throw new UnsupportedOperationException("not exercised by this allocator");
        }
    }
}
