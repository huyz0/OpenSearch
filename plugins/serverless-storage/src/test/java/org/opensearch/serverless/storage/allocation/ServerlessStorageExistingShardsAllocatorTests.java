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
import org.opensearch.serverless.storage.ServerlessStoragePlugin;

import java.util.List;

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

    // -- Cache-locality hysteresis (rfc-serverless-opensearch.md &sect;10) --

    private ClusterState buildServerlessStorageClusterStateWithAffinity(String preferredNodeId, long recordedAtMillis) {
        IndexMetadata indexMetadata = IndexMetadata.builder(INDEX_NAME)
            .settings(settings(Version.CURRENT).put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        if (preferredNodeId != null) {
            indexMetadata = ReaderCacheAffinityMetadata.withShardCacheAffinity(indexMetadata, 0, preferredNodeId, recordedAtMillis);
        }
        DiscoveryNode node1 = newNode("node1");
        DiscoveryNode node2 = newNode("node2");

        Metadata metadata = Metadata.builder().put(indexMetadata, false).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetadata).build();
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(node1).add(node2).localNodeId("node1").build();

        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).nodes(discoveryNodes).build();
    }

    private ShardRouting unassignedReaderShard(ClusterState state) {
        ShardId shardId = new ShardId(new Index(INDEX_NAME, state.metadata().index(INDEX_NAME).getIndexUUID()), 0);
        return TestShardRouting.newShardRouting(
            shardId,
            null,
            false,
            true,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );
    }

    /**
     * The node id {@link ServerlessStorageExistingShardsAllocator} would pick with no cache
     * affinity in play at all -- {@code RoutingNodes} iteration order isn't guaranteed to match
     * insertion order (it varies with the test's random seed), so every test below computes this
     * baseline dynamically from the exact same cluster state rather than assuming a fixed node id,
     * then asserts either "matches the baseline" (preference not honored) or "differs from the
     * baseline, equals the recorded node" (preference honored).
     */
    private String baselineFirstApprovedNodeId(ClusterState state, ShardRouting shard) {
        RoutingAllocation allocation = newRoutingAllocation(yesAllocationDeciders(), state);
        RecordingHandler handler = new RecordingHandler();
        new ServerlessStorageExistingShardsAllocator(0L).allocateUnassigned(shard, allocation, handler);
        return handler.initializedNodeId;
    }

    public void testPrefersTheCacheAffinityRecordedNodeOverTheFirstApprovedNode() {
        ClusterState noAffinityState = buildServerlessStorageClusterStateWithAffinity(null, 0L);
        String baselineNodeId = baselineFirstApprovedNodeId(noAffinityState, unassignedReaderShard(noAffinityState));
        String otherNodeId = "node1".equals(baselineNodeId) ? "node2" : "node1";

        ClusterState state = buildServerlessStorageClusterStateWithAffinity(otherNodeId, System.currentTimeMillis());
        RoutingAllocation allocation = newRoutingAllocation(yesAllocationDeciders(), state);
        ServerlessStorageExistingShardsAllocator allocator = new ServerlessStorageExistingShardsAllocator(60_000L);
        ShardRouting shard = unassignedReaderShard(state);

        RecordingHandler handler = new RecordingHandler();
        allocator.allocateUnassigned(shard, allocation, handler);

        assertEquals(otherNodeId, handler.initializedNodeId);
    }

    /**
     * Regression test for a real bug: {@code explainUnassignedShardAllocation} used to skip the
     * cache-affinity preference entirely and always report the first decider-approved node in
     * iteration order, even when a real {@code allocateUnassigned} call for the identical shard
     * and cluster state would have honored a fresh cache-affinity preference and picked a
     * different node -- so {@code _cluster/allocation/explain} could report a target that didn't
     * match what actually happens on a real reroute.
     */
    public void testExplainAlsoPrefersTheCacheAffinityRecordedNodeOverTheFirstApprovedNode() {
        ClusterState noAffinityState = buildServerlessStorageClusterStateWithAffinity(null, 0L);
        String baselineNodeId = baselineFirstApprovedNodeId(noAffinityState, unassignedReaderShard(noAffinityState));
        String otherNodeId = "node1".equals(baselineNodeId) ? "node2" : "node1";

        ClusterState state = buildServerlessStorageClusterStateWithAffinity(otherNodeId, System.currentTimeMillis());
        RoutingAllocation allocation = newRoutingAllocation(yesAllocationDeciders(), state);
        ServerlessStorageExistingShardsAllocator allocator = new ServerlessStorageExistingShardsAllocator(60_000L);
        ShardRouting shard = unassignedReaderShard(state);

        AllocateUnassignedDecision decision = allocator.explainUnassignedShardAllocation(shard, allocation);

        assertTrue(decision.isDecisionTaken());
        assertEquals(otherNodeId, decision.getTargetNode().getId());
    }

    /**
     * A real regression this session's own code review caught: {@code firstDeciderApprovedNode}
     * used to lose its O(1) early-exit whenever a cache-affinity preference existed, scanning
     * every node's deciders instead of checking only the preferred one. Proven here with a
     * call-counting decider, not just by asserting the right node wins (which the earlier, buggy
     * version also got right -- it just did far more work to get there).
     */
    private static final class CountingAllocationDecider extends org.opensearch.cluster.routing.allocation.decider.AllocationDecider {
        private final java.util.concurrent.atomic.AtomicInteger callCount = new java.util.concurrent.atomic.AtomicInteger();

        @Override
        public org.opensearch.cluster.routing.allocation.decider.Decision canAllocate(
            ShardRouting shardRouting,
            org.opensearch.cluster.routing.RoutingNode node,
            RoutingAllocation allocation
        ) {
            callCount.incrementAndGet();
            return org.opensearch.cluster.routing.allocation.decider.Decision.YES;
        }
    }

    public void testHonoringAFreshPreferenceCostsExactlyOneDeciderCheckNotAFullScan() {
        ClusterState noAffinityState = buildServerlessStorageClusterStateWithAffinity(null, 0L);
        String baselineNodeId = baselineFirstApprovedNodeId(noAffinityState, unassignedReaderShard(noAffinityState));
        String otherNodeId = "node1".equals(baselineNodeId) ? "node2" : "node1";

        ClusterState state = buildServerlessStorageClusterStateWithAffinity(otherNodeId, System.currentTimeMillis());
        CountingAllocationDecider countingDecider = new CountingAllocationDecider();
        RoutingAllocation allocation = newRoutingAllocation(
            new org.opensearch.cluster.routing.allocation.decider.AllocationDeciders(java.util.List.of(countingDecider)),
            state
        );
        ServerlessStorageExistingShardsAllocator allocator = new ServerlessStorageExistingShardsAllocator(60_000L);
        ShardRouting shard = unassignedReaderShard(state);

        RecordingHandler handler = new RecordingHandler();
        allocator.allocateUnassigned(shard, allocation, handler);

        assertEquals(otherNodeId, handler.initializedNodeId);
        assertEquals(
            "honoring a fresh, decider-approved preference must check only that one node, not scan every node in the cluster",
            1,
            countingDecider.callCount.get()
        );
    }

    public void testFallsBackToFirstApprovedNodeWhenNoAffinityIsRecorded() {
        ClusterState state = buildServerlessStorageClusterStateWithAffinity(null, 0L);
        String baselineNodeId = baselineFirstApprovedNodeId(state, unassignedReaderShard(state));

        RoutingAllocation allocation = newRoutingAllocation(yesAllocationDeciders(), state);
        ServerlessStorageExistingShardsAllocator allocator = new ServerlessStorageExistingShardsAllocator(60_000L);
        ShardRouting shard = unassignedReaderShard(state);

        RecordingHandler handler = new RecordingHandler();
        allocator.allocateUnassigned(shard, allocation, handler);

        assertEquals(baselineNodeId, handler.initializedNodeId);
    }

    public void testCacheAffinityIsIgnoredWhenTheFeatureIsDisabledViaNonPositiveTtl() {
        ClusterState noAffinityState = buildServerlessStorageClusterStateWithAffinity(null, 0L);
        String baselineNodeId = baselineFirstApprovedNodeId(noAffinityState, unassignedReaderShard(noAffinityState));
        String otherNodeId = "node1".equals(baselineNodeId) ? "node2" : "node1";

        ClusterState state = buildServerlessStorageClusterStateWithAffinity(otherNodeId, System.currentTimeMillis());
        RoutingAllocation allocation = newRoutingAllocation(yesAllocationDeciders(), state);
        ServerlessStorageExistingShardsAllocator allocator = new ServerlessStorageExistingShardsAllocator(0L);
        ShardRouting shard = unassignedReaderShard(state);

        RecordingHandler handler = new RecordingHandler();
        allocator.allocateUnassigned(shard, allocation, handler);

        assertEquals(baselineNodeId, handler.initializedNodeId);
    }

    public void testCacheAffinityIsIgnoredForAWriterShardEvenWhenRecorded() {
        // A recorded affinity always came from a reader shard start (see applyStartedShards); a
        // writer/primary shard of the same shard id must never consult it.
        ClusterState noAffinityState = buildServerlessStorageClusterStateWithAffinity(null, 0L);
        ShardId baselineShardId = new ShardId(new Index(INDEX_NAME, noAffinityState.metadata().index(INDEX_NAME).getIndexUUID()), 0);
        ShardRouting baselineWriterShard = TestShardRouting.newShardRouting(
            baselineShardId,
            null,
            true,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );
        String baselineNodeId = baselineFirstApprovedNodeId(noAffinityState, baselineWriterShard);
        String otherNodeId = "node1".equals(baselineNodeId) ? "node2" : "node1";

        ClusterState state = buildServerlessStorageClusterStateWithAffinity(otherNodeId, System.currentTimeMillis());
        RoutingAllocation allocation = newRoutingAllocation(yesAllocationDeciders(), state);
        ServerlessStorageExistingShardsAllocator allocator = new ServerlessStorageExistingShardsAllocator(60_000L);
        ShardId shardId = new ShardId(new Index(INDEX_NAME, state.metadata().index(INDEX_NAME).getIndexUUID()), 0);
        ShardRouting writerShard = TestShardRouting.newShardRouting(
            shardId,
            null,
            true,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );

        RecordingHandler handler = new RecordingHandler();
        allocator.allocateUnassigned(writerShard, allocation, handler);

        assertEquals(baselineNodeId, handler.initializedNodeId);
    }

    public void testApplyStartedShardsIsANoOpWithoutARecorder() {
        // The no-arg / test-only-TTL constructors never wire a ReaderCacheAffinityRecorder --
        // applyStartedShards must simply do nothing, not throw.
        ClusterState state = buildServerlessStorageClusterStateWithAffinity(null, 0L);
        RoutingAllocation allocation = newRoutingAllocation(yesAllocationDeciders(), state);
        ServerlessStorageExistingShardsAllocator allocator = new ServerlessStorageExistingShardsAllocator(60_000L);
        ShardId shardId = new ShardId(new Index(INDEX_NAME, state.metadata().index(INDEX_NAME).getIndexUUID()), 0);
        ShardRouting startedReaderShard = TestShardRouting.newShardRouting(shardId, "node2", false, true, ShardRoutingState.STARTED, null);

        allocator.applyStartedShards(List.of(startedReaderShard), allocation);
        // No exception, and (since there's no recorder) no affinity record materializes from this call alone.
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
