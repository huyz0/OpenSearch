/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.split.InPlaceMergeShardClusterStateUpdateRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MetadataInPlaceMergeShardServiceTests extends OpenSearchTestCase {

    // --- applyMergeShardRequest: success cases ---

    public void testApplyMergeShardRequestUpdatesMetadata() {
        ClusterState state = createPostSplitClusterState("test-index", 3, 0, 0, 2);
        ClusterState updatedState = applyRequest(state, newRequest("test-index", 0));

        SplitShardsMetadata mergeMetadata = updatedState.metadata().index("test-index").getSplitShardsMetadata();
        assertFalse("parent must no longer be a split parent after merge", mergeMetadata.isSplitParent(0));
        assertTrue("parent shard 0 must be active again after merge", isActive(mergeMetadata, 0));
        assertEquals("no children should remain recorded for the merged parent", 0, mergeMetadata.getChildShardIdsOfParent(0).size());
    }

    public void testApplyMergeShardRequestRoundTripsASplit() {
        // Split shard 0 into 2, commit it, then merge back -- the parent's own hash range resolves
        // to the parent again, exactly as it did pre-split (getShardIdOfHash needs no code change).
        ClusterState state = createPostSplitClusterState("test-index", 3, 0, 0, 2);
        SplitShardsMetadata beforeMerge = state.metadata().index("test-index").getSplitShardsMetadata();
        assertTrue("precondition: shard 0 is a committed split parent", beforeMerge.isSplitParent(0));

        ClusterState updatedState = applyRequest(state, newRequest("test-index", 0));

        SplitShardsMetadata afterMerge = updatedState.metadata().index("test-index").getSplitShardsMetadata();
        assertEquals(0, afterMerge.getShardIdOfHash(0, randomInt(), false));
    }

    public void testApplyMergeShardRequestCallsReroute() {
        ClusterState state = createPostSplitClusterState("test-index", 3, 0, 1, 3);

        boolean[] rerouteCalled = { false };
        MetadataInPlaceMergeShardService.applyMergeShardRequest(state, newRequest("test-index", 1), (cs, reason) -> {
            rerouteCalled[0] = true;
            assertTrue(reason.contains("shard [1]"));
            assertTrue(reason.contains("test-index"));
            return cs;
        });

        assertTrue(rerouteCalled[0]);
    }

    public void testApplyMergeShardRequestBumpsVersion() {
        ClusterState state = createPostSplitClusterState("test-index", 3, 0, 0, 2);
        long versionBefore = state.metadata().index("test-index").getVersion();

        ClusterState updatedState = applyRequest(state, newRequest("test-index", 0));

        long versionAfter = updatedState.metadata().index("test-index").getVersion();
        assertTrue(versionAfter > versionBefore);
    }

    public void testApplyMergeShardRequestPreservesExistingMetadata() {
        ClusterState state = createPostSplitClusterState("test-index", 3, 0, 0, 2);
        IndexMetadata originalMetadata = state.metadata().index("test-index");

        ClusterState updatedState = applyRequest(state, newRequest("test-index", 0));

        IndexMetadata updatedMetadata = updatedState.metadata().index("test-index");
        assertEquals(originalMetadata.getNumberOfShards(), updatedMetadata.getNumberOfShards());
        assertEquals(originalMetadata.getNumberOfReplicas(), updatedMetadata.getNumberOfReplicas());
        assertEquals(originalMetadata.getIndex(), updatedMetadata.getIndex());
    }

    // --- applyMergeShardRequest: routing table wiring ---

    public void testApplyMergeShardRequestRevivesParentPrimaryRouting() {
        ClusterState state = createPostSplitClusterState("test-index", 3, 1, 0, 2);

        ClusterState updatedState = applyRequest(state, newRequest("test-index", 0));

        IndexRoutingTable indexRoutingTable = updatedState.routingTable().index("test-index");
        IndexShardRoutingTable parentShardTable = indexRoutingTable.shard(0);
        assertNotNull("parent shard 0 must be revived after merge", parentShardTable);

        ShardRouting parentPrimary = parentShardTable.primaryShard();
        assertEquals(ShardRoutingState.UNASSIGNED, parentPrimary.state());
        assertTrue(parentPrimary.recoverySource() instanceof RecoverySource.InPlaceMergeShardRecoverySource);

        // numberOfReplicas == 1 for this fixture
        assertEquals(1, parentShardTable.replicaShards().size());
        ShardRouting parentReplica = parentShardTable.replicaShards().get(0);
        assertEquals(ShardRoutingState.UNASSIGNED, parentReplica.state());
        assertTrue(parentReplica.recoverySource() instanceof RecoverySource.PeerRecoverySource);
    }

    public void testApplyMergeShardRequestRetiresChildRoutingEntries() {
        ClusterState state = createPostSplitClusterState("test-index", 3, 0, 0, 2);
        Set<Integer> childIds = state.metadata().index("test-index").getSplitShardsMetadata().getChildShardIdsOfParent(0);
        assertEquals(2, childIds.size());

        ClusterState updatedState = applyRequest(state, newRequest("test-index", 0));

        IndexRoutingTable indexRoutingTable = updatedState.routingTable().index("test-index");
        for (int childId : childIds) {
            assertNull("child shard [" + childId + "] must be retired after merge", indexRoutingTable.shard(childId));
        }
    }

    public void testApplyMergeShardRequestPreservesSiblingShardRoutingEntries() {
        ClusterState state = createPostSplitClusterState("test-index", 3, 0, 0, 2);

        ClusterState updatedState = applyRequest(state, newRequest("test-index", 0));

        IndexRoutingTable indexRoutingTable = updatedState.routingTable().index("test-index");
        // Shards 1 and 2 (never split) must be untouched by the merge of shard 0.
        for (int shardId = 1; shardId < 3; shardId++) {
            IndexShardRoutingTable shardTable = indexRoutingTable.shard(shardId);
            assertNotNull(shardTable);
            assertEquals(ShardRoutingState.STARTED, shardTable.primaryShard().state());
        }
    }

    // --- applyMergeShardRequest: error cases ---

    public void testApplyMergeShardRequestThrowsIfNeverSplit() {
        ClusterState state = createClusterState("test-index", 3, 0);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> applyRequest(state, newRequest("test-index", 0)));
        assertTrue(e.getMessage().contains("has not been split"));
    }

    public void testApplyMergeShardRequestThrowsIfSplitStillInProgress() {
        // Split shard 0 but do NOT commit it -- it stays in-progress.
        ClusterState base = createClusterState("test-index", 3, 0);
        IndexMetadata indexMetadata = base.metadata().index("test-index");
        SplitShardsMetadata.Builder splitBuilder = new SplitShardsMetadata.Builder(indexMetadata.getSplitShardsMetadata());
        splitBuilder.splitShard(0, 2);
        IndexMetadata.Builder imBuilder = IndexMetadata.builder(indexMetadata).splitShardsMetadata(splitBuilder.build());
        ClusterState state = ClusterState.builder(base).metadata(Metadata.builder(base.metadata()).put(imBuilder)).build();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> applyRequest(state, newRequest("test-index", 0)));
        assertTrue(e.getMessage().contains("still in progress"));
    }

    public void testApplyMergeShardRequestThrowsForNonExistentIndex() {
        ClusterState state = createPostSplitClusterState("test-index", 3, 0, 0, 2);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> applyRequest(state, newRequest("non-existent-index", 0))
        );
        assertTrue(e.getMessage().contains("not found"));
    }

    public void testApplyMergeShardRequestThrowsIfChildPrimaryNotStarted() {
        // A committed split whose children are UNASSIGNED (not yet allocated) cannot be merged.
        ClusterState state = createPostSplitClusterState("test-index", 3, 0, 0, 2, ShardRoutingState.UNASSIGNED);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> applyRequest(state, newRequest("test-index", 0)));
        assertTrue(e.getMessage().contains("primary shard is not started"));
    }

    public void testApplyMergeShardRequestThrowsInMixedVersionCluster() {
        ClusterState state = createPostSplitClusterState("test-index", 3, 0, 0, 2);
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.V_3_6_0));
        nodesBuilder.add(new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.V_3_5_0));
        state = ClusterState.builder(state).nodes(nodesBuilder).build();

        ClusterState finalState = state;
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> applyRequest(finalState, newRequest("test-index", 0)));
        assertTrue(e.getMessage().contains("same version"));
    }

    public void testApplyMergeShardRequestThrowsIfNodeBelowMergeVersion() {
        ClusterState state = createPostSplitClusterState("test-index", 3, 0, 0, 2);
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.V_3_5_0));
        nodesBuilder.add(new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.V_3_5_0));
        state = ClusterState.builder(state).nodes(nodesBuilder).build();

        ClusterState finalState = state;
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> applyRequest(finalState, newRequest("test-index", 0)));
        assertTrue(e.getMessage().contains(Version.V_3_7_0.toString()));
    }

    public void testApplyMergeShardRequestThrowsIfVirtualShardsEnabled() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_VIRTUAL_SHARDS, 6)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(indexSettings).build();
        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata).build())
            .build();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> applyRequest(state, newRequest("test-index", 0)));
        assertTrue(e.getMessage().contains("virtual shards"));
    }

    // --- helpers ---

    private static boolean isActive(SplitShardsMetadata metadata, int shardId) {
        return metadata.getChildShardIdsOfParent(shardId).isEmpty() && metadata.isSplitParent(shardId) == false;
    }

    private static ClusterState applyRequest(ClusterState state, InPlaceMergeShardClusterStateUpdateRequest request) {
        return MetadataInPlaceMergeShardService.applyMergeShardRequest(state, request, (cs, reason) -> cs);
    }

    private static InPlaceMergeShardClusterStateUpdateRequest newRequest(String index, int parentShardId) {
        return new InPlaceMergeShardClusterStateUpdateRequest("test", index, parentShardId);
    }

    private ClusterState createClusterState(String indexName, int numShards, int numReplicas) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
            .build();

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(indexSettings).build();
        Index index = indexMetadata.getIndex();

        IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(index);
        for (int i = 0; i < numShards; i++) {
            indexRoutingBuilder.addIndexShard(startedShardTable(index, i, numReplicas));
        }

        return ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().add(indexRoutingBuilder).build())
            .build();
    }

    private ClusterState createPostSplitClusterState(String indexName, int numShards, int numReplicas, int parentShardId, int splitInto) {
        return createPostSplitClusterState(indexName, numShards, numReplicas, parentShardId, splitInto, ShardRoutingState.STARTED);
    }

    /**
     * Builds a cluster state in which {@code parentShardId} has already been split into
     * {@code splitInto} children AND that split has committed: the parent's routing entry is
     * retired, and each child is routed in {@code childState}. This is exactly the post-split state a
     * real cluster reaches once {@code MetadataInPlaceSplitShardCommitService} commits, and the only
     * state from which an in-place merge is legal.
     */
    private ClusterState createPostSplitClusterState(
        String indexName,
        int numShards,
        int numReplicas,
        int parentShardId,
        int splitInto,
        ShardRoutingState childState
    ) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
            .build();

        IndexMetadata baseIndexMetadata = IndexMetadata.builder(indexName).settings(indexSettings).build();

        // Split + commit the split in metadata, mirroring the split service + commit service.
        SplitShardsMetadata.Builder splitBuilder = new SplitShardsMetadata.Builder(baseIndexMetadata.getSplitShardsMetadata());
        List<ShardRange> childRanges = splitBuilder.splitShard(parentShardId, splitInto);
        Set<Integer> childIds = new HashSet<>();
        childRanges.forEach(r -> childIds.add(r.shardId()));
        splitBuilder.updateSplitMetadataForChildShards(parentShardId, childIds);
        SplitShardsMetadata committedSplit = splitBuilder.build();

        IndexMetadata.Builder imBuilder = IndexMetadata.builder(baseIndexMetadata).splitShardsMetadata(committedSplit);
        for (int childId : childIds) {
            imBuilder.putInSyncAllocationIds(childId, java.util.Collections.emptySet());
            imBuilder.primaryTerm(childId, 1);
        }
        IndexMetadata indexMetadata = imBuilder.build();
        Index index = indexMetadata.getIndex();

        IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(index);
        // Original shards except the (now retired) parent.
        for (int i = 0; i < numShards; i++) {
            if (i == parentShardId) {
                continue;
            }
            indexRoutingBuilder.addIndexShard(startedShardTable(index, i, numReplicas));
        }
        // Children, in the requested routing state.
        for (int childId : childIds) {
            IndexShardRoutingTable.Builder shardBuilder = new IndexShardRoutingTable.Builder(new ShardId(index, childId));
            if (childState == ShardRoutingState.UNASSIGNED) {
                shardBuilder.addShard(
                    ShardRouting.newUnassigned(
                        new ShardId(index, childId),
                        true,
                        RecoverySource.InPlaceSplitShardRecoverySource.INSTANCE,
                        new org.opensearch.cluster.routing.UnassignedInfo(
                            org.opensearch.cluster.routing.UnassignedInfo.Reason.INDEX_CREATED,
                            "child"
                        )
                    )
                );
            } else {
                shardBuilder.addShard(TestShardRouting.newShardRouting(new ShardId(index, childId), "node1", true, childState));
                for (int r = 0; r < numReplicas; r++) {
                    shardBuilder.addShard(
                        TestShardRouting.newShardRouting(new ShardId(index, childId), "node" + (r + 2), false, childState)
                    );
                }
            }
            indexRoutingBuilder.addIndexShard(shardBuilder.build());
        }

        return ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().add(indexRoutingBuilder).build())
            .build();
    }

    private static IndexShardRoutingTable startedShardTable(Index index, int shardId, int numReplicas) {
        IndexShardRoutingTable.Builder shardBuilder = new IndexShardRoutingTable.Builder(new ShardId(index, shardId));
        shardBuilder.addShard(TestShardRouting.newShardRouting(new ShardId(index, shardId), "node1", true, ShardRoutingState.STARTED));
        for (int r = 0; r < numReplicas; r++) {
            shardBuilder.addShard(
                TestShardRouting.newShardRouting(new ShardId(index, shardId), "node" + (r + 2), false, ShardRoutingState.STARTED)
            );
        }
        return shardBuilder.build();
    }
}
