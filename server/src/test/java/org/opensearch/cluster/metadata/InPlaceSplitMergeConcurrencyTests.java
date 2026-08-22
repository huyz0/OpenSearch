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
import org.opensearch.action.admin.indices.split.InPlaceSplitShardClusterStateUpdateRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Concurrency / racing-request coverage for the in-place split/merge feature.
 *
 * <p>Every split/merge/commit/cancel is a single {@code AckedClusterStateUpdateTask} on the elected
 * cluster-manager, so OpenSearch's own machinery guarantees they are applied one at a time, each against
 * whatever cluster state is current when it runs -- true concurrent mutation of the same state is not
 * reachable by design. What a real cluster genuinely does hit is <em>concurrent submission</em>: two
 * clients firing overlapping requests that the cluster-manager then serializes into a back-to-back pair.
 * These tests exercise exactly that -- apply request A to a state, then apply request B to A's result --
 * for every racing pair on overlapping shard ids, asserting the feature's own preconditions turn each
 * genuinely-conflicting second request into a clean {@link IllegalArgumentException} (never silent state
 * corruption or a crash), while genuinely-independent pairs both succeed.
 */
public class InPlaceSplitMergeConcurrencyTests extends OpenSearchTestCase {

    // --- Scenario 1: two concurrent operations on the SAME shard id ---

    public void testTwoConcurrentSplitsOfSameShardExactlyOneWins() {
        // Two clients both ask to split shard 0. The cluster-manager serializes them: A lands, then B is
        // applied to A's resulting state and must be cleanly rejected -- not allowed to double-split 0.
        ClusterState state = createClusterState("test-index", 3, 1);

        ClusterState afterA = applySplit(state, splitRequest("test-index", 0, 2));
        assertTrue(afterA.metadata().index("test-index").getSplitShardsMetadata().isSplitOfShardInProgress(0));
        Set<Integer> childrenAfterA = afterA.metadata().index("test-index").getSplitShardsMetadata().getChildShardIdsOfParent(0);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> applySplit(afterA, splitRequest("test-index", 0, 2))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("already in progress"));

        // The losing request left no trace: shard 0 still has exactly A's children, no second child set.
        assertEquals(childrenAfterA, afterA.metadata().index("test-index").getSplitShardsMetadata().getChildShardIdsOfParent(0));
    }

    public void testSplitThenMergeSameShardBeforeCommitRejected() {
        // Split 0 is in progress (not committed). A racing merge of 0 must be rejected: you cannot merge a
        // split that has not even finished splitting.
        ClusterState afterSplit = applySplit(createClusterState("test-index", 3, 0), splitRequest("test-index", 0, 2));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> applyMerge(afterSplit, mergeRequest("test-index", 0))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("still in progress"));
    }

    public void testMergeThenSplitSameParentRejected() {
        // Merge of parent 0 is pending (parent revived, children still live). A racing split of that same
        // parent 0 must be rejected -- 0 is still recorded as a split parent throughout the pending merge.
        ClusterState afterMerge = applyMerge(createCommittedSplitState("test-index", 3, 1, 0, 2), mergeRequest("test-index", 0));
        assertTrue(afterMerge.metadata().index("test-index").getSplitShardsMetadata().isMergeOfShardInProgress(0));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> applySplit(afterMerge, splitRequest("test-index", 0, 2))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("already been split"));
    }

    public void testMergeThenSplitOfLiveMergeChildRejected() {
        // Merge of parent 0 is pending; its children stay live and STARTED. A racing split targeting one of
        // those still-live children must be rejected: letting a child re-split mid-merge would leave the
        // pending merge permanently un-committable (its commit re-validates the children and would throw
        // inside the commit task, wedging the merge). Guarded by isChildOfInProgressMerge in the split service.
        ClusterState afterMerge = applyMerge(createCommittedSplitState("test-index", 3, 0, 0, 2), mergeRequest("test-index", 0));
        Set<Integer> childIds = afterMerge.metadata().index("test-index").getSplitShardsMetadata().getChildShardIdsOfParent(0);
        int liveChild = childIds.iterator().next();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> applySplit(afterMerge, splitRequest("test-index", liveChild, 2))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("child of an in-progress merge"));
    }

    // --- Scenario 3: two DIFFERENT, non-overlapping operations back to back ---

    public void testTwoConcurrentSplitsOfDifferentShardsBothSucceed() {
        // Two clients split two different shards. Serialized back to back, both must land independently with
        // no cross-contamination of per-shard split state.
        ClusterState state = createClusterState("test-index", 5, 0);

        ClusterState afterA = applySplit(state, splitRequest("test-index", 0, 2));
        ClusterState afterB = applySplit(afterA, splitRequest("test-index", 3, 3));

        SplitShardsMetadata md = afterB.metadata().index("test-index").getSplitShardsMetadata();
        assertTrue(md.isSplitOfShardInProgress(0));
        assertTrue(md.isSplitOfShardInProgress(3));
        assertEquals(2, md.getChildShardIdsOfParent(0).size());
        assertEquals(3, md.getChildShardIdsOfParent(3).size());
        // Child id sets are disjoint -- the second split did not reuse the first's reserved ids.
        Set<Integer> all = new HashSet<>(md.getChildShardIdsOfParent(0));
        all.addAll(md.getChildShardIdsOfParent(3));
        assertEquals(5, all.size());
    }

    public void testSplitOneShardAndMergeAnotherParentBothSucceed() {
        // Independent operations on different shards of the same index: split shard 1 while merging the
        // already-split parent 0. Both must apply cleanly with no interference.
        ClusterState state = createCommittedSplitState("test-index", 3, 0, 0, 2);

        ClusterState afterMerge = applyMerge(state, mergeRequest("test-index", 0));
        ClusterState afterSplit = applySplit(afterMerge, splitRequest("test-index", 1, 2));

        SplitShardsMetadata md = afterSplit.metadata().index("test-index").getSplitShardsMetadata();
        assertTrue("merge of 0 still pending, untouched by the split of 1", md.isMergeOfShardInProgress(0));
        assertTrue("split of 1 recorded independently", md.isSplitOfShardInProgress(1));
        // Parent 0's children are unchanged by the split of 1.
        assertEquals(2, md.getChildShardIdsOfParent(0).size());
    }

    // --- Scenario 4: a child of an in-progress split targeted by another op before its own split commits ---

    public void testSplitOfChildBeforeItsOwnSplitCommitsRejected() {
        // Split 0 is in progress; its children are UNASSIGNED (recovering), not yet STARTED. A racing split
        // targeting such a child must be rejected because the child's primary is not started.
        ClusterState afterSplit = applySplit(createClusterState("test-index", 3, 0), splitRequest("test-index", 0, 2));
        int child = afterSplit.metadata().index("test-index").getSplitShardsMetadata().getChildShardIdsOfParent(0).iterator().next();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> applySplit(afterSplit, splitRequest("test-index", child, 2))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("primary shard is not started"));
    }

    public void testMergeUsingChildOfInProgressSplitAsParentRejected() {
        // A child of an in-progress split has no children of its own -- its shard id is not an original
        // root shard id at all (it lies outside rootShardsToAllChildren's range), so trying to merge
        // "into" it is structurally impossible and must be rejected, not crash.
        ClusterState afterSplit = applySplit(createClusterState("test-index", 3, 0), splitRequest("test-index", 0, 2));
        int child = afterSplit.metadata().index("test-index").getSplitShardsMetadata().getChildShardIdsOfParent(0).iterator().next();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> applyMerge(afterSplit, mergeRequest("test-index", child))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("not an original root shard"));
    }

    // --- helpers (mirroring MetadataInPlaceSplitShardServiceTests / MetadataInPlaceMergeShardServiceTests) ---

    private static ClusterState applySplit(ClusterState state, InPlaceSplitShardClusterStateUpdateRequest request) {
        return MetadataInPlaceSplitShardService.applySplitShardRequest(state, request, (cs, reason) -> cs);
    }

    private static ClusterState applyMerge(ClusterState state, InPlaceMergeShardClusterStateUpdateRequest request) {
        return MetadataInPlaceMergeShardService.applyMergeShardRequest(state, request, (cs, reason) -> cs);
    }

    private static InPlaceSplitShardClusterStateUpdateRequest splitRequest(String index, int shardId, int splitInto) {
        return new InPlaceSplitShardClusterStateUpdateRequest("test", index, shardId, splitInto);
    }

    private static InPlaceMergeShardClusterStateUpdateRequest mergeRequest(String index, int parentShardId) {
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

    /**
     * Builds a cluster state in which {@code parentShardId} has already been split into {@code splitInto}
     * children AND that split has committed (parent routing retired, children STARTED) -- the only state
     * from which an in-place merge is legal. Mirrors the merge service test fixture.
     */
    private ClusterState createCommittedSplitState(String indexName, int numShards, int numReplicas, int parentShardId, int splitInto) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
            .build();

        IndexMetadata baseIndexMetadata = IndexMetadata.builder(indexName).settings(indexSettings).build();

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
        for (int i = 0; i < numShards; i++) {
            if (i == parentShardId) {
                continue;
            }
            indexRoutingBuilder.addIndexShard(startedShardTable(index, i, numReplicas));
        }
        for (int childId : childIds) {
            IndexShardRoutingTable.Builder shardBuilder = new IndexShardRoutingTable.Builder(new ShardId(index, childId));
            shardBuilder.addShard(TestShardRouting.newShardRouting(new ShardId(index, childId), "node1", true, ShardRoutingState.STARTED));
            for (int r = 0; r < numReplicas; r++) {
                shardBuilder.addShard(
                    TestShardRouting.newShardRouting(new ShardId(index, childId), "node" + (r + 2), false, ShardRoutingState.STARTED)
                );
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
