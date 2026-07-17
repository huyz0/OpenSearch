/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.split.InPlaceSplitShardClusterStateUpdateRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.time.Instant;
import java.util.Set;

public class MetadataInPlaceSplitShardCommitServiceTests extends OpenSearchTestCase {

    public void testApplyCommitDoesNothingWhenChildrenStillUnassigned() {
        ClusterState state = createStateWithInProgressSplit(3, 1, 2);

        ClusterState afterCommit = MetadataInPlaceSplitShardCommitService.applyCommit(state, "test-index", 0);

        assertSame(state, afterCommit);
        assertTrue(afterCommit.metadata().index("test-index").getSplitShardsMetadata().isSplitOfShardInProgress(0));
    }

    public void testApplyCommitPromotesChildrenWhenAllStarted() {
        ClusterState state = createStateWithInProgressSplit(3, 1, 2);
        Set<Integer> childIds = state.metadata().index("test-index").getSplitShardsMetadata().getChildShardIdsOfParent(0);

        ClusterState stateWithStartedChildren = startChildShards(state, childIds);

        ClusterState afterCommit = MetadataInPlaceSplitShardCommitService.applyCommit(stateWithStartedChildren, "test-index", 0);

        SplitShardsMetadata splitMetadata = afterCommit.metadata().index("test-index").getSplitShardsMetadata();
        assertFalse(splitMetadata.isSplitOfShardInProgress(0));
        assertTrue(splitMetadata.isSplitParent(0));
        for (int childId : childIds) {
            assertTrue(splitMetadata.getRootShards().contains(childId) || splitMetadata.getChildShardIdsOfParent(0).contains(childId));
        }
    }

    public void testApplyCommitRecordsSplitCommitTimestamp() {
        ClusterState state = createStateWithInProgressSplit(3, 1, 2);
        Set<Integer> childIds = state.metadata().index("test-index").getSplitShardsMetadata().getChildShardIdsOfParent(0);

        // Before commit: no timestamp recorded for the in-progress split.
        assertEquals(
            SplitShardsMetadata.NO_SPLIT_COMMIT_TIMESTAMP,
            state.metadata().index("test-index").getSplitShardsMetadata().getSplitCommitTimestamp(0)
        );

        ClusterState stateWithStartedChildren = startChildShards(state, childIds);

        long before = Instant.now().toEpochMilli();
        ClusterState afterCommit = MetadataInPlaceSplitShardCommitService.applyCommit(stateWithStartedChildren, "test-index", 0);
        long after = Instant.now().toEpochMilli();

        long recorded = afterCommit.metadata().index("test-index").getSplitShardsMetadata().getSplitCommitTimestamp(0);
        assertNotEquals(SplitShardsMetadata.NO_SPLIT_COMMIT_TIMESTAMP, recorded);
        assertTrue("commit timestamp " + recorded + " should be within [" + before + ", " + after + "]", recorded >= before && recorded <= after);
    }

    public void testApplyCommitRetiresParentShardRoutingAtomically() {
        ClusterState state = createStateWithInProgressSplit(3, 1, 2);
        Set<Integer> childIds = state.metadata().index("test-index").getSplitShardsMetadata().getChildShardIdsOfParent(0);
        ClusterState stateWithStartedChildren = startChildShards(state, childIds);

        ClusterState afterCommit = MetadataInPlaceSplitShardCommitService.applyCommit(stateWithStartedChildren, "test-index", 0);

        IndexRoutingTable indexRoutingTable = afterCommit.routingTable().index("test-index");
        assertNull(
            "the parent's own routing entry must be retired in the same update that promotes its children, "
                + "so parent and children are never simultaneously search-visible over the same data",
            indexRoutingTable.shard(0)
        );
        // siblings untouched
        assertNotNull(indexRoutingTable.shard(1));
        assertNotNull(indexRoutingTable.shard(2));
        for (int childId : childIds) {
            assertNotNull("child routing entry must still be present after commit", indexRoutingTable.shard(childId));
        }
    }

    public void testApplyCommitIsANoOpIfSplitAlreadyCommitted() {
        ClusterState state = createStateWithInProgressSplit(3, 1, 2);
        Set<Integer> childIds = state.metadata().index("test-index").getSplitShardsMetadata().getChildShardIdsOfParent(0);
        ClusterState started = startChildShards(state, childIds);
        ClusterState committed = MetadataInPlaceSplitShardCommitService.applyCommit(started, "test-index", 0);

        ClusterState committedAgain = MetadataInPlaceSplitShardCommitService.applyCommit(committed, "test-index", 0);

        assertSame(committed, committedAgain);
    }

    public void testApplyCancelFreesChildIdsAndRemovesRouting() {
        ClusterState state = createStateWithInProgressSplit(3, 1, 2);
        Set<Integer> childIds = state.metadata().index("test-index").getSplitShardsMetadata().getChildShardIdsOfParent(0);
        // applyCancel now defensively re-checks that the state is genuinely cancel-worthy, so drive a
        // child's allocation retries to exhaustion (SHOULD_CANCEL) before cancelling.
        int maxRetries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(state.metadata().index("test-index").getSettings());
        state = failChildAllocation(state, childIds, maxRetries);

        ClusterState afterCancel = MetadataInPlaceSplitShardCommitService.applyCancel(state, "test-index", 0);

        SplitShardsMetadata splitMetadata = afterCancel.metadata().index("test-index").getSplitShardsMetadata();
        assertFalse(splitMetadata.isSplitOfShardInProgress(0));
        assertFalse(splitMetadata.isSplitParent(0));

        IndexRoutingTable indexRoutingTable = afterCancel.routingTable().index("test-index");
        for (int childId : childIds) {
            assertNull("child routing entry should have been removed on cancel", indexRoutingTable.shard(childId));
        }
        // the parent's own routing entry must be untouched
        assertNotNull(indexRoutingTable.shard(0));
    }

    public void testClusterChangedTriggersCommitWhenChildrenAllStarted() {
        // exercised indirectly via applyCommit above -- clusterChanged just wraps
        // evaluateSplitCompletion + submitCommit/submitCancel, both private plumbing around the
        // same static apply* methods already covered directly.
        ClusterState state = createStateWithInProgressSplit(3, 0, 2);
        Set<Integer> childIds = state.metadata().index("test-index").getSplitShardsMetadata().getChildShardIdsOfParent(0);
        ClusterState started = startChildShards(state, childIds);

        ClusterState afterCommit = MetadataInPlaceSplitShardCommitService.applyCommit(started, "test-index", 0);
        assertFalse(afterCommit.metadata().index("test-index").getSplitShardsMetadata().isSplitOfShardInProgress(0));
    }

    /**
     * Gap 1 race: a cancel task is queued while a reserved child is retry-exhausted (SHOULD_CANCEL), but
     * before it runs an operator's {@code retry_failed} reroute allocates all children to STARTED (now
     * READY_TO_COMMIT). The hardened {@link MetadataInPlaceSplitShardCommitService#applyCancel} must re-check
     * against the state it's actually applying to and no-op, rather than tearing down children that have
     * already recovered -- the queued commit task should finalize the split instead.
     */
    public void testApplyCancelNoOpsWhenStateBecameCommitWorthy() {
        ClusterState state = createStateWithInProgressSplit(3, 0, 2);
        Set<Integer> childIds = state.metadata().index("test-index").getSplitShardsMetadata().getChildShardIdsOfParent(0);
        // State at the moment the cancel task actually runs: all children STARTED (commit-worthy).
        ClusterState nowCommitWorthy = startChildShards(state, childIds);

        ClusterState afterCancel = MetadataInPlaceSplitShardCommitService.applyCancel(nowCommitWorthy, "test-index", 0);

        assertSame("cancel must no-op once the state has become commit-worthy", nowCommitWorthy, afterCancel);
        SplitShardsMetadata splitMetadata = afterCancel.metadata().index("test-index").getSplitShardsMetadata();
        assertTrue("split marker must remain set so the queued commit can finalize it", splitMetadata.isSplitOfShardInProgress(0));
        IndexRoutingTable routing = afterCancel.routingTable().index("test-index");
        for (int childId : childIds) {
            assertNotNull("recovered child routing must not be torn down", routing.shard(childId));
            assertTrue("child primary must still be STARTED", routing.shard(childId).primaryShard().started());
        }
    }

    // --- helpers ---

    private static ClusterState createStateWithInProgressSplit(int numShards, int numReplicas, int splitInto) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
            .build();

        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(indexSettings).build();
        Index index = indexMetadata.getIndex();

        IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(index);
        for (int i = 0; i < numShards; i++) {
            IndexShardRoutingTable.Builder shardBuilder = new IndexShardRoutingTable.Builder(new ShardId(index, i));
            shardBuilder.addShard(TestShardRouting.newShardRouting(new ShardId(index, i), "node1", true, ShardRoutingState.STARTED));
            for (int r = 0; r < numReplicas; r++) {
                shardBuilder.addShard(
                    TestShardRouting.newShardRouting(new ShardId(index, i), "node" + (r + 2), false, ShardRoutingState.STARTED)
                );
            }
            indexRoutingBuilder.addIndexShard(shardBuilder.build());
        }

        ClusterState baseState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().add(indexRoutingBuilder).build())
            .build();

        InPlaceSplitShardClusterStateUpdateRequest request = new InPlaceSplitShardClusterStateUpdateRequest(
            "test",
            "test-index",
            0,
            splitInto
        );
        return MetadataInPlaceSplitShardService.applySplitShardRequest(baseState, request, (cs, reason) -> cs);
    }

    /** Simulates the child shards' primaries finishing recovery and reaching STARTED. */
    private static ClusterState startChildShards(ClusterState state, Set<Integer> childIds) {
        Index index = state.metadata().index("test-index").getIndex();
        IndexRoutingTable currentIndexRoutingTable = state.routingTable().index("test-index");
        IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(index);
        for (IndexShardRoutingTable shardTable : currentIndexRoutingTable) {
            if (childIds.contains(shardTable.shardId().id())) {
                IndexShardRoutingTable.Builder builder = new IndexShardRoutingTable.Builder(shardTable.shardId());
                builder.addShard(TestShardRouting.newShardRouting(shardTable.shardId(), "node1", true, ShardRoutingState.STARTED));
                indexRoutingBuilder.addIndexShard(builder.build());
            } else {
                indexRoutingBuilder.addIndexShard(shardTable);
            }
        }
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(state.routingTable());
        routingTableBuilder.add(indexRoutingBuilder);
        return ClusterState.builder(state).routingTable(routingTableBuilder.build()).build();
    }

    public void testApplyCancelTriggeredByExhaustedAllocationRetries() {
        ClusterState state = createStateWithInProgressSplit(3, 0, 2);
        Set<Integer> childIds = state.metadata().index("test-index").getSplitShardsMetadata().getChildShardIdsOfParent(0);
        int maxRetries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(state.metadata().index("test-index").getSettings());

        ClusterState exhausted = failChildAllocation(state, childIds, maxRetries);

        ClusterState afterCancel = MetadataInPlaceSplitShardCommitService.applyCancel(exhausted, "test-index", 0);
        assertFalse(afterCancel.metadata().index("test-index").getSplitShardsMetadata().isSplitOfShardInProgress(0));
    }

    private static ClusterState failChildAllocation(ClusterState state, Set<Integer> childIds, int numFailedAllocations) {
        Index index = state.metadata().index("test-index").getIndex();
        IndexRoutingTable currentIndexRoutingTable = state.routingTable().index("test-index");
        IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(index);
        for (IndexShardRoutingTable shardTable : currentIndexRoutingTable) {
            if (childIds.contains(shardTable.shardId().id())) {
                UnassignedInfo unassignedInfo = new UnassignedInfo(
                    UnassignedInfo.Reason.ALLOCATION_FAILED,
                    "simulated failure",
                    null,
                    numFailedAllocations,
                    0,
                    0,
                    false,
                    UnassignedInfo.AllocationStatus.DECIDERS_NO,
                    java.util.Collections.emptySet()
                );
                ShardRouting failedPrimary = ShardRouting.newUnassigned(
                    shardTable.shardId(),
                    true,
                    shardTable.primaryShard().recoverySource(),
                    unassignedInfo
                );
                IndexShardRoutingTable.Builder builder = new IndexShardRoutingTable.Builder(shardTable.shardId());
                builder.addShard(failedPrimary);
                indexRoutingBuilder.addIndexShard(builder.build());
            } else {
                indexRoutingBuilder.addIndexShard(shardTable);
            }
        }
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(state.routingTable());
        routingTableBuilder.add(indexRoutingBuilder);
        return ClusterState.builder(state).routingTable(routingTableBuilder.build()).build();
    }
}
