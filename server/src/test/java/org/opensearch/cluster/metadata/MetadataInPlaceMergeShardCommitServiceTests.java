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
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.mockito.ArgumentCaptor;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests the two-phase in-place merge commit/cancel driver -- the mirror of
 * {@link MetadataInPlaceSplitShardCommitServiceTests}. The commit path finalizes a pending merge once the
 * revived parent starts; the cancel path is the real automatic rollback: it proves that when the revived
 * parent never recovers, the children were never destroyed and the shard range stays servable through
 * them.
 */
public class MetadataInPlaceMergeShardCommitServiceTests extends OpenSearchTestCase {

    private static final String INDEX = "test-index";

    public void testApplyCommitDoesNothingWhileParentStillUnassigned() {
        ClusterState pending = pendingMergeState();

        ClusterState afterCommit = MetadataInPlaceMergeShardCommitService.applyCommit(pending, INDEX, 0);

        assertSame(pending, afterCommit);
        SplitShardsMetadata metadata = afterCommit.metadata().index(INDEX).getSplitShardsMetadata();
        assertTrue("merge must still be pending", metadata.isMergeOfShardInProgress(0));
        // Children untouched and still routed.
        assertNotNull(afterCommit.routingTable().index(INDEX).shard(1));
        assertNotNull(afterCommit.routingTable().index(INDEX).shard(2));
    }

    public void testApplyCommitFinalizesMergeWhenParentStarted() {
        ClusterState started = startParent(pendingMergeState());

        ClusterState afterCommit = MetadataInPlaceMergeShardCommitService.applyCommit(started, INDEX, 0);

        SplitShardsMetadata metadata = afterCommit.metadata().index(INDEX).getSplitShardsMetadata();
        assertFalse("merge marker cleared on commit", metadata.isMergeOfShardInProgress(0));
        assertFalse("parent no longer a split parent after commit", metadata.isSplitParent(0));
        assertEquals("no children recorded after commit", 0, metadata.getChildShardIdsOfParent(0).size());

        IndexRoutingTable routing = afterCommit.routingTable().index(INDEX);
        assertNotNull("parent routing must remain (now STARTED)", routing.shard(0));
        assertNull("child 1 routing retired on commit", routing.shard(1));
        assertNull("child 2 routing retired on commit", routing.shard(2));

        // Post-commit, the hash space is served by the parent again.
        assertEquals(0, metadata.getShardIdOfHash(0, randomInt()));
    }

    public void testApplyCommitIsANoOpIfMergeAlreadyCommitted() {
        ClusterState committed = MetadataInPlaceMergeShardCommitService.applyCommit(startParent(pendingMergeState()), INDEX, 0);

        ClusterState committedAgain = MetadataInPlaceMergeShardCommitService.applyCommit(committed, INDEX, 0);

        assertSame(committed, committedAgain);
    }

    /**
     * The critical rollback-proving test. Drive a pending merge whose revived parent exhausts its
     * allocation-retry budget, cancel it, and assert the merge is genuinely rolled back: the children's
     * routing/metadata is fully intact and the shard range resolves back to the children through the real
     * {@link OperationRouting#generateShardId} path -- not merely "children still present in some map".
     */
    public void testApplyCancelRollsBackToFullyServableChildren() {
        ClusterState pending = pendingMergeState();
        int maxRetries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(pending.metadata().index(INDEX).getSettings());
        ClusterState exhausted = failParentAllocation(pending, maxRetries);

        // Sanity: while pending, the (not-yet-started) parent shard is not what a hash resolves to --
        // the children still own the range.
        SplitShardsMetadata beforeCancel = exhausted.metadata().index(INDEX).getSplitShardsMetadata();
        assertTrue(beforeCancel.isMergeOfShardInProgress(0));

        // Drive the rollback through the SAME detection the live listener uses, so this test also proves
        // the cancel-trigger condition fires (break that condition and this whole test goes red).
        assertEquals(
            "an exhausted revived parent must be detected as needing rollback",
            MetadataInPlaceMergeShardCommitService.MergeCompletionState.SHOULD_CANCEL,
            MetadataInPlaceMergeShardCommitService.evaluateMergeCompletion(exhausted, exhausted.metadata().index(INDEX), 0)
        );

        ClusterState afterCancel = MetadataInPlaceMergeShardCommitService.applyCancel(exhausted, INDEX, 0);

        IndexMetadata mergedIndexMetadata = afterCancel.metadata().index(INDEX);
        SplitShardsMetadata metadata = mergedIndexMetadata.getSplitShardsMetadata();
        // Marker cleared, but the split is intact: parent still a split parent, children still recorded.
        assertFalse("pending-merge marker cleared on cancel", metadata.isMergeOfShardInProgress(0));
        assertTrue("split must survive the rollback -- parent is still a split parent", metadata.isSplitParent(0));
        assertEquals("both children still recorded after rollback", Set.of(1, 2), metadata.getChildShardIdsOfParent(0));

        // The revived parent's routing entry is gone; the children's are untouched.
        IndexRoutingTable routing = afterCancel.routingTable().index(INDEX);
        assertNull("revived parent routing removed on rollback", routing.shard(0));
        assertNotNull("child 1 routing intact", routing.shard(1));
        assertNotNull("child 2 routing intact", routing.shard(2));
        assertTrue("child 1 primary still started and serving", routing.shard(1).primaryShard().started());
        assertTrue("child 2 primary still started and serving", routing.shard(2).primaryShard().started());

        // Real serviceability: every possible document routes to a live child (1 or 2), never to the
        // dead parent (0). Sweep enough ids that both children are exercised.
        boolean sawChild1 = false, sawChild2 = false;
        for (int i = 0; i < 5000; i++) {
            int shardId = OperationRouting.generateShardId(mergedIndexMetadata, Integer.toString(i), null);
            assertNotEquals("hash must never resolve to the rolled-back parent", 0, shardId);
            assertTrue("hash must resolve to a child (1 or 2), got " + shardId, shardId == 1 || shardId == 2);
            sawChild1 |= shardId == 1;
            sawChild2 |= shardId == 2;
        }
        assertTrue("both children must actually serve part of the range", sawChild1 && sawChild2);
    }

    public void testEvaluateMergeCompletionClassifiesParentState() {
        ClusterState pending = pendingMergeState();
        int maxRetries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(pending.metadata().index(INDEX).getSettings());

        // Parent still recovering (0 failures) -> still in progress.
        assertEquals(
            MetadataInPlaceMergeShardCommitService.MergeCompletionState.STILL_IN_PROGRESS,
            MetadataInPlaceMergeShardCommitService.evaluateMergeCompletion(pending, pending.metadata().index(INDEX), 0)
        );
        // Parent within retry budget -> still in progress, not yet a rollback.
        ClusterState withinBudget = failParentAllocation(pending, maxRetries - 1);
        assertEquals(
            MetadataInPlaceMergeShardCommitService.MergeCompletionState.STILL_IN_PROGRESS,
            MetadataInPlaceMergeShardCommitService.evaluateMergeCompletion(withinBudget, withinBudget.metadata().index(INDEX), 0)
        );
        // Parent STARTED -> ready to commit.
        ClusterState started = startParent(pending);
        assertEquals(
            MetadataInPlaceMergeShardCommitService.MergeCompletionState.READY_TO_COMMIT,
            MetadataInPlaceMergeShardCommitService.evaluateMergeCompletion(started, started.metadata().index(INDEX), 0)
        );
    }

    /**
     * Gap 1 race: a cancel task is queued while the revived parent is retry-exhausted (SHOULD_CANCEL), but
     * before it executes an operator's {@code retry_failed} reroute allocates the parent and drives it to
     * STARTED (now READY_TO_COMMIT). The hardened {@link MetadataInPlaceMergeShardCommitService#applyCancel}
     * must re-check the completion state against the state it's actually applying to and no-op, rather than
     * rolling back a parent that already holds the merged data (which would wastefully discard a STARTED
     * primary; the queued commit task should retire the children instead).
     */
    public void testApplyCancelNoOpsWhenStateBecameCommitWorthy() {
        // State at the moment the cancel task actually runs: parent is now STARTED (commit-worthy).
        ClusterState nowCommitWorthy = startParent(pendingMergeState());
        assertEquals(
            MetadataInPlaceMergeShardCommitService.MergeCompletionState.READY_TO_COMMIT,
            MetadataInPlaceMergeShardCommitService.evaluateMergeCompletion(nowCommitWorthy, nowCommitWorthy.metadata().index(INDEX), 0)
        );

        ClusterState afterCancel = MetadataInPlaceMergeShardCommitService.applyCancel(nowCommitWorthy, INDEX, 0);

        // No-op: nothing rolled back.
        assertSame("cancel must no-op once the state has become commit-worthy", nowCommitWorthy, afterCancel);
        SplitShardsMetadata metadata = afterCancel.metadata().index(INDEX).getSplitShardsMetadata();
        assertTrue("merge marker must remain set so the queued commit can finalize it", metadata.isMergeOfShardInProgress(0));
        IndexRoutingTable routing = afterCancel.routingTable().index(INDEX);
        assertNotNull("STARTED parent routing must not be discarded", routing.shard(0));
        assertTrue("parent must still be STARTED", routing.shard(0).primaryShard().started());
        assertNotNull("child 1 routing intact", routing.shard(1));
        assertNotNull("child 2 routing intact", routing.shard(2));
    }

    /**
     * Gap 2: drive the real {@code clusterChanged} listener dispatch (not the static apply*
     * methods directly) and assert it submits the expected follow-up cluster-state-update task. Every other
     * test bypasses this listener method; a regression in its iteration/filtering/dispatch would pass them
     * all yet only surface in a live cluster.
     */
    public void testClusterChangedSubmitsCommitTaskWhenParentStarted() {
        ClusterService clusterService = mock(ClusterService.class);
        MetadataInPlaceMergeShardCommitService service = new MetadataInPlaceMergeShardCommitService(Settings.EMPTY, clusterService);

        ClusterState before = withClusterManagerNode(pendingMergeState());
        ClusterState after = withClusterManagerNode(startParent(pendingMergeState()));

        service.clusterChanged(new ClusterChangedEvent("test", after, before));

        ArgumentCaptor<String> sourceCaptor = ArgumentCaptor.forClass(String.class);
        verify(clusterService, times(1)).submitStateUpdateTask(sourceCaptor.capture(), any(ClusterStateUpdateTask.class));
        assertTrue(
            "listener must dispatch a commit task, got: " + sourceCaptor.getValue(),
            sourceCaptor.getValue().startsWith("commit in-place merge of shard [0]")
        );
    }

    /** A non-cluster-manager local node must make the listener a complete no-op (no task submitted). */
    public void testClusterChangedNoOpWhenNotClusterManager() {
        ClusterService clusterService = mock(ClusterService.class);
        MetadataInPlaceMergeShardCommitService service = new MetadataInPlaceMergeShardCommitService(Settings.EMPTY, clusterService);

        // States without an elected cluster-manager local node -> localNodeClusterManager() == false.
        ClusterState before = pendingMergeState();
        ClusterState after = startParent(pendingMergeState());

        service.clusterChanged(new ClusterChangedEvent("test", after, before));

        verify(clusterService, never()).submitStateUpdateTask(any(String.class), any(ClusterStateUpdateTask.class));
    }

    // --- helpers ---

    /** Attaches an elected cluster-manager local node so {@code event.localNodeClusterManager()} is true. */
    private static ClusterState withClusterManagerNode(ClusterState state) {
        DiscoveryNode node = new DiscoveryNode(
            "node1",
            "node1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        return ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder().add(node).localNodeId("node1").clusterManagerNodeId("node1").build())
            .build();
    }

    /** A committed split of shard 0 into children {1,2}, with both children STARTED (a cluster at rest). */
    private static ClusterState postSplitState() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        IndexMetadata base = IndexMetadata.builder(INDEX).settings(indexSettings).build();

        SplitShardsMetadata.Builder splitBuilder = new SplitShardsMetadata.Builder(base.getSplitShardsMetadata());
        List<ShardRange> childRanges = splitBuilder.splitShard(0, 2);
        Set<Integer> childIds = new HashSet<>();
        childRanges.forEach(r -> childIds.add(r.shardId()));
        splitBuilder.updateSplitMetadataForChildShards(0, childIds);

        IndexMetadata.Builder imBuilder = IndexMetadata.builder(base).splitShardsMetadata(splitBuilder.build());
        for (int childId : childIds) {
            imBuilder.putInSyncAllocationIds(childId, Collections.emptySet());
            imBuilder.primaryTerm(childId, 1);
        }
        IndexMetadata indexMetadata = imBuilder.build();
        Index index = indexMetadata.getIndex();

        IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(index);
        for (int childId : childIds) {
            IndexShardRoutingTable.Builder shardBuilder = new IndexShardRoutingTable.Builder(new ShardId(index, childId));
            shardBuilder.addShard(TestShardRouting.newShardRouting(new ShardId(index, childId), "node1", true, ShardRoutingState.STARTED));
            indexRoutingBuilder.addIndexShard(shardBuilder.build());
        }

        return ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().add(indexRoutingBuilder).build())
            .build();
    }

    /** Phase 1: mark the merge of shard 0 pending, reviving the parent as an UNASSIGNED primary. */
    private static ClusterState pendingMergeState() {
        return MetadataInPlaceMergeShardService.applyMergeShardRequest(
            postSplitState(),
            new InPlaceMergeShardClusterStateUpdateRequest("test", INDEX, 0),
            (cs, reason) -> cs
        );
    }

    /** Drives the revived parent primary (shard 0) to STARTED, as real allocation would. */
    private static ClusterState startParent(ClusterState state) {
        Index index = state.metadata().index(INDEX).getIndex();
        IndexRoutingTable current = state.routingTable().index(INDEX);
        IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(index);
        for (IndexShardRoutingTable shardTable : current) {
            if (shardTable.shardId().id() == 0) {
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

    /** Fails the revived parent primary's allocation {@code numFailedAllocations} times (still UNASSIGNED). */
    private static ClusterState failParentAllocation(ClusterState state, int numFailedAllocations) {
        Index index = state.metadata().index(INDEX).getIndex();
        IndexRoutingTable current = state.routingTable().index(INDEX);
        IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(index);
        for (IndexShardRoutingTable shardTable : current) {
            if (shardTable.shardId().id() == 0) {
                UnassignedInfo unassignedInfo = new UnassignedInfo(
                    UnassignedInfo.Reason.ALLOCATION_FAILED,
                    "simulated failure",
                    null,
                    numFailedAllocations,
                    0,
                    0,
                    false,
                    UnassignedInfo.AllocationStatus.DECIDERS_NO,
                    Collections.emptySet()
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
