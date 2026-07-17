/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.MetadataInPlaceMergeShardCommitService.StuckMergeParent;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;

public class MetadataInPlaceMergeShardCommitServiceTests extends OpenSearchTestCase {

    private static final int HIGH_FAILURE_COUNT = 100; // well above index.allocation.max_retries default

    /**
     * CC3: a revived in-place-merge parent primary that has exhausted its allocation-retry budget is
     * detected as stuck, carrying the retired children it can no longer reach.
     */
    public void testDetectsStuckMergeParent() {
        ClusterState state = mergeParentState(
            HIGH_FAILURE_COUNT,
            new RecoverySource.InPlaceMergeShardRecoverySource(Arrays.asList(childRange(1), childRange(2)))
        );

        List<StuckMergeParent> stuck = MetadataInPlaceMergeShardCommitService.findStuckMergeParents(state);

        assertEquals(1, stuck.size());
        assertEquals(0, stuck.get(0).parentShardId().id());
        assertEquals("test-index", stuck.get(0).parentShardId().getIndexName());
        assertEquals(Arrays.asList(1, 2), stuck.get(0).retiredChildShardIds());
    }

    /**
     * A revived merge parent still within its allocation-retry budget is not yet stuck -- it may still
     * recover, so it must not raise the alarm.
     */
    public void testMergeParentWithinRetryBudgetIsNotStuck() {
        // One failure, well below the index.allocation.max_retries default of 5 -- still recoverable.
        ClusterState state = mergeParentState(
            1,
            new RecoverySource.InPlaceMergeShardRecoverySource(Arrays.asList(childRange(1), childRange(2)))
        );

        assertTrue(MetadataInPlaceMergeShardCommitService.findStuckMergeParents(state).isEmpty());
    }

    /**
     * An ordinary unassigned primary that exhausted its retries -- but is not a revived merge parent --
     * is not this service's concern.
     */
    public void testNonMergeUnassignedPrimaryIsNotStuck() {
        ClusterState state = mergeParentState(HIGH_FAILURE_COUNT, RecoverySource.EmptyStoreRecoverySource.INSTANCE);

        assertTrue(MetadataInPlaceMergeShardCommitService.findStuckMergeParents(state).isEmpty());
    }

    /**
     * The stuck-parent condition must be loud and operator-actionable: driving it through the listener
     * emits a distinct WARN naming the shard and stating rollback is manual, rather than leaving a
     * silently-red shard.
     */
    public void testStuckMergeParentEmitsActionableWarning() throws Exception {
        ClusterState state = mergeParentState(
            HIGH_FAILURE_COUNT,
            new RecoverySource.InPlaceMergeShardRecoverySource(Arrays.asList(childRange(1), childRange(2)))
        );
        MetadataInPlaceMergeShardCommitService service = new MetadataInPlaceMergeShardCommitService(
            Settings.EMPTY,
            mock(ClusterService.class)
        );

        try (
            MockLogAppender appender = MockLogAppender.createForLoggers(
                LogManager.getLogger(MetadataInPlaceMergeShardCommitService.class)
            )
        ) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "stuck merge warning",
                    MetadataInPlaceMergeShardCommitService.class.getCanonicalName(),
                    Level.WARN,
                    "*In-place merge of shard [0]*cannot complete*remains recoverable*"
                )
            );
            service.clusterChanged(new ClusterChangedEvent("test", state, emptyState()));
            appender.assertAllExpectationsMatched();
        }
    }

    private static ShardRange childRange(int shardId) {
        // Two disjoint halves of the hash space; exact bounds don't matter to this service.
        return shardId == 1
            ? new ShardRange(1, Integer.MIN_VALUE, 0)
            : new ShardRange(2, 1, Integer.MAX_VALUE);
    }

    private static ClusterState mergeParentState(int numFailedAllocations, RecoverySource recoverySource) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settings).build();
        Index index = indexMetadata.getIndex();
        ShardId shardId = new ShardId(index, 0);

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
        ShardRouting primary = ShardRouting.newUnassigned(shardId, true, recoverySource, unassignedInfo);
        IndexShardRoutingTable.Builder shardBuilder = new IndexShardRoutingTable.Builder(shardId);
        shardBuilder.addShard(primary);
        IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(index);
        indexRoutingBuilder.addIndexShard(shardBuilder.build());

        DiscoveryNode localNode = new DiscoveryNode(
            "node1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(localNode).localNodeId("node1").clusterManagerNodeId("node1").build();

        return ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(nodes)
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().add(indexRoutingBuilder).build())
            .build();
    }

    private static ClusterState emptyState() {
        DiscoveryNode localNode = new DiscoveryNode(
            "node1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(localNode).localNodeId("node1").clusterManagerNodeId("node1").build();
        return ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).nodes(nodes).build();
    }
}
