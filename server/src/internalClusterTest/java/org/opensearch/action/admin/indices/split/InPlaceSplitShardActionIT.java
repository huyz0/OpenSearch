/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.split;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.SplitShardsMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Set;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

/**
 * End-to-end proof that {@link InPlaceSplitShardAction} (dynamic-partitioning-plan.md Phase 0.6)
 * actually reaches {@code MetadataInPlaceSplitShardService} and produces a real, schedulable
 * child-shard routing entry -- exercising the whole chain built across Tasks 1-14: the action
 * itself, {@code MetadataInPlaceSplitShardService}'s routing-table wiring, and
 * {@code SplitShardsMetadata}'s own bookkeeping.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class InPlaceSplitShardActionIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "in-place-split-action-it";

    public void testInPlaceSplitShardActionCreatesRealChildRouting() {
        internalCluster().startNode();
        createIndex(INDEX_NAME, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(INDEX_NAME);

        AcknowledgedResponse response = client().execute(
            InPlaceSplitShardAction.INSTANCE,
            new InPlaceSplitShardAction.Request(INDEX_NAME, 0, 2)
        ).actionGet();
        assertTrue("in-place split request should be acknowledged", response.isAcknowledged());

        ClusterService clusterService = internalCluster().clusterService();
        IndexMetadata indexMetadata = clusterService.state().metadata().index(INDEX_NAME);
        SplitShardsMetadata splitShardsMetadata = indexMetadata.getSplitShardsMetadata();
        assertTrue("shard 0's split should be recorded as in progress", splitShardsMetadata.isSplitOfShardInProgress(0));
        Set<Integer> childIds = splitShardsMetadata.getChildShardIdsOfParent(0);
        assertEquals(2, childIds.size());

        IndexRoutingTable indexRoutingTable = clusterService.state().routingTable().index(INDEX_NAME);
        for (int childId : childIds) {
            IndexShardRoutingTable childShardTable = indexRoutingTable.shard(childId);
            assertNotNull("expected a real routing entry for child shard [" + childId + "]", childShardTable);
            ShardRouting childPrimary = childShardTable.primaryShard();
            assertTrue(
                "child primary must recover via InPlaceSplitShardRecoverySource",
                childPrimary.recoverySource() instanceof org.opensearch.cluster.routing.RecoverySource.InPlaceSplitShardRecoverySource
            );
        }
    }

    public void testInPlaceSplitShardActionCommitsAndRetiresParentRouting() throws Exception {
        internalCluster().startNode();
        createIndex(INDEX_NAME, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(INDEX_NAME);

        client().execute(InPlaceSplitShardAction.INSTANCE, new InPlaceSplitShardAction.Request(INDEX_NAME, 0, 2)).actionGet();

        ClusterService clusterService = internalCluster().clusterService();
        // The commit driver (MetadataInPlaceSplitShardCommitService) fires asynchronously off
        // cluster-state-changed events once the children reach STARTED -- wait for it rather than
        // asserting synchronously right after the split request returns.
        assertBusy(() -> {
            IndexMetadata indexMetadata = clusterService.state().metadata().index(INDEX_NAME);
            assertFalse("split should have committed by now", indexMetadata.getSplitShardsMetadata().isSplitOfShardInProgress(0));
        });

        IndexRoutingTable indexRoutingTable = clusterService.state().routingTable().index(INDEX_NAME);
        assertNull(
            "the parent shard's routing entry must be retired once the split commits -- otherwise "
                + "parent and children would be simultaneously search-visible over the same data",
            indexRoutingTable.shard(0)
        );

        SplitShardsMetadata committedMetadata = clusterService.state().metadata().index(INDEX_NAME).getSplitShardsMetadata();
        java.util.Set<Integer> activeShardIds = new java.util.HashSet<>();
        for (java.util.Iterator<Integer> it = committedMetadata.getActiveShardIterator(); it.hasNext();) {
            activeShardIds.add(it.next());
        }
        assertFalse("shard 0 (the now-retired parent) must not be reported active", activeShardIds.contains(0));
        assertEquals("exactly the 2 child shards should be active post-commit", 2, activeShardIds.size());
        for (int childId : activeShardIds) {
            assertNotNull("child shard should still be routed", indexRoutingTable.shard(childId));
        }
    }

    public void testInPlaceSplitShardActionForcesAFlushBeforeSplitting() {
        internalCluster().startNode();
        createIndex(INDEX_NAME, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(INDEX_NAME);

        // An unflushed write -- present in the translog/in-memory buffer, not yet in any published
        // segment/manifest generation. dynamic-partitioning-progress.md's "Task 18": without a
        // forced flush before splitting, this document would be silently absent from every child.
        client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").get();

        int uncommittedOpsBeforeSplit = parentShardTranslogUncommittedOps();
        assertTrue("the unflushed write must be visible as an uncommitted translog op", uncommittedOpsBeforeSplit > 0);

        client().execute(InPlaceSplitShardAction.INSTANCE, new InPlaceSplitShardAction.Request(INDEX_NAME, 0, 2)).actionGet();

        // Read immediately after the split request returns -- at this point shard 0 (the parent)
        // is still guaranteed present: the commit driver can only retire it once the children have
        // reached STARTED, which cannot have happened yet (this call only just recorded the split
        // as in-progress). Checking THIS shard's own translog specifically, not an index-aggregate
        // stat, avoids being fooled by unrelated flushes the new child shards' own engine
        // construction performs (InternalEngine flushes once during translog recovery regardless).
        int uncommittedOpsAfterSplit = parentShardTranslogUncommittedOps();
        assertEquals(
            "the split request must force a flush of the parent shard specifically before proceeding, "
                + "clearing its translog's uncommitted op count -- was "
                + uncommittedOpsBeforeSplit
                + " before",
            0,
            uncommittedOpsAfterSplit
        );
    }

    /** The parent shard (0)'s own translog uncommitted-operation count, read directly, not aggregated across the index. */
    private int parentShardTranslogUncommittedOps() {
        org.opensearch.action.admin.indices.stats.IndicesStatsResponse stats = client().admin()
            .indices()
            .prepareStats(INDEX_NAME)
            .clear()
            .setTranslog(true)
            .get();
        for (org.opensearch.action.admin.indices.stats.ShardStats shardStats : stats.getShards()) {
            if (shardStats.getShardRouting().shardId().id() == 0) {
                return shardStats.getStats().getTranslog().getUncommittedOperations();
            }
        }
        throw new AssertionError("expected to find shard 0's stats");
    }

    public void testInPlaceSplitShardActionRejectsInvalidSplitInto() {
        internalCluster().startNode();
        createIndex(INDEX_NAME, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(INDEX_NAME);

        expectThrows(
            org.opensearch.action.ActionRequestValidationException.class,
            () -> client().execute(InPlaceSplitShardAction.INSTANCE, new InPlaceSplitShardAction.Request(INDEX_NAME, 0, 1)).actionGet()
        );
    }
}
