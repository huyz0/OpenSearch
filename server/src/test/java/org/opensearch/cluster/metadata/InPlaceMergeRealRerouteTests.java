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
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Exercises {@link MetadataInPlaceMergeShardService#applyMergeShardRequest} through a <em>real</em>
 * {@link AllocationService} reroute and shard-start cycle -- the coverage gap that let a Task-20-style
 * assertion slip past {@link MetadataInPlaceMergeShardServiceTests} (which drives an identity-function
 * reroute) and only surface as a hung real-cluster IT.
 */
public class InPlaceMergeRealRerouteTests extends OpenSearchAllocationTestCase {

    public void testMergeRevivesParentThroughRealRerouteAndCommits() {
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );

        ClusterState state = postSplitStartedState("idx", 0, 2);

        // Bring the cluster up with the two started children (mirrors a committed split at rest).
        state = ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).localNodeId("node1").clusterManagerNodeId("node1"))
            .build();
        state = allocation.reroute(state, "reroute");
        state = startInitializingShardsAndReroute(allocation, state);

        // Phase 1: mark shard 0's merge pending, reviving the parent -- children stay live -- driving the
        // REAL reroute.
        ClusterState merged = MetadataInPlaceMergeShardService.applyMergeShardRequest(
            state,
            new InPlaceMergeShardClusterStateUpdateRequest("test-merge", "idx", 0),
            allocation::reroute
        );

        // While pending, the children are still routed and serving; the parent is only recovering.
        assertTrue(merged.metadata().index("idx").getSplitShardsMetadata().isMergeOfShardInProgress(0));
        assertNotNull("child 1 still live while pending", merged.routingTable().index("idx").shard(1));
        assertNotNull("child 2 still live while pending", merged.routingTable().index("idx").shard(2));

        // Drive the revived parent primary from UNASSIGNED to STARTED through real allocation.
        merged = startInitializingShardsAndReroute(allocation, merged);
        assertTrue("parent primary must be started before commit", merged.routingTable().index("idx").shard(0).primaryShard().started());

        // Phase 2 (commit): the commit driver finalizes the merge now that the parent is STARTED.
        merged = MetadataInPlaceMergeShardCommitService.applyCommit(merged, "idx", 0);

        IndexRoutingTable routing = merged.routingTable().index("idx");
        assertNotNull("parent shard 0 must be revived", routing.shard(0));
        assertTrue("parent primary must be started", routing.shard(0).primaryShard().started());
        assertNull("child shard 1 must be retired on commit", routing.shard(1));
        assertNull("child shard 2 must be retired on commit", routing.shard(2));
        assertFalse(merged.metadata().index("idx").getSplitShardsMetadata().isMergeOfShardInProgress(0));
    }

    private ClusterState postSplitStartedState(String indexName, int parentShardId, int splitInto) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        IndexMetadata base = IndexMetadata.builder(indexName).settings(indexSettings).build();

        SplitShardsMetadata.Builder splitBuilder = new SplitShardsMetadata.Builder(base.getSplitShardsMetadata());
        List<ShardRange> childRanges = splitBuilder.splitShard(parentShardId, splitInto);
        Set<Integer> childIds = new HashSet<>();
        childRanges.forEach(r -> childIds.add(r.shardId()));
        splitBuilder.updateSplitMetadataForChildShards(parentShardId, childIds);

        IndexMetadata.Builder imBuilder = IndexMetadata.builder(base).splitShardsMetadata(splitBuilder.build());
        // The parent was a live, started primary before it was split and retired -- so its own
        // in-sync allocation set still carries the stale pre-split primary's allocation id, exactly
        // as a real committed split leaves it.
        imBuilder.putInSyncAllocationIds(parentShardId, Set.of("stale-pre-split-parent-alloc-id"));
        imBuilder.primaryTerm(parentShardId, 2);
        for (int childId : childIds) {
            imBuilder.putInSyncAllocationIds(childId, Collections.emptySet());
            imBuilder.primaryTerm(childId, 1);
        }
        IndexMetadata indexMetadata = imBuilder.build();
        Index index = indexMetadata.getIndex();

        IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(index);
        for (int childId : childIds) {
            IndexShardRoutingTable.Builder shardBuilder = new IndexShardRoutingTable.Builder(new ShardId(index, childId));
            shardBuilder.addShard(
                ShardRouting.newUnassigned(
                    new ShardId(index, childId),
                    true,
                    org.opensearch.cluster.routing.RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                    new org.opensearch.cluster.routing.UnassignedInfo(
                        org.opensearch.cluster.routing.UnassignedInfo.Reason.INDEX_CREATED,
                        "child"
                    )
                )
            );
            indexRoutingBuilder.addIndexShard(shardBuilder.build());
        }

        return ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().add(indexRoutingBuilder).build())
            .build();
    }
}
