/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.allocation;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.AllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.nodecapacity.NodeWarmupCoordinator;

/**
 * Withholds reader shard allocation from a node marked warming -- node autoscaling design doc part
 * 2 (pre-warm before rotation), Phase 3: "a new node boots, it should not become allocation-eligible
 * cold ... a warmup hook prefetches the boot set for likely-incoming shards ... then the control
 * plane flips the attribute and the node enters rotation warm." {@link NodeWarmupCoordinator} is
 * where a node is actually marked/unmarked; this decider is what makes the mark have an effect.
 *
 * <p>Reader-only, matching {@link ReaderShardPlacementAllocationDecider}'s asymmetry: a writer
 * shard's cold-start cost is dominated by lease acquisition and WAL replay, not local cache state,
 * so gating writer placement on warmup isn't part of this decider (the design doc's "writer-pool
 * floor" open question covers whether that's worth adding later). Every shard of every index that
 * hasn't opted into serverless storage is entirely unaffected, matching every other {@link
 * AllocationDecider} in this plugin.
 */
public class NodeWarmupAllocationDecider extends AllocationDecider {

    /** The decider name this class is registered under. */
    public static final String NAME = "serverless_storage_node_warmup";

    /** Creates a decider with no state; every decision is derived from cluster state passed to it per call. */
    public NodeWarmupAllocationDecider() {}

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return decide(shardRouting, node, allocation);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        // A node that starts warming after a reader shard is already assigned to it is not evicted --
        // warmup only ever gates new placement, matching how drain (Phase 2) evacuates explicitly via
        // CancelAllocationCommand rather than relying on canRemain=NO to force it.
        return allocation.decision(Decision.YES, NAME, "warmup gate only applies to new allocation, not to remaining");
    }

    private Decision decide(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.isSearchOnly() == false) {
            return allocation.decision(Decision.YES, NAME, "not a reader shard, no opinion");
        }
        IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
        boolean isServerlessStorageIndex = ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.get(indexMetadata.getSettings());
        if (isServerlessStorageIndex == false) {
            return allocation.decision(Decision.YES, NAME, "index has not opted into serverless storage, no opinion");
        }
        boolean nodeIsWarming = NodeWarmupCoordinator.currentlyWarmingNames(allocation.metadata()).contains(node.node().getName());
        if (nodeIsWarming) {
            return allocation.decision(Decision.NO, NAME, "node is warming, not yet eligible for reader shard allocation");
        }
        return allocation.decision(Decision.YES, NAME, "node is not warming");
    }
}
