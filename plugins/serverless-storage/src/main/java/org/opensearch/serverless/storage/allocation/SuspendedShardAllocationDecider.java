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

/**
 * Enforces writer-shard suspension (rfc-serverless-opensearch.md &sect;7.3) purely through the
 * ordinary {@link AllocationDecider} SPI -- no core diff needed, confirmed by direct research into
 * {@code ExistingShardsAllocator}/{@code TransportReplicationAction} before this was written (see
 * this feature's own commit message): a shard marked suspended via {@link SuspendedShardsMetadata}
 * simply fails both halves of ordinary allocation decision-making, which core already handles
 * correctly on its own:
 *
 * <ul>
 *   <li>{@link #canAllocate}: {@code NO} on every node keeps a not-yet-assigned suspended shard
 *       unassigned -- {@link ServerlessStorageExistingShardsAllocator#allocateUnassigned} already
 *       loops every {@code AllocationDecider} via {@code firstDeciderApprovedNode} and calls {@code
 *       removeAndIgnore} when none approve, exactly the same path an ordinary "no capacity"
 *       decision already takes. No change needed there.
 *   <li>{@link #canRemain}: {@code NO} is what actually evicts an <em>already started</em> writer
 *       shard -- core's balancer/rebalance pass re-checks {@code canRemain} for started shards on
 *       every reroute and unassigns any that now fail it, the same mechanism that already moves a
 *       shard off a node that newly fails a disk-watermark or awareness decider. This is the actual
 *       "stop consuming compute" half of suspension: {@code ShardSuspensionCoordinator} pairs
 *       marking a shard suspended with an explicit {@code RerouteService#reroute} call so this
 *       eviction happens promptly rather than waiting for an unrelated cluster-state change.
 * </ul>
 *
 * <p>Scoped to non-search-only (writer) copies only, mirroring {@link
 * ReaderShardPlacementAllocationDecider}'s own {@code isSearchOnly()} split -- reader-shard
 * scale-to-zero (&sect;7.3's "reader shards scale to zero the same way") is deliberately out of
 * scope for this first increment, since a reader shard's own admission-control/staleness story
 * (already implemented, &sect;7.2) is a materially different mechanism from a writer's.
 */
public class SuspendedShardAllocationDecider extends AllocationDecider {

    /** Creates a decider with no state; every decision is derived from cluster/routing state passed to it per call. */
    public SuspendedShardAllocationDecider() {}

    /** The decider name this class is registered under. */
    public static final String NAME = "serverless_storage_suspended_shard";

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return decide(shardRouting, allocation);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return decide(shardRouting, allocation);
    }

    private Decision decide(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (shardRouting.isSearchOnly()) {
            return allocation.decision(Decision.YES, NAME, "reader shard, suspension does not apply");
        }
        IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
        boolean isServerlessStorageIndex = ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.get(indexMetadata.getSettings());
        if (isServerlessStorageIndex == false) {
            return allocation.decision(Decision.YES, NAME, "index has not opted into serverless storage, no opinion");
        }
        if (SuspendedShardsMetadata.isSuspended(indexMetadata, shardRouting.id())) {
            return allocation.decision(Decision.NO, NAME, "writer shard is suspended (scale-to-zero), awaiting reactivation");
        }
        return allocation.decision(Decision.YES, NAME, "writer shard is not suspended");
    }
}
