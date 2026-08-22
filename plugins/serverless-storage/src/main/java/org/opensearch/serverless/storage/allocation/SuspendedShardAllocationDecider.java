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
 * Enforces both writer- and reader-shard suspension (rfc-serverless-opensearch.md &sect;7.3) purely
 * through the ordinary {@link AllocationDecider} SPI -- no core diff needed, confirmed by direct
 * research into {@code ExistingShardsAllocator}/{@code TransportReplicationAction} before this was
 * written (see this feature's own commit message): a shard marked suspended via {@link
 * SuspendedShardsMetadata} simply fails both halves of ordinary allocation decision-making, which
 * core already handles correctly on its own:
 *
 * <ul>
 *   <li>{@link #canAllocate}: {@code NO} on every node keeps a not-yet-assigned suspended shard
 *       unassigned -- {@link ServerlessStorageExistingShardsAllocator#allocateUnassigned} already
 *       loops every {@code AllocationDecider} via {@code firstDeciderApprovedNode} and calls {@code
 *       removeAndIgnore} when none approve, exactly the same path an ordinary "no capacity"
 *       decision already takes. No change needed there.
 *   <li>{@link #canRemain}: {@code NO} is what actually evicts an <em>already started</em> shard --
 *       core's balancer/rebalance pass re-checks {@code canRemain} for started shards on every
 *       reroute and unassigns any that now fail it, the same mechanism that already moves a shard
 *       off a node that newly fails a disk-watermark or awareness decider. This is the actual "stop
 *       consuming compute" half of suspension: {@code ShardSuspensionCoordinator} pairs marking a
 *       shard suspended with an explicit {@code CancelAllocationCommand} so this eviction happens
 *       promptly rather than waiting for an unrelated cluster-state change (see that class's own
 *       javadoc for why {@code canRemain} alone, without the explicit cancel, does not evict a
 *       started shard with no valid relocation target).
 * </ul>
 *
 * <p>Writer and reader (search-only) copies are suspended completely independently, reading {@link
 * SuspendedShardsMetadata#isSuspended}/{@link SuspendedShardsMetadata#isReaderSuspended}
 * respectively based on {@link ShardRouting#isSearchOnly()} -- the same split {@link
 * ReaderShardPlacementAllocationDecider} already uses for placement, just applied to suspension
 * instead. Reader-shard suspension was deliberately deferred out of this class's first version
 * (writer-only); extending it required no change to the eviction/allocation mechanism itself, only
 * this per-role metadata lookup -- the mechanism was already role-agnostic by construction.
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
        IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
        boolean isServerlessStorageIndex = ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.get(indexMetadata.getSettings());
        if (isServerlessStorageIndex == false) {
            return allocation.decision(Decision.YES, NAME, "index has not opted into serverless storage, no opinion");
        }
        boolean isReader = shardRouting.isSearchOnly();
        boolean suspended = isReader
            ? SuspendedShardsMetadata.isReaderSuspended(indexMetadata, shardRouting.id())
            : SuspendedShardsMetadata.isSuspended(indexMetadata, shardRouting.id());
        if (suspended) {
            return allocation.decision(
                Decision.NO,
                NAME,
                "%s shard is suspended (scale-to-zero), awaiting reactivation",
                isReader ? "reader" : "writer"
            );
        }
        return allocation.decision(Decision.YES, NAME, "%s shard is not suspended", isReader ? "reader" : "writer");
    }
}
