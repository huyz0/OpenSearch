/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.allocation;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.opensearch.cluster.routing.allocation.FailedShard;
import org.opensearch.cluster.routing.allocation.NodeAllocationResult;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.Decision;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@link ExistingShardsAllocator} for serverless-storage indices (rfc-serverless-opensearch.md
 * &sect;7.1.2), registered under {@link #NAME} and selected per-index via {@code
 * index.allocation.existing_shards_allocator} (see {@code
 * org.opensearch.serverless.storage.ServerlessStorageIndexSettingProvider}, which sets it
 * automatically for every index that opts into serverless storage -- operators never configure
 * this setting themselves, since it's {@link
 * ExistingShardsAllocator#EXISTING_SHARDS_ALLOCATOR_SETTING}'s own {@code PrivateIndex} property
 * forbids setting it directly at index creation).
 *
 * <p>The default {@code GatewayAllocator}/{@code ShardsBatchGatewayAllocator} exist to answer "which
 * node already has an in-sync on-disk copy of this shard" -- a question that has no meaningful
 * answer for this plugin's shards, which are "no peer recovery, no segment copy" by design
 * (&sect;7.1): no node ever holds a locally-persisted authoritative copy, the object store is the
 * only durable copy, addressed via the shard's manifest/head rather than any node's local disk
 * state. Left under the default allocator, every serverless-storage shard activation would look to
 * core exactly like the case confirmed by reading {@code PrimaryShardAllocator} directly: every
 * candidate node's {@code NodeGatewayStartedShards} response has no local data, so {@code
 * orderedAllocationCandidates} is empty and the shard is left {@code UNASSIGNED} with status {@code
 * NO_VALID_SHARD_COPY} -- indefinitely, since there is no timeout or automatic fallback anywhere in
 * gateway/allocation code that converts that into an allocation; only an explicit operator command
 * escapes it. Unattended recovery is the entire point of this plugin, so that is unacceptable.
 *
 * <p>This allocator sidesteps the question instead of trying to answer it: it never calls {@code
 * TransportNodesListGatewayStartedShards} or reasons about local allocation ids at all. It simply
 * picks the first {@link org.opensearch.cluster.routing.allocation.decider.AllocationDeciders}-approved
 * node for every unassigned shard, exactly as {@link
 * #explainUnassignedShardAllocation}'s own logic mirrors for the {@code _cluster/allocation/explain}
 * API. This is safe precisely because allocation here is only ever "willingness to try," never final
 * authority: the actual correctness guarantee comes from {@code
 * ObjectStoreCommitHeadPublisher#publishCommitAsHead}'s term-fencing CAS (already implemented and
 * formally verified, &sect;6.4/&sect;6.5) -- a node that this allocator picked but that shouldn't
 * really hold the primary simply loses the fencing race on its first publish attempt and is fenced
 * out, exactly as already covered by {@code ObjectStoreCommitHeadPublisherTests}/{@code
 * ObjectStoreWriterEngineTests}. There is deliberately no other bookkeeping in this class (no
 * caches, no in-flight fetch tracking) -- there is nothing to cache when there is no fetch.
 */
public final class ServerlessStorageExistingShardsAllocator implements ExistingShardsAllocator {

    public static final String NAME = "serverless_storage";

    @Override
    public void beforeAllocation(RoutingAllocation allocation) {
        // No cache to invalidate -- this allocator never fetches or caches anything (see class javadoc).
    }

    @Override
    public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {
        // Nothing to prepare -- replica allocation goes through the exact same decider-only logic as primaries.
    }

    @Override
    public void allocateUnassigned(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        DiscoveryNode target = firstDeciderApprovedNode(shardRouting, allocation, null);
        if (target == null) {
            unassignedAllocationHandler.removeAndIgnore(UnassignedInfo.AllocationStatus.DECIDERS_NO, allocation.changes());
            return;
        }
        unassignedAllocationHandler.initialize(target.getId(), null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE, allocation.changes());
    }

    @Override
    public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting unassignedShard, RoutingAllocation routingAllocation) {
        List<NodeAllocationResult> nodeDecisions = new ArrayList<>();
        DiscoveryNode target = firstDeciderApprovedNode(unassignedShard, routingAllocation, nodeDecisions);
        if (target == null) {
            return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.DECIDERS_NO, nodeDecisions);
        }
        return AllocateUnassignedDecision.yes(target, null, nodeDecisions, false);
    }

    /**
     * The one piece of actual logic in this class: the first node every {@code AllocationDecider}
     * (node attribute filters, disk watermarks, awareness, this plugin's own {@link
     * ReaderShardPlacementAllocationDecider}, etc. -- every ordinary decider still applies, only the
     * gateway/data-presence question is skipped) approves for {@code shardRouting}, or {@code null}
     * if none do. {@code nodeDecisions}, if non-null, is populated with every node's decision for
     * {@link #explainUnassignedShardAllocation}'s benefit; {@link #allocateUnassigned} passes {@code
     * null} since it only needs the answer, not a full explanation.
     */
    private static DiscoveryNode firstDeciderApprovedNode(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        List<NodeAllocationResult> nodeDecisions
    ) {
        DiscoveryNode target = null;
        int weightRanking = 0;
        for (RoutingNode routingNode : allocation.routingNodes()) {
            Decision decision = allocation.deciders().canAllocate(shardRouting, routingNode, allocation);
            if (nodeDecisions != null) {
                nodeDecisions.add(new NodeAllocationResult(routingNode.node(), decision, ++weightRanking));
            }
            if (decision.type() == Decision.Type.YES && target == null) {
                target = routingNode.node();
                if (nodeDecisions == null) {
                    break;
                }
            }
        }
        return target;
    }

    @Override
    public void cleanCaches() {
        // No cache (see class javadoc).
    }

    @Override
    public void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation) {
        // No in-flight state to invalidate.
    }

    @Override
    public void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation) {
        // No in-flight state to invalidate. A failed shard simply becomes unassigned again and is
        // retried through allocateUnassigned on the next reroute, same as any other shard.
    }

    @Override
    public int getNumberOfInFlightFetches() {
        return 0; // this allocator never fetches anything (see class javadoc).
    }
}
