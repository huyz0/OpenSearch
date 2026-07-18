/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.allocation;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.opensearch.cluster.routing.allocation.FailedShard;
import org.opensearch.cluster.routing.allocation.NodeAllocationResult;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;

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

    /** Creates an allocator with no state, since this class caches nothing (see the class-level javadoc). */
    public ServerlessStorageExistingShardsAllocator() {}

    /**
     * Test-only constructor: sets the cache-locality TTL directly without requiring a real {@link
     * ClusterService} (which {@link #setDependencies} needs only to construct a {@link
     * ReaderCacheAffinityRecorder}, itself only ever used by {@link #applyStartedShards}, not by
     * placement preference reads). {@link #cacheAffinityRecorder} stays {@code null}, so {@link
     * #applyStartedShards} is a no-op under this constructor -- fine for tests exercising only
     * placement preference.
     *
     * @param cacheAffinityTtlMillis how long a recorded affinity stays honorable; non-positive disables the preference entirely.
     */
    ServerlessStorageExistingShardsAllocator(long cacheAffinityTtlMillis) {
        this.cacheAffinityTtlMillis = cacheAffinityTtlMillis;
    }

    /** The allocator name registered via {@code ServerlessStorageIndexSettingProvider} for every serverless-storage index. */
    public static final String NAME = "serverless_storage";

    // Set once, via #setDependencies, by ServerlessStoragePlugin#createComponents -- unset (null
    // recorder, zero TTL) in every test that constructs this allocator directly with the no-arg
    // constructor, which simply disables cache-locality preference and falls back to this
    // class's original "first decider-approved node" behavior, exactly as before this feature
    // existed.
    private volatile ReaderCacheAffinityRecorder cacheAffinityRecorder;
    private volatile long cacheAffinityTtlMillis;

    /**
     * Wires this allocator's cache-locality hysteresis (rfc-serverless-opensearch.md &sect;10)
     * -- constructed eagerly by {@code ClusterPlugin#getExistingShardsAllocators}, before {@code
     * ClusterService} exists, this is the same late-setter shape {@code
     * ShardReactivationActionFilter#setDependencies} already uses for the same reason.
     *
     * @param clusterService used by the {@link ReaderCacheAffinityRecorder} this constructs to persist affinity records.
     * @param cacheAffinityTtlMillis how long a recorded affinity stays honorable; non-positive disables the preference entirely.
     */
    public void setDependencies(ClusterService clusterService, long cacheAffinityTtlMillis) {
        this.cacheAffinityRecorder = new ReaderCacheAffinityRecorder(clusterService);
        this.cacheAffinityTtlMillis = cacheAffinityTtlMillis;
    }

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
     * The node picked for {@code shardRouting}: the cache-locality-preferred node (rfc-serverless-opensearch.md
     * &sect;10) if {@code shardRouting} is a reader shard with a fresh, still-decider-approved
     * affinity record, otherwise simply the first node every {@code AllocationDecider} (node
     * attribute filters, disk watermarks, awareness, this plugin's own {@link
     * ReaderShardPlacementAllocationDecider}, etc. -- every ordinary decider still applies, only the
     * gateway/data-presence question is skipped) approves for {@code shardRouting}; {@code null} if
     * none do. {@code nodeDecisions}, if non-null, is populated with every node's decision for
     * {@link #explainUnassignedShardAllocation}'s benefit; {@link #allocateUnassigned} passes {@code
     * null} since it only needs the answer, not a full explanation.
     *
     * <p><b>The preferred node is checked directly, not found by scanning</b> -- a real regression
     * caught by code review: an earlier version of this method tracked the preferred node inside
     * the same scan used to find the first-approved fallback, which meant the scan's early-break
     * (this method's entire point on the hot real-allocation path, {@code nodeDecisions == null})
     * never fired once a preference existed, silently turning every unassigned reader-shard
     * placement into a full {@code canAllocate} scan of every node in the cluster instead of an
     * O(1) lookup. {@link RoutingNodes#node(String)} answers "is the preferred node even a
     * candidate" directly, so the real-allocation path costs one lookup plus one decider check when
     * a preference is honored, exactly as cheap as it was before this feature existed.
     */
    private DiscoveryNode firstDeciderApprovedNode(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        List<NodeAllocationResult> nodeDecisions
    ) {
        if (nodeDecisions == null) {
            String preferredNodeId = preferredCacheAffinityNodeId(shardRouting, allocation);
            if (preferredNodeId != null) {
                RoutingNode preferredRoutingNode = allocation.routingNodes().node(preferredNodeId);
                if (preferredRoutingNode != null
                    && allocation.deciders().canAllocate(shardRouting, preferredRoutingNode, allocation).type() == Decision.Type.YES) {
                    return preferredRoutingNode.node();
                }
            }
            // No preference to honor (feature disabled, writer shard, no/stale record, or the
            // preferred node itself isn't decider-approved): fall back to the first
            // decider-approved node, unchanged from this method's pre-cache-affinity behavior.
            for (RoutingNode routingNode : allocation.routingNodes()) {
                if (allocation.deciders().canAllocate(shardRouting, routingNode, allocation).type() == Decision.Type.YES) {
                    return routingNode.node();
                }
            }
            return null;
        }

        // The explain path: always needs every node's decision anyway, so it gets no benefit from
        // the fast path above's early-break -- but it must still honor the SAME cache-affinity
        // preference the real path does, or _cluster/allocation/explain reports a target that
        // doesn't match what a real reroute would actually pick. A prior version of this method
        // skipped the preference entirely on this branch, a real bug caught by code review.
        String preferredNodeId = preferredCacheAffinityNodeId(shardRouting, allocation);
        DiscoveryNode preferredTarget = null;
        DiscoveryNode firstApprovedTarget = null;
        int weightRanking = 0;
        for (RoutingNode routingNode : allocation.routingNodes()) {
            Decision decision = allocation.deciders().canAllocate(shardRouting, routingNode, allocation);
            nodeDecisions.add(new NodeAllocationResult(routingNode.node(), decision, ++weightRanking));
            if (decision.type() == Decision.Type.YES) {
                if (firstApprovedTarget == null) {
                    firstApprovedTarget = routingNode.node();
                }
                if (preferredNodeId != null && preferredNodeId.equals(routingNode.nodeId())) {
                    preferredTarget = routingNode.node();
                }
            }
        }
        return preferredTarget != null ? preferredTarget : firstApprovedTarget;
    }

    /**
     * The cache-locality-preferred node id for {@code shardRouting}, or {@code null} if this
     * feature is disabled ({@link #setDependencies} never called, or a non-positive TTL), {@code
     * shardRouting} isn't a reader shard, or no fresh affinity record exists for it.
     */
    private String preferredCacheAffinityNodeId(ShardRouting shardRouting, RoutingAllocation allocation) {
        long ttlMillis = cacheAffinityTtlMillis;
        if (ttlMillis <= 0 || shardRouting.isSearchOnly() == false) {
            return null;
        }
        IndexMetadata indexMetadata = allocation.metadata().index(shardRouting.index());
        if (indexMetadata == null || ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.get(indexMetadata.getSettings()) == false) {
            return null;
        }
        int shardId = shardRouting.id();
        if (ReaderCacheAffinityMetadata.isAffinityFresh(indexMetadata, shardId, System.currentTimeMillis(), ttlMillis) == false) {
            return null;
        }
        return ReaderCacheAffinityMetadata.preferredNodeId(indexMetadata, shardId);
    }

    @Override
    public void cleanCaches() {
        // No cache (see class javadoc).
    }

    @Override
    public void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation) {
        ReaderCacheAffinityRecorder recorder = cacheAffinityRecorder;
        if (recorder == null) {
            return;
        }
        for (ShardRouting shardRouting : startedShards) {
            if (shardRouting.isSearchOnly() == false) {
                continue;
            }
            IndexMetadata indexMetadata = allocation.metadata().index(shardRouting.index());
            if (indexMetadata == null
                || ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.get(indexMetadata.getSettings()) == false) {
                continue;
            }
            recorder.recordStarted(indexMetadata.getIndexUUID(), shardRouting.id(), shardRouting.currentNodeId());
        }
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
