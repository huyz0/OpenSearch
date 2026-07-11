/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.allocation.SuspendedShardsMetadata;
import org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidateEntry;
import org.opensearch.transport.client.Client;

import java.util.List;

/**
 * The "do the work" half of scale-to-zero writer suspension (rfc-serverless-opensearch.md
 * &sect;7.3), consuming {@link ScaleToZeroCandidateEntry} the same way {@code
 * CompactionSchedulerTask}'s own trigger consumes {@code CompactionPolicy}'s candidate selection --
 * this class makes no eligibility decision of its own, it only acts on {@link
 * ScaleToZeroCandidateEntry#candidate()} already being {@code true}.
 *
 * <p>Marking a shard suspended is a plain {@link IndexMetadata} custom-data mutation ({@link
 * SuspendedShardsMetadata#withShardSuspended}) submitted as an ordinary {@link
 * ClusterStateUpdateTask} -- the same shape any other cluster-state-mutating feature in core uses,
 * requiring no new customization point.
 *
 * <p><b>Eviction needs an explicit {@link CancelAllocationCommand}, not just a reroute -- a real
 * correction made during this feature's own integration testing, not the original design.</b> The
 * original design assumed {@code SuspendedShardAllocationDecider}'s {@code canRemain} returning
 * {@code NO} plus a plain {@link ClusterService#getRerouteService()} call would be enough, mirroring
 * how a disk-watermark breach evicts a shard. It is not: reading {@code LocalShardsBalancer.decideMove}
 * directly (core's actual reroute/move logic) shows a shard that fails {@code canRemain} is only ever
 * <em>moved</em> to a better node, never force-unassigned, and finding no valid target (which our own
 * decider guarantees, since it also returns {@code NO} from {@code canAllocate} everywhere while
 * suspended) simply leaves the shard exactly where it is -- core has no "no target, so unassign
 * anyway" fallback for a plain reroute. This was caught by {@code
 * ServerlessStorageShardSuspensionIT} asserting the shard's routing state directly, not by the
 * decider's own unit tests (which only ever check the decision in isolation, not what core's
 * balancer actually does with it). The fix uses the same sanctioned, already-existing mechanism an
 * operator's {@code POST _cluster/reroute} with a cancel command uses: {@link
 * CancelAllocationCommand} with {@code allowPrimary=true} (safe here specifically because {@code
 * SuspendedShardAllocationDecider} already guarantees nothing will re-allocate this shard back
 * mid-cancel) explicitly force-unassigns the shard from its current node, after which it correctly
 * stays {@code UNASSIGNED} (the decider still says {@code NO} to every candidate node).
 */
public final class ShardSuspensionCoordinator {

    private static final Logger logger = LogManager.getLogger(ShardSuspensionCoordinator.class);

    private final ClusterService clusterService;
    private final Client client;

    /**
     * Creates a coordinator.
     *
     * @param clusterService used both to mutate index metadata and to read the shard's current node.
     * @param client dispatches the {@link CancelAllocationCommand} that actually evicts the shard.
     */
    public ShardSuspensionCoordinator(ClusterService clusterService, Client client) {
        this.clusterService = clusterService;
        this.client = client;
    }

    /**
     * Marks every {@link ScaleToZeroCandidateEntry#candidate()} shard in {@code candidates} as
     * suspended, skipping any already suspended (idempotent, so calling this on every scheduled
     * evaluation tick -- most of which will find nothing new to do -- is cheap and safe).
     *
     * @param candidates one evaluation's worth of scale-to-zero candidates; only entries with
     *                   {@link ScaleToZeroCandidateEntry#candidate()} {@code true} are acted on.
     */
    public void suspendCandidates(List<ScaleToZeroCandidateEntry> candidates) {
        for (ScaleToZeroCandidateEntry entry : candidates) {
            if (entry.candidate()) {
                suspend(entry.indexUuid(), entry.shardId());
            }
        }
    }

    private void suspend(String indexUuid, int shardId) {
        clusterService.submitStateUpdateTask("serverless-storage-suspend-shard", new ClusterStateUpdateTask(Priority.NORMAL) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                IndexMetadata indexMetadata = findByUuid(currentState.metadata(), indexUuid);
                if (indexMetadata == null || SuspendedShardsMetadata.isSuspended(indexMetadata, shardId)) {
                    return currentState;
                }
                IndexMetadata updated = SuspendedShardsMetadata.withShardSuspended(indexMetadata, shardId);
                return ClusterState.builder(currentState).metadata(Metadata.builder(currentState.metadata()).put(updated, true)).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (oldState != newState) {
                    logger.info("suspended serverless-storage writer shard [" + indexUuid + "][" + shardId + "]");
                    evict(newState, indexUuid, shardId);
                }
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("failed to suspend serverless-storage writer shard [" + indexUuid + "][" + shardId + "]", e);
            }
        });
    }

    private void evict(ClusterState state, String indexUuid, int shardId) {
        IndexMetadata indexMetadata = findByUuid(state.metadata(), indexUuid);
        if (indexMetadata == null) {
            return;
        }
        String indexName = indexMetadata.getIndex().getName();
        IndexShardRoutingTable shardRoutingTable = state.routingTable().index(indexName).shard(shardId);
        ShardRouting primary = shardRoutingTable.primaryShard();
        if (primary.unassigned()) {
            return; // already evicted, or was never assigned in the first place -- nothing to cancel.
        }

        ClusterRerouteRequest reroute = new ClusterRerouteRequest();
        reroute.add(new CancelAllocationCommand(indexName, shardId, primary.currentNodeId(), true));
        client.admin().cluster().reroute(reroute, ActionListener.wrap(response -> {
            logger.info("evicted suspended serverless-storage writer shard [" + indexUuid + "][" + shardId + "]");
        }, e -> logger.warn("failed to evict suspended serverless-storage writer shard [" + indexUuid + "][" + shardId + "]", e)));
    }

    private static IndexMetadata findByUuid(Metadata metadata, String indexUuid) {
        for (IndexMetadata indexMetadata : metadata.indices().values()) {
            if (indexUuid.equals(indexMetadata.getIndexUUID())) {
                return indexMetadata;
            }
        }
        return null;
    }
}
