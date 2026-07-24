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
 * The "do the work" half of scale-to-zero suspension, both writer and reader (rfc-serverless-opensearch.md
 * &sect;7.3), consuming {@link ScaleToZeroCandidateEntry} the same way {@code
 * CompactionSchedulerTask}'s own trigger consumes {@code CompactionPolicy}'s candidate selection --
 * this class makes no eligibility decision of its own, it only acts on a candidate already being
 * flagged eligible.
 *
 * <p>Marking a shard suspended is a plain {@link IndexMetadata} custom-data mutation ({@link
 * SuspendedShardsMetadata#withShardSuspended}/{@link SuspendedShardsMetadata#withReaderShardSuspended})
 * submitted as an ordinary {@link ClusterStateUpdateTask} -- the same shape any other
 * cluster-state-mutating feature in core uses, requiring no new customization point.
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
 *
 * <p><b>Eviction cancels every currently-assigned copy of the suspended role, not just one.</b> A
 * shard's reader role can have several concurrently-assigned search-only replica copies ({@code
 * index.number_of_search_only_replicas > 1}); suspending the reader role means evicting all of
 * them, via {@link IndexShardRoutingTable#searchOnlyReplicas()}. The writer role is not always a
 * lone primary either -- {@code index.number_of_replicas > 0} is permitted on a serverless-storage
 * index (only an explicitly requested {@code SEGMENT} replication type is rejected), so writer
 * suspension evicts the primary plus every ordinary replica via {@link
 * IndexShardRoutingTable#writerReplicas()} too.
 *
 * <p><b>Hysteresis via {@link #cooldownMillis}, rfc-serverless-opensearch.md &sect;16 Phase 4's
 * "balancer hysteresis" milestone.</b> Without it, a shard idling just past the threshold, getting a
 * single request, reactivating, and immediately idling again would suspend and reactivate on every
 * single evaluation tick -- real, wasted allocation churn (a reroute plus a {@link
 * CancelAllocationCommand} every cycle) rather than a one-time cost. {@link #suspend} checks {@link
 * SuspendedShardsMetadata#isSuspensionAllowed} before ever marking a shard suspended, skipping (not
 * suspending, not erroring) any shard reactivated more recently than {@link #cooldownMillis} ago.
 * Zero (the default for the 2-arg constructor, matching every existing caller's prior behavior)
 * disables the guard entirely.
 */
public final class ShardSuspensionCoordinator {

    private static final Logger logger = LogManager.getLogger(ShardSuspensionCoordinator.class);

    private final ClusterService clusterService;
    private final Client client;
    private final long cooldownMillis;

    /**
     * Creates a coordinator with hysteresis disabled ({@code cooldownMillis = 0}) -- equivalent to
     * the 3-arg constructor with {@code 0}, kept for callers that predate the hysteresis guard.
     *
     * @param clusterService used both to mutate index metadata and to read the shard's current node.
     * @param client dispatches the {@link CancelAllocationCommand} that actually evicts the shard.
     */
    public ShardSuspensionCoordinator(ClusterService clusterService, Client client) {
        this(clusterService, client, 0L);
    }

    /**
     * Creates a coordinator.
     *
     * @param clusterService used both to mutate index metadata and to read the shard's current node.
     * @param client dispatches the {@link CancelAllocationCommand} that actually evicts the shard.
     * @param cooldownMillis the minimum time that must have passed since a shard's own last
     *                       reactivation before it may be suspended again; non-positive disables the guard.
     */
    public ShardSuspensionCoordinator(ClusterService clusterService, Client client, long cooldownMillis) {
        this.clusterService = clusterService;
        this.client = client;
        this.cooldownMillis = cooldownMillis;
    }

    /**
     * Marks every {@link ScaleToZeroCandidateEntry#candidate()} shard's <em>writer</em> copy in
     * {@code candidates} as suspended, skipping any already suspended (idempotent, so calling this
     * on every scheduled evaluation tick -- most of which will find nothing new to do -- is cheap
     * and safe).
     *
     * @param candidates one evaluation's worth of scale-to-zero candidates; only entries with
     *                   {@link ScaleToZeroCandidateEntry#candidate()} {@code true} are acted on.
     */
    public void suspendCandidates(List<ScaleToZeroCandidateEntry> candidates) {
        for (ScaleToZeroCandidateEntry entry : candidates) {
            if (entry.candidate()) {
                suspendWriterShard(entry.indexUuid(), entry.shardId());
            }
        }
    }

    /**
     * Marks every {@link ScaleToZeroCandidateEntry#readerCandidate()} shard's <em>reader</em>
     * (search-only) copy in {@code candidates} as suspended, skipping any already suspended.
     * Independent of {@link #suspendCandidates} -- a shard's writer and reader copies are
     * suspended on their own, unrelated schedules (see {@link ScaleToZeroCandidateEntry}'s own
     * javadoc).
     *
     * @param candidates one evaluation's worth of scale-to-zero candidates; only entries with
     *                   {@link ScaleToZeroCandidateEntry#readerCandidate()} {@code true} are acted on.
     */
    public void suspendReaderCandidates(List<ScaleToZeroCandidateEntry> candidates) {
        for (ScaleToZeroCandidateEntry entry : candidates) {
            if (entry.readerCandidate()) {
                suspendReaderShard(entry.indexUuid(), entry.shardId());
            }
        }
    }

    /**
     * Marks one shard's writer copy suspended. A no-op (idempotent) if already suspended.
     *
     * @param indexUuid the index the shard belongs to.
     * @param shardId the shard number to suspend.
     */
    public void suspendWriterShard(String indexUuid, int shardId) {
        suspend(indexUuid, shardId, false);
    }

    /**
     * Marks one shard's reader (search-only) copy suspended. A no-op (idempotent) if already
     * suspended.
     *
     * @param indexUuid the index the shard belongs to.
     * @param shardId the shard number to suspend.
     */
    public void suspendReaderShard(String indexUuid, int shardId) {
        suspend(indexUuid, shardId, true);
    }

    private void suspend(String indexUuid, int shardId, boolean reader) {
        // Cheap pre-check against whatever state is already locally known (no submission, no
        // cluster-manager task-queue round trip): in steady state, most evaluated candidates on
        // most ticks are already suspended, and suspendCandidates/suspendReaderCandidates call this
        // once per candidate every single tick regardless. Skipping the submission entirely when
        // it's already a no-op avoids that queueing overhead scaling with candidate count on every
        // tick. Purely an optimization -- the authoritative check inside execute() below still runs
        // for whatever this pre-check doesn't skip, so a stale read here (this coordinator's cached
        // state momentarily behind reality) can only ever cause a harmless one-tick delay, never an
        // incorrect suspend.
        IndexMetadata preCheckMetadata = findByUuid(clusterService.state().metadata(), indexUuid);
        if (preCheckMetadata != null) {
            boolean alreadySuspendedPreCheck = reader
                ? SuspendedShardsMetadata.isReaderSuspended(preCheckMetadata, shardId)
                : SuspendedShardsMetadata.isSuspended(preCheckMetadata, shardId);
            if (alreadySuspendedPreCheck) {
                return;
            }
        }
        String role = reader ? "reader" : "writer";
        clusterService.submitStateUpdateTask("serverless-storage-suspend-shard", new ClusterStateUpdateTask(Priority.NORMAL) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                IndexMetadata indexMetadata = findByUuid(currentState.metadata(), indexUuid);
                if (indexMetadata == null) {
                    return currentState;
                }
                boolean alreadySuspended = reader
                    ? SuspendedShardsMetadata.isReaderSuspended(indexMetadata, shardId)
                    : SuspendedShardsMetadata.isSuspended(indexMetadata, shardId);
                if (alreadySuspended) {
                    return currentState;
                }
                if (SuspendedShardsMetadata.isSuspensionAllowed(
                    indexMetadata,
                    shardId,
                    reader,
                    System.currentTimeMillis(),
                    cooldownMillis
                ) == false) {
                    logger.debug(
                        "skipping suspend of serverless-storage "
                            + role
                            + " shard ["
                            + indexUuid
                            + "]["
                            + shardId
                            + "]: reactivated too recently (hysteresis)"
                    );
                    return currentState;
                }
                IndexMetadata updated = reader
                    ? SuspendedShardsMetadata.withReaderShardSuspended(indexMetadata, shardId)
                    : SuspendedShardsMetadata.withShardSuspended(indexMetadata, shardId);
                return ClusterState.builder(currentState).metadata(Metadata.builder(currentState.metadata()).put(updated, true)).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (oldState != newState) {
                    logger.info("suspended serverless-storage " + role + " shard [" + indexUuid + "][" + shardId + "]");
                    evict(newState, indexUuid, shardId, reader);
                }
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("failed to suspend serverless-storage " + role + " shard [" + indexUuid + "][" + shardId + "]", e);
            }
        });
    }

    /**
     * Force-unassigns the suspended shard's copies. Deliberately unconditional -- it does <em>not</em>
     * check whether the engine's best-effort final quiescent publish (see {@code
     * ObjectStoreWriterEngine#flushAndPublishQuiescentBestEffort}) actually succeeded, and that is
     * safe on the durability axis, not an oversight:
     *
     * <ul>
     *   <li>A publish that fails does <em>not</em> advance the durability watermark ({@code
     *       ObjectStoreWriterEngine} only calls {@code recordDurablePublication} <em>after</em> {@code
     *       publishCommitAsHead} returns true), so {@code ObjectStoreDurabilityTranslogDeletionPolicy}
     *       keeps the un-published op's local translog generation -- the acked op stays on local disk.</li>
     *   <li>Cancel-driven unassignment never deletes that local data: core's {@code
     *       IndicesStore.shardCanBeDeleted} only authorizes deletion when every copy of the shard is
     *       {@code STARTED} and none is local, but a suspended shard is {@code UNASSIGNED} and pinned
     *       there by {@code SuspendedShardAllocationDecider}, so no copy is {@code STARTED} anywhere.
     *       The local Lucene directory + translog survive on the node until reactivation, and a
     *       same-node reactivation replays them ({@code LOCAL_TRANSLOG_RECOVERY}).</li>
     *   <li>For an idle suspension candidate, prior periodic flush/publish has already published every
     *       acked write long before the quiescent flush runs, so the reactivation manifest read
     *       already covers them regardless of the quiescent publish's outcome.</li>
     * </ul>
     *
     * The only residual window -- an acked-but-unpublished op whose reactivation lands on a
     * <em>different</em> node with WAL mirroring off -- is the general, pre-existing WAL-off durability
     * limitation (unpublished ops are not cross-node durable), identical for any relocation/restart,
     * not something this eviction introduces. Gating eviction on the publish outcome would also require
     * building the coordinator-to-live-engine channel this class's own javadoc documents as out of
     * scope. See dynamic-partitioning-progress.md's scale-to-zero CONCERN 2 section, and {@code
     * ObjectStoreWriterEngineTests#testFailedQuiescentPublishDoesNotAdvanceDurabilitySoTheUnpublishedOpSurvivesLocally}.
     */
    private void evict(ClusterState state, String indexUuid, int shardId, boolean reader) {
        IndexMetadata indexMetadata = findByUuid(state.metadata(), indexUuid);
        if (indexMetadata == null) {
            // Distinguishable from "nothing to evict" below -- the index vanished (deleted)
            // between this shard being marked suspended and this eviction actually running, not a
            // real failure, but worth a log line so an operator investigating a shard that never
            // got force-unassigned can tell "index gone" apart from every other silent no-op path.
            logger.debug(
                "skipping eviction of suspended serverless-storage shard [" + indexUuid + "][" + shardId + "] -- index no longer exists"
            );
            return;
        }
        String indexName = indexMetadata.getIndex().getName();
        IndexShardRoutingTable shardRoutingTable = state.routingTable().index(indexName).shard(shardId);
        // Writer suspension must evict every writer-role copy, not just the primary: a
        // serverless-storage index can have index.number_of_replicas > 0 (ServerlessStorageIndexSettingProvider
        // only rejects an explicitly requested SEGMENT replication.type, not ordinary writer
        // replicas under the default DOCUMENT type), and SuspendedShardAllocationDecider suspends
        // both the primary and any ordinary replica alike for the writer role. Evicting only the
        // primary would leave a replica permanently STARTED -- canRemain=NO on its own never forces
        // an unassignment (see this class's own javadoc above), so nothing else would ever evict it.
        List<ShardRouting> toEvict;
        if (reader) {
            toEvict = shardRoutingTable.searchOnlyReplicas();
        } else {
            toEvict = new java.util.ArrayList<>();
            toEvict.add(shardRoutingTable.primaryShard());
            toEvict.addAll(shardRoutingTable.writerReplicas());
        }

        ClusterRerouteRequest reroute = new ClusterRerouteRequest();
        int assignedCount = 0;
        for (ShardRouting shardRouting : toEvict) {
            if (shardRouting.unassigned() == false) {
                reroute.add(new CancelAllocationCommand(indexName, shardId, shardRouting.currentNodeId(), true));
                assignedCount++;
            }
        }
        if (assignedCount == 0) {
            return; // already evicted, or was never assigned in the first place -- nothing to cancel.
        }

        String role = reader ? "reader" : "writer";
        client.admin().cluster().reroute(reroute, ActionListener.wrap(response -> {
            logger.info("evicted suspended serverless-storage " + role + " shard [" + indexUuid + "][" + shardId + "]");
        }, e -> logger.warn("failed to evict suspended serverless-storage " + role + " shard [" + indexUuid + "][" + shardId + "]", e)));
    }

    /** Invokes {@link #evict} directly against a caller-built state -- test-only visibility. */
    void evictForTesting(ClusterState state, String indexUuid, int shardId, boolean reader) {
        evict(state, indexUuid, shardId, reader);
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
