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
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.allocation.SuspendedShardsMetadata;
import org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidateEntry;
import org.opensearch.transport.client.Client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private final boolean pruneRoutingEntry;

    /**
     * Where a gated index's suspensions are recorded, since it has no {@code IndexMetadata} to record them
     * in. Null leaves gated indices on the old path, where H9a measured that they simply never sleep.
     */
    private final GatedShardSuspensionRegistry gatedSuspensions;

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
        this(clusterService, client, cooldownMillis, false);
    }

    /**
     * Creates a coordinator, optionally pruning the {@link IndexRoutingTable} of a fully cold index.
     *
     * @param pruneRoutingEntry when true, an index whose every shard is suspended in every
     *                          configured role, and whose every copy has actually been evicted, has
     *                          its routing entry removed entirely. This is the property that makes a
     *                          quiescent tenant free to the allocator (A6 measured 17x on
     *                          steady-state reroute at 10,000 cold indices), and it is off by
     *                          default because it changes the scale-to-zero lifecycle.
     */
    public ShardSuspensionCoordinator(ClusterService clusterService, Client client, long cooldownMillis, boolean pruneRoutingEntry) {
        this(clusterService, client, cooldownMillis, pruneRoutingEntry, null);
    }

    /**
     * Creates a coordinator that can also suspend gated indices.
     *
     * @param gatedSuspensions where an index absent from cluster state records its sleeping shards. Null
     *                         keeps the pre-H9d behaviour, in which a gated index's suspension submits a
     *                         cluster state task that can only be a no-op.
     */
    public ShardSuspensionCoordinator(
        ClusterService clusterService,
        Client client,
        long cooldownMillis,
        boolean pruneRoutingEntry,
        GatedShardSuspensionRegistry gatedSuspensions
    ) {
        this.clusterService = clusterService;
        this.client = client;
        this.cooldownMillis = cooldownMillis;
        this.pruneRoutingEntry = pruneRoutingEntry;
        this.gatedSuspensions = gatedSuspensions;
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
        ClusterState currentState = clusterService.state();
        IndexMetadata preCheckMetadata = findByUuid(currentState.metadata(), indexUuid);
        if (preCheckMetadata == null && gatedSuspensions != null) {
            // A gated index. There is no metadata to rewrite, so there is nothing for a cluster state task
            // to do: H9a measured that the task is submitted, finds nothing, and returns the state
            // unchanged, which is how a gated index came to be unable to sleep at all.
            //
            // Recording it locally is not a workaround for the missing metadata, it is the only affordable
            // design. Scale-to-zero suspends shards continuously, so at a hundred million indices a
            // publication per suspension would leave the cluster manager doing nothing else. Placement
            // reads this on the next routing resolution, with no publication and no round trip.
            if (gatedSuspensions.suspend(indexUuid, shardId)) {
                logger.debug("suspended gated shard [{}][{}] without a cluster state update", indexUuid, shardId);
            }
            // Eviction still runs on every tick for the same self-healing reason as the already-suspended
            // branch below: the reroute that unassigns the copies is asynchronous and can be lost.
            evict(currentState, indexUuid, shardId, reader);
            return;
        }
        if (preCheckMetadata != null) {
            boolean alreadySuspendedPreCheck = reader
                ? SuspendedShardsMetadata.isReaderSuspended(preCheckMetadata, shardId)
                : SuspendedShardsMetadata.isSuspended(preCheckMetadata, shardId);
            if (alreadySuspendedPreCheck) {
                // Reconciliation, not just a no-op: clusterStateProcessed below only ever calls
                // evict() once, on the single tick that actually flips the suspended flag -- if that
                // one attempt is lost (a cluster-manager failover between the cluster-state commit
                // and the reroute call, or the reroute call itself failing), nothing would otherwise
                // ever retry it, since execute() below always short-circuits once already suspended.
                // evict() is already cheap and idempotent when there is nothing left to evict (its
                // own assignedCount==0 no-op below), so calling it unconditionally here on every
                // already-suspended candidate, every tick, safely self-heals a lost eviction without
                // needing any new scheduling or state to track which attempts succeeded.
                evict(currentState, indexUuid, shardId, reader);
                // Deliberately here and not in clusterStateProcessed below. Pruning needs every copy
                // to be actually UNASSIGNED, and the eviction that unassigns them is an asynchronous
                // reroute -- at the moment the suspend flag commits, the shards are still assigned,
                // so a prune attempt there would always find the condition unmet. This branch runs
                // once per already-suspended candidate on every tick, so it is the one that
                // eventually observes the settled state, and it self-heals a prune lost to a
                // cluster-manager failover for the same reason evict() above does.
                maybePruneRoutingEntry(currentState, indexUuid);
                return;
            }
        }
        SuspendShardTask task = new SuspendShardTask(indexUuid, shardId, reader);
        clusterService.submitStateUpdateTask(
            "serverless-storage-suspend-shard",
            task,
            ClusterStateTaskConfig.build(Priority.NORMAL),
            suspendExecutor,
            task
        );
    }

    /**
     * One shard's suspension, as a batchable unit.
     *
     * <p>Batched because this is submitted once per candidate shard per tick, and G1 measured a plain
     * submission at 6 to 13 ms of publication each, linear in the number of tasks. A hundred cold shards
     * was a hundred publications; folded, it is one.
     *
     * <p>{@code suspended} is the part batching would otherwise lose. Unbatched, each task saw its own
     * {@code oldState != newState} and evicted only if it had really changed something. A batch gets one
     * pair of states for every task in it, so whether a particular shard was suspended has to be recorded
     * while its transform runs. Without it, every task in a batch containing one real suspension would
     * evict.
     */
    private final class SuspendShardTask implements ClusterStateTaskListener {
        private final String indexUuid;
        private final int shardId;
        private final boolean reader;
        private volatile boolean suspended;

        SuspendShardTask(String indexUuid, int shardId, boolean reader) {
            this.indexUuid = indexUuid;
            this.shardId = shardId;
            this.reader = reader;
        }

        String role() {
            return reader ? "reader" : "writer";
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            onSuspendProcessed(this, newState);
        }

        @Override
        public void onFailure(String source, Exception e) {
            logger.warn("failed to suspend serverless-storage " + role() + " shard [" + indexUuid + "][" + shardId + "]", e);
        }
    }

    /**
     * Folds a batch of suspensions into one state, and answers each task individually.
     *
     * <p>The shape is {@code MetadataCreateIndexService#createIndexExecutor}'s, deliberately: every task
     * is recorded as its own success or failure, so one shard that cannot be suspended fails only itself
     * and the rest of the batch still applies. That also guarantees every task's listener runs, which
     * matters more here than for create-index: a suspension whose listener never fires is a shard that
     * is marked suspended in cluster state and never evicted, so it stays assigned and consuming a node.
     */
    private final ClusterStateTaskExecutor<SuspendShardTask> suspendExecutor = new ClusterStateTaskExecutor<>() {
        @Override
        public ClusterTasksResult<SuspendShardTask> execute(ClusterState currentState, List<SuspendShardTask> tasks) {
            ClusterTasksResult.Builder<SuspendShardTask> builder = ClusterTasksResult.builder();
            ClusterState state = currentState;
            for (SuspendShardTask task : tasks) {
                try {
                    ClusterState next = applySuspend(state, task);
                    task.suspended = next != state;
                    state = next;
                    builder.success(task);
                } catch (Exception e) {
                    task.suspended = false;
                    builder.failure(task, e);
                }
            }
            return builder.build(state);
        }
    };

    /** Per-task completion, so a batch does not collapse many callbacks into one. */
    private void onSuspendProcessed(SuspendShardTask task, ClusterState newState) {
        if (task.suspended) {
            logger.info("suspended serverless-storage " + task.role() + " shard [" + task.indexUuid + "][" + task.shardId + "]");
            evict(newState, task.indexUuid, task.shardId, task.reader);
        }
    }

    /**
     * One index's cold routing entry, as a batchable unit. Submitted once per cold index per tick, so
     * it has the same shape of cost as suspension and gets the same treatment.
     */
    private final class PruneRoutingEntryTask implements ClusterStateTaskListener {
        private final String indexUuid;
        private volatile boolean pruned;

        PruneRoutingEntryTask(String indexUuid) {
            this.indexUuid = indexUuid;
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            if (pruned) {
                logger.info("pruned routing entry of fully cold serverless-storage index [{}]", indexUuid);
            }
        }

        @Override
        public void onFailure(String source, Exception e) {
            logger.warn("failed to prune routing entry of serverless-storage index [" + indexUuid + "]", e);
        }
    }

    private final ClusterStateTaskExecutor<PruneRoutingEntryTask> pruneExecutor = (currentState, tasks) -> {
        ClusterStateTaskExecutor.ClusterTasksResult.Builder<PruneRoutingEntryTask> builder = ClusterStateTaskExecutor.ClusterTasksResult
            .builder();
        ClusterState state = currentState;
        for (PruneRoutingEntryTask task : tasks) {
            try {
                ClusterState next = applyPrune(state, task);
                task.pruned = next != state;
                state = next;
                builder.success(task);
            } catch (Exception e) {
                task.pruned = false;
                builder.failure(task, e);
            }
        }
        return builder.build(state);
    };

    /** The prune transform, unchanged from the unbatched task. */
    private ClusterState applyPrune(ClusterState currentState, PruneRoutingEntryTask task) {
        IndexMetadata indexMetadata = findByUuid(currentState.metadata(), task.indexUuid);
        if (indexMetadata == null || isFullyColdAndEvicted(currentState, indexMetadata) == false) {
            return currentState;
        }
        return ClusterState.builder(currentState)
            .routingTable(RoutingTable.builder(currentState.routingTable()).remove(indexMetadata.getIndex().getName()).build())
            .build();
    }

    /** The transform the unbatched task used to perform inline, unchanged. */
    private ClusterState applySuspend(ClusterState currentState, SuspendShardTask task) {
        IndexMetadata indexMetadata = findByUuid(currentState.metadata(), task.indexUuid);
        if (indexMetadata == null) {
            return currentState;
        }
        boolean alreadySuspended = task.reader
            ? SuspendedShardsMetadata.isReaderSuspended(indexMetadata, task.shardId)
            : SuspendedShardsMetadata.isSuspended(indexMetadata, task.shardId);
        if (alreadySuspended) {
            return currentState;
        }
        if (SuspendedShardsMetadata.isSuspensionAllowed(
            indexMetadata,
            task.shardId,
            task.reader,
            System.currentTimeMillis(),
            cooldownMillis
        ) == false) {
            logger.debug(
                "skipping suspend of serverless-storage "
                    + task.role()
                    + " shard ["
                    + task.indexUuid
                    + "]["
                    + task.shardId
                    + "]: reactivated too recently (hysteresis)"
            );
            return currentState;
        }
        IndexMetadata updated = task.reader
            ? SuspendedShardsMetadata.withReaderShardSuspended(indexMetadata, task.shardId)
            : SuspendedShardsMetadata.withShardSuspended(indexMetadata, task.shardId);
        return ClusterState.builder(currentState).metadata(Metadata.builder(currentState.metadata()).put(updated, true)).build();
    }

    /**
     * Force-unassigns the suspended shard's copies. Deliberately unconditional -- it does <em>not</em>
     * check whether the engine's last publish attempt succeeded, and that is safe on the durability
     * axis, not an oversight:
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
        IndexRoutingTable indexRoutingTable = state.routingTable().index(indexName);
        if (indexRoutingTable == null) {
            logger.debug(
                "skipping eviction of suspended serverless-storage shard [{}][{}] -- index has no routing table",
                indexUuid,
                shardId
            );
            return;
        }
        IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId);
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

    /**
     * Removes a fully cold index's {@link IndexRoutingTable} entry, which is what stops the allocator
     * from paying for a quiescent tenant at all.
     *
     * <p>Today a suspended shard stays in the routing table as {@code UNASSIGNED} so {@code
     * SuspendedShardAllocationDecider} can keep returning {@code NO} for it -- once per candidate
     * node, on every reroute. A6 measured that: roughly 15 ms of steady-state reroute per 1,000 cold
     * indices, linear in them, against a flat ~9 ms when the entry is simply absent. At 10,000 cold
     * indices that is 154-171 ms versus 9 ms, paid on the cluster-manager on every cluster state
     * change.
     *
     * <p>The entry is per-index but suspension is per-shard-per-role, so this fires only when the
     * <em>whole</em> index is cold: every shard suspended in the writer role, every shard suspended
     * in the reader role if the index has search-only replicas configured at all, and every copy
     * already evicted. A partially suspended index keeps its entry, because removing it would take
     * the still-serving shards down with it.
     *
     * <p>The inverse is {@link
     * org.opensearch.serverless.storage.scaletozero.action.TransportReactivateShardsAction}, which
     * recreates the entry in the same cluster-state update that clears the suspended marker.
     * Recreation is deliberately not gated on this coordinator's setting: an entry pruned while the
     * setting was on must still come back if it is turned off afterwards.
     *
     * <p>Called once per suspended shard per tick, so an N-shard index can queue up to N of these in
     * the single tick where the condition first holds. They are cheap and every one after the first
     * finds the entry already gone and no-ops in {@code execute()}, so this is left uncoordinated
     * rather than given a per-index guard whose own staleness would be a second thing to get wrong.
     */
    private void maybePruneRoutingEntry(ClusterState state, String indexUuid) {
        if (pruneRoutingEntry == false) {
            return;
        }
        // Cheap pre-check against the locally known state, same shape as suspend()'s: in steady
        // state a cold index is already pruned and this is the common case, and skipping the
        // submission avoids queueing a no-op task per cold index per tick. The authoritative check
        // runs again inside execute().
        IndexMetadata preCheck = findByUuid(state.metadata(), indexUuid);
        if (preCheck == null || isFullyColdAndEvicted(state, preCheck) == false) {
            return;
        }
        PruneRoutingEntryTask task = new PruneRoutingEntryTask(indexUuid);
        clusterService.submitStateUpdateTask(
            "serverless-storage-prune-cold-routing-entry",
            task,
            ClusterStateTaskConfig.build(Priority.NORMAL),
            pruneExecutor,
            task
        );
    }

    /**
     * True iff the index has a routing entry, every shard of it is suspended in every role the index
     * actually configures, and no copy is still assigned to a node.
     */
    private static boolean isFullyColdAndEvicted(ClusterState state, IndexMetadata indexMetadata) {
        IndexRoutingTable indexRoutingTable = state.routingTable().index(indexMetadata.getIndex().getName());
        if (indexRoutingTable == null) {
            return false; // already pruned
        }
        int shardCount = indexMetadata.getNumberOfShards();
        boolean hasReaders = indexMetadata.getNumberOfSearchOnlyReplicas() > 0;
        for (int shardId = 0; shardId < shardCount; shardId++) {
            if (SuspendedShardsMetadata.isSuspended(indexMetadata, shardId) == false) {
                return false;
            }
            if (hasReaders && SuspendedShardsMetadata.isReaderSuspended(indexMetadata, shardId) == false) {
                return false;
            }
        }
        for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
            for (ShardRouting shardRouting : shardRoutingTable) {
                if (shardRouting.unassigned() == false) {
                    return false; // eviction has not settled yet; try again next tick
                }
            }
        }
        return true;
    }

    /** Invokes {@link #maybePruneRoutingEntry} against a caller-built state -- test-only visibility. */
    void maybePruneRoutingEntryForTesting(ClusterState state, String indexUuid) {
        maybePruneRoutingEntry(state, indexUuid);
    }

    /** Whether an index is in the state {@link #maybePruneRoutingEntry} would act on -- test-only visibility. */
    static boolean isFullyColdAndEvictedForTesting(ClusterState state, IndexMetadata indexMetadata) {
        return isFullyColdAndEvicted(state, indexMetadata);
    }

    /** Invokes {@link #evict} directly against a caller-built state -- test-only visibility. */
    void evictForTesting(ClusterState state, String indexUuid, int shardId, boolean reader) {
        evict(state, indexUuid, shardId, reader);
    }

    /**
     * The uuid index for one {@link Metadata} instance, rebuilt only when that instance changes.
     *
     * <p>Two fields rather than a map keyed by metadata, because the access pattern is a tick walking
     * many shards of the same cluster state: the previous entry is the one wanted almost every time, and
     * a cache holding more would retain old {@code Metadata} instances, each of which can be gigabytes.
     */
    private volatile Metadata uuidIndexBuiltFrom;
    private volatile Map<String, IndexMetadata> uuidIndex;

    /**
     * Resolves an index by uuid, scanning once per cluster state version rather than once per call.
     *
     * <p>{@code Metadata} has no uuid lookup, so this used to scan every index on each of its five call
     * sites, and the suspend path runs per candidate shard per tick: a tick suspending a hundred shards
     * scanned the whole cluster a hundred times. Memoising against the {@code Metadata} instance is safe
     * because it is immutable and shared, so identity is a sound cache key.
     *
     * <p>This does not make the cost independent of the index count, it makes it paid once per state
     * version instead of once per shard. Removing it entirely means not needing metadata here at all,
     * which is H9c, since suspension state currently lives inside {@code IndexMetadata}.
     */
    private IndexMetadata findByUuid(Metadata metadata, String indexUuid) {
        Map<String, IndexMetadata> index = uuidIndex;
        if (metadata != uuidIndexBuiltFrom || index == null) {
            Map<String, IndexMetadata> rebuilt = new HashMap<>(metadata.indices().size());
            for (IndexMetadata indexMetadata : metadata.indices().values()) {
                rebuilt.put(indexMetadata.getIndexUUID(), indexMetadata);
            }
            index = rebuilt;
            uuidIndex = rebuilt;
            uuidIndexBuiltFrom = metadata;
        }
        return index.get(indexUuid);
    }
}
