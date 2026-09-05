/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.allocation.ReaderCacheAffinityMetadata;
import org.opensearch.serverless.storage.scaletozero.ScaleToZeroCandidatesSchedulerTask;
import org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidateEntry;
import org.opensearch.serverless.storage.scheduling.JitteredScheduling;
import org.opensearch.serverless.storage.util.SustainedCandidateTracker;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Computes and caches the cluster-wide {@link NodeCapacitySignal}, node autoscaling design doc part 1
 * ("The aggregation task") -- the in-cluster half of the node-autoscaling contract; an external
 * control plane (not part of this repository) polls {@link
 * org.opensearch.serverless.storage.nodecapacity.action.RestNodeCapacityAction} to read what this
 * service last computed and decides node-count changes from it.
 *
 * <p>Runs only on the elected cluster-manager node, checked fresh every tick -- the same reasoning
 * {@link ScaleToZeroCandidatesSchedulerTask} already documents: the routing table this service reads
 * is already fully known on the cluster-manager, so running this anywhere else would just duplicate
 * work with no additional information.
 *
 * <p>Reuses {@link ScaleToZeroCandidatesSchedulerTask#latestCandidates()} for per-shard idleness
 * rather than forking a second definition of "idle" -- two independent idle definitions would drift.
 * A node's {@code allShardsIdle} is deliberately not an average: a node holding one hot shard among
 * several idle ones must never be flagged as a drain candidate.
 */
public final class NodeCapacitySignalService implements Closeable {

    private static final Logger logger = LogManager.getLogger(NodeCapacitySignalService.class);

    private final ClusterService clusterService;
    private final ScaleToZeroCandidatesSchedulerTask scaleToZeroTask;
    private final long readerCacheAffinityTtlMillis;
    private final DrainCoordinator drainCoordinator;
    private final NodeWarmupCoordinator warmupCoordinator;
    private final Scheduler.Cancellable task;
    private final SustainedCandidateTracker<String> writerDrainTracker;
    private final SustainedCandidateTracker<String> readerDrainTracker;
    private final AtomicInteger writerSustainedPressureTicks = new AtomicInteger();
    private final AtomicInteger readerSustainedPressureTicks = new AtomicInteger();
    private final AtomicReference<NodeCapacitySignal> latestSignal = new AtomicReference<>(NodeCapacitySignal.empty());

    /** This service's own tick interval, needed to decide when a scale-to-zero snapshot is too old to use (finding N-4). */
    private final TimeValue interval;

    /**
     * The {@code evaluationSequence} of the scale-to-zero snapshot the drain streak was last
     * advanced against. The streak is only allowed to advance when this changes (finding N-4).
     */
    private long lastIdlenessSequence = -1L;

    /**
     * The drain-candidate lists the previous tick produced, replayed unchanged on a tick whose
     * idleness snapshot has not advanced -- see {@code RoleAccumulator#toSignal} (finding N-4).
     * {@code SustainedCandidateTracker} lives in a package this change does not own, so the "do not
     * re-count the same observation" rule is enforced here, at the only call site that has the
     * freshness information to enforce it with.
     */
    private volatile List<String> lastWriterDrainCandidates = List.of();
    private volatile List<String> lastReaderDrainCandidates = List.of();

    /**
     * In-flight guards for the two hygiene sweeps. Without them the sweeps re-dispatched an identical
     * cluster-state task on every tick while a previous one was still queued behind a busy
     * cluster-manager, each of which triggers a reroute -- a self-sustaining load source (finding N-10).
     */
    private final java.util.concurrent.atomic.AtomicBoolean excludeSweepInFlight = new java.util.concurrent.atomic.AtomicBoolean();
    private final java.util.concurrent.atomic.AtomicBoolean warmingSweepInFlight = new java.util.concurrent.atomic.AtomicBoolean();

    /**
     * Starts the scheduled evaluation.
     *
     * @param threadPool schedules {@link #evaluate()} on a fixed delay.
     * @param interval how often to evaluate.
     * @param clusterService used on every tick to check whether this is currently the elected
     *                       cluster-manager node, and to read the routing table/metadata.
     * @param scaleToZeroTask supplies per-shard idleness via {@link
     *                        ScaleToZeroCandidatesSchedulerTask#latestCandidates()} -- shared, not
     *                        re-derived, so idleness never disagrees between the two features.
     * @param readerCacheAffinityTtlMillis passed straight through to {@link
     *                                     ReaderCacheAffinityMetadata#isAffinityFresh}.
     * @param requiredConsecutiveDrainTicks how many consecutive fully-idle ticks a node must have
     *                                      before it appears in {@link RoleCapacitySignal#drainCandidates()}.
     */
    public NodeCapacitySignalService(
        ThreadPool threadPool,
        TimeValue interval,
        ClusterService clusterService,
        ScaleToZeroCandidatesSchedulerTask scaleToZeroTask,
        long readerCacheAffinityTtlMillis,
        int requiredConsecutiveDrainTicks
    ) {
        this.clusterService = clusterService;
        this.scaleToZeroTask = scaleToZeroTask;
        this.interval = interval;
        this.readerCacheAffinityTtlMillis = readerCacheAffinityTtlMillis;
        this.drainCoordinator = new DrainCoordinator(clusterService);
        this.warmupCoordinator = new NodeWarmupCoordinator(clusterService);
        this.writerDrainTracker = new SustainedCandidateTracker<>(requiredConsecutiveDrainTicks);
        this.readerDrainTracker = new SustainedCandidateTracker<>(requiredConsecutiveDrainTicks);
        // Jittered rather than started on the exact configured interval (finding L-10). Every node
        // constructs this task at roughly the same moment after a cluster restart or a rolling
        // upgrade, and scheduleWithFixedDelay never recomputes the delay, so an un-jittered start
        // leaves every node's copy of this loop ticking in lockstep for the lifetime of the process
        // -- a synchronised burst of cluster-manager work and object-store requests every interval,
        // forever, which is exactly the recovery-stampede shape RFC section 13 asks the reconcilers
        // to avoid. JitteredScheduling only ever extends the first interval, never shortens it.
        this.task = threadPool.scheduleWithFixedDelay(this::evaluateSafely, JitteredScheduling.jitter(interval), ThreadPool.Names.GENERIC);
    }

    private void evaluateSafely() {
        try {
            evaluate();
        } catch (Throwable t) {
            // Deliberately Throwable: see ScaleToZeroCandidatesSchedulerTask#evaluateSafely for why
            // ClusterService#state() can throw an AssertionError during a node's early startup window,
            // and why a scheduled task must never let one bad tick kill its own recurring schedule.
            logger.warn("node capacity signal evaluation failed, will retry next tick", t);
        }
    }

    void evaluate() {
        ClusterState state = clusterService.state();
        if (state.nodes().isLocalNodeElectedClusterManager() == false) {
            return; // not our turn -- see class javadoc for why only the cluster-manager runs this
        }

        long now = System.currentTimeMillis();

        // Finding N-4. The scale-to-zero candidate list is a cached snapshot that is replaced only on
        // a successful fan-out and never invalidated on failure, and this service ticks on its own,
        // unrelated interval -- nothing validates the two against each other. With node capacity at
        // 10s and scale-to-zero at 5m, the drain tracker used to increment its streak 30 times
        // against the *same unchanged snapshot*, so "sustained idleness for 10 consecutive ticks"
        // was certified by a single observation: a shard idle for one 5-minute sample got its node
        // published as a drain candidate 100 seconds later, and the control plane terminated a node
        // whose shard had resumed traffic four minutes earlier.
        //
        // Two guards, because they fail differently. Age: a snapshot older than a generous multiple
        // of this service's interval is not used at all, so a failing fan-out stops silently
        // certifying idleness rather than certifying it forever. Identity: the drain streak only
        // advances when the snapshot's evaluation sequence has actually moved, so a fast consumer
        // cannot manufacture consecutive observations out of one.
        long snapshotAtMillis = scaleToZeroTask.latestCandidatesAtMillis();
        long snapshotSequence = scaleToZeroTask.evaluationSequence();
        boolean idlenessUsable = snapshotAtMillis > 0 && now - snapshotAtMillis <= maxIdlenessAgeMillis();
        Map<String, ScaleToZeroCandidateEntry> idleByShard = new HashMap<>();
        if (idlenessUsable) {
            for (ScaleToZeroCandidateEntry entry : scaleToZeroTask.latestCandidates()) {
                idleByShard.put(entry.indexUuid() + "/" + entry.shardId(), entry);
            }
        } else if (snapshotAtMillis > 0) {
            logger.warn(
                "scale-to-zero idleness snapshot is {} ms old (limit {} ms) -- reporting no shard as idle this tick rather "
                    + "than treating a stale verdict as current",
                now - snapshotAtMillis,
                maxIdlenessAgeMillis()
            );
        }
        boolean snapshotAdvanced = idlenessUsable && snapshotSequence != lastIdlenessSequence;
        if (snapshotAdvanced) {
            lastIdlenessSequence = snapshotSequence;
        }

        Set<String> excludedNames = DrainCoordinator.currentlyExcludedNames(state);
        Set<String> warmingNames = NodeWarmupCoordinator.currentlyWarmingNames(state);
        RoleAccumulator writer = new RoleAccumulator(excludedNames, snapshotAdvanced);
        RoleAccumulator reader = new RoleAccumulator(excludedNames, snapshotAdvanced);
        // Finding N-6: seed both pools from cluster membership so a node holding no shard of this
        // role is still visible -- as available headroom, and as the ideal drain target it is.
        seedNodesFromMembership(state, writer, reader);
        // Finding N-3: a reader shard cannot allocate anywhere while every reader-role node is either
        // warming or excluded, no matter how many nodes are added. Computed once, not per shard.
        boolean readerPoolFullyUnavailable = everyReaderNodeUnavailable(state, excludedNames, warmingNames);

        for (IndexRoutingTable indexRoutingTable : state.routingTable()) {
            IndexMetadata indexMetadata = state.metadata().index(indexRoutingTable.getIndex());
            if (indexMetadata == null
                || ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.get(indexMetadata.getSettings()) == false) {
                continue;
            }
            String indexUuid = indexMetadata.getIndexUUID();
            String indexName = indexMetadata.getIndex().getName();
            for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                int shardId = shardRoutingTable.shardId().id();
                for (ShardRouting shardRouting : shardRoutingTable) {
                    boolean isReader = shardRouting.isSearchOnly();
                    RoleAccumulator acc = isReader ? reader : writer;
                    if (shardRouting.unassigned() || shardRouting.currentNodeId() == null) {
                        if (isBlockedRatherThanStarvedOfCapacity(shardRouting, indexMetadata, isReader, readerPoolFullyUnavailable)) {
                            // Finding N-3: counted, reported, but deliberately not fed to the
                            // scale-up trigger or the saturation gate -- adding a node would not
                            // place this shard, so treating it as demand provisions capacity forever.
                            acc.blockedUnassignedShardCount++;
                        } else {
                            acc.unassignedShardCount++;
                            acc.unassignedByIndex.merge(indexName, 1, Integer::sum);
                        }
                        continue;
                    }
                    String nodeId = shardRouting.currentNodeId();
                    DiscoveryNode node = state.nodes().get(nodeId);
                    String nodeName = node == null ? nodeId : node.getName();
                    NodeAccumulator nodeAcc = acc.nodes.computeIfAbsent(nodeId, id -> new NodeAccumulator(id, nodeName));
                    nodeAcc.assignedShardCount++;
                    ScaleToZeroCandidateEntry idleEntry = idleByShard.get(indexUuid + "/" + shardId);
                    boolean idle = isReader
                        ? (idleEntry != null && idleEntry.readerCandidate())
                        : (idleEntry != null && idleEntry.candidate());
                    if (idle) {
                        nodeAcc.idleShardCount++;
                    } else {
                        nodeAcc.hasNonIdleShard = true;
                    }
                    if (isReader
                        && ReaderCacheAffinityMetadata.isAffinityFresh(indexMetadata, shardId, now, readerCacheAffinityTtlMillis)
                        && nodeId.equals(ReaderCacheAffinityMetadata.preferredNodeId(indexMetadata, shardId))) {
                        nodeAcc.hotAffinityShardCount++;
                    }
                }
            }
        }

        RoleCapacitySignal writerSignal = writer.toSignal(
            writerDrainTracker,
            writerSustainedPressureTicks.updateAndGet(t -> writer.unassignedShardCount > 0 ? t + 1 : 0),
            lastWriterDrainCandidates
        );
        RoleCapacitySignal readerSignal = reader.toSignal(
            readerDrainTracker,
            readerSustainedPressureTicks.updateAndGet(t -> reader.unassignedShardCount > 0 ? t + 1 : 0),
            lastReaderDrainCandidates
        );
        lastWriterDrainCandidates = writerSignal.drainCandidates();
        lastReaderDrainCandidates = readerSignal.drainCandidates();
        latestSignal.set(new NodeCapacitySignal(writerSignal, readerSignal));
        logger.debug(
            "node capacity signal: writer[{} nodes, {} unassigned] reader[{} nodes, {} unassigned]",
            writerSignal.nodeCount(),
            writerSignal.unassignedShardCount(),
            readerSignal.nodeCount(),
            readerSignal.unassignedShardCount()
        );

        sweepStaleExcludeNames(state, excludedNames);
        sweepStaleWarmingNames(state);
    }

    /**
     * How old a scale-to-zero idleness snapshot may be before this service refuses to use it. Four
     * of this service's own ticks: long enough that an ordinary scale-to-zero interval longer than
     * this one is still honoured (the snapshot is simply reused until it is replaced), short enough
     * that a fan-out which has genuinely stopped working is noticed rather than trusted forever.
     * Floored at a minute so a very short capacity interval does not make every snapshot look stale.
     */
    private long maxIdlenessAgeMillis() {
        return Math.max(60_000L, interval.millis() * 4);
    }

    /**
     * Adds a zero-shard entry for every node in each role's pool, so the accumulators below start
     * from cluster membership rather than from whatever happens to hold a shard (finding N-6).
     *
     * <p>Role membership is read from the same node attribute {@code
     * ReaderShardPlacementAllocationDecider} allocates on: a node carrying {@code
     * serverless_storage_reader=true} is in the reader pool, and any other data node is in the
     * writer pool. Cluster-manager-only and other non-data nodes are in neither, since no shard of
     * either role can ever land on them and counting them as headroom would overstate capacity.
     */
    private static void seedNodesFromMembership(ClusterState state, RoleAccumulator writer, RoleAccumulator reader) {
        for (DiscoveryNode node : state.nodes()) {
            if (node.isDataNode() == false) {
                continue;
            }
            boolean isReaderNode = "true".equals(
                node.getAttributes()
                    .get(org.opensearch.serverless.storage.allocation.ReaderShardPlacementAllocationDecider.READER_NODE_ATTRIBUTE)
            );
            RoleAccumulator acc = isReaderNode ? reader : writer;
            acc.nodes.computeIfAbsent(node.getId(), id -> new NodeAccumulator(id, node.getName()));
        }
    }

    /**
     * Whether every reader-role node in the cluster is currently unable to accept a reader shard --
     * because it is warming ({@code NodeWarmupAllocationDecider} answers {@code NO}) or because it is
     * drain-excluded. In that state an unassigned reader shard is blocked, not starved of capacity,
     * and this is exactly the condition that used to latch reader scale-up off cluster-wide from a
     * single misconfigured node (finding N-3, composing with N-1).
     */
    private static boolean everyReaderNodeUnavailable(ClusterState state, Set<String> excludedNames, Set<String> warmingNames) {
        boolean sawReaderNode = false;
        for (DiscoveryNode node : state.nodes()) {
            if (node.isDataNode() == false
                || "true".equals(
                    node.getAttributes()
                        .get(org.opensearch.serverless.storage.allocation.ReaderShardPlacementAllocationDecider.READER_NODE_ATTRIBUTE)
                ) == false) {
                continue;
            }
            sawReaderNode = true;
            if (warmingNames.contains(node.getName()) == false && excludedNames.contains(node.getName()) == false) {
                return false;
            }
        }
        // No reader node at all is genuinely "needs capacity", not "blocked": adding one would fix it.
        return sawReaderNode;
    }

    /**
     * Whether an unassigned shard copy is unassigned for a reason that adding a node would not fix
     * (finding N-3). Three such reasons, all cheap to test from the routing entry and metadata:
     *
     * <ul>
     *   <li>scale-to-zero deliberately holds a suspended shard {@code UNASSIGNED} -- that is the
     *       feature working, not a capacity shortfall;</li>
     *   <li>a copy that has exhausted {@code index.allocation.max_retries} will not be retried by any
     *       reroute until an operator resets its failures;</li>
     *   <li>a reader copy while every reader-role node is warming or excluded.</li>
     * </ul>
     */
    private static boolean isBlockedRatherThanStarvedOfCapacity(
        ShardRouting shardRouting,
        IndexMetadata indexMetadata,
        boolean isReader,
        boolean everyReaderNodeUnavailable
    ) {
        int shardId = shardRouting.shardId().id();
        boolean suspended = isReader
            ? org.opensearch.serverless.storage.allocation.SuspendedShardsMetadata.isReaderSuspended(indexMetadata, shardId)
            : org.opensearch.serverless.storage.allocation.SuspendedShardsMetadata.isSuspended(indexMetadata, shardId);
        if (suspended) {
            return true;
        }
        org.opensearch.cluster.routing.UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
        if (unassignedInfo != null) {
            int maxRetries = org.opensearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(
                indexMetadata.getSettings()
            );
            if (unassignedInfo.getNumFailedAllocations() >= maxRetries) {
                return true;
            }
        }
        return isReader && everyReaderNodeUnavailable;
    }

    /**
     * Removes any excluded name with no matching live node from {@code cluster.routing.allocation.exclude._name}
     * -- node autoscaling design doc part 1's "exclude-list hygiene" rule: a leaked exclude entry
     * must not be able to silently poison a future node that reuses the departed node's name.
     */
    private void sweepStaleExcludeNames(ClusterState state, Set<String> excludedNames) {
        if (excludedNames.isEmpty()) {
            return;
        }
        Set<String> liveNames = new HashSet<>();
        for (DiscoveryNode node : state.nodes()) {
            liveNames.add(node.getName());
        }
        Set<String> stale = new HashSet<>(excludedNames);
        stale.removeAll(liveNames);
        if (stale.isEmpty()) {
            return;
        }
        // Finding N-10: without this guard the sweep re-dispatched an identical cluster-state task
        // on every tick while the previous one was still queued behind a busy cluster-manager, and
        // each of those triggers a reroute -- so a slow cluster manager made itself slower.
        if (excludeSweepInFlight.compareAndSet(false, true) == false) {
            return;
        }
        drainCoordinator.removeStaleNames(stale, org.opensearch.core.action.ActionListener.wrap(response -> {
            excludeSweepInFlight.set(false);
            logger.debug("removed stale drain exclude entries: {}", stale);
        }, e -> {
            excludeSweepInFlight.set(false);
            logger.warn("failed to remove stale drain exclude entries {}, will retry next tick: {}", stale, e);
        }));
    }

    /**
     * Removes any warming name with no matching live node from {@link
     * NodeWarmupCoordinator#WARMING_NAMES_SETTING_KEY} -- same hygiene rule as {@link
     * #sweepStaleExcludeNames}, since a leaked warming mark blocks reader allocation to a future node
     * that reuses the departed node's name just as permanently as a leaked exclude entry would.
     */
    private void sweepStaleWarmingNames(ClusterState state) {
        Set<String> warmingNames = NodeWarmupCoordinator.currentlyWarmingNames(state);
        if (warmingNames.isEmpty()) {
            return;
        }
        Set<String> liveNames = new HashSet<>();
        for (DiscoveryNode node : state.nodes()) {
            liveNames.add(node.getName());
        }
        Set<String> stale = new HashSet<>(warmingNames);
        stale.removeAll(liveNames);
        if (stale.isEmpty()) {
            return;
        }
        if (warmingSweepInFlight.compareAndSet(false, true) == false) {
            return; // see sweepStaleExcludeNames for why (finding N-10).
        }
        warmupCoordinator.removeStaleNames(stale, org.opensearch.core.action.ActionListener.wrap(response -> {
            warmingSweepInFlight.set(false);
            logger.debug("removed stale warming entries: {}", stale);
        }, e -> {
            warmingSweepInFlight.set(false);
            logger.warn("failed to remove stale warming entries {}, will retry next tick: {}", stale, e);
        }));
    }

    /** The most recently computed signal, or an empty-but-valid signal if no evaluation has completed yet. */
    public NodeCapacitySignal latestSignal() {
        return latestSignal.get();
    }

    /** Invokes {@link #evaluate()} synchronously, rather than waiting out the scheduled interval -- test-only visibility. */
    void evaluateForTesting() {
        evaluate();
    }

    /** Cancels the scheduled evaluation; does not touch {@link #latestSignal()}, which simply stops updating. */
    @Override
    public void close() {
        task.cancel();
    }

    /** Whether the scheduled evaluation has been cancelled (by {@link #close()}) -- test-only. */
    public boolean isCancelledForTesting() {
        return task.isCancelled();
    }

    private static final class RoleAccumulator {
        final Map<String, NodeAccumulator> nodes = new LinkedHashMap<>();
        final Map<String, Integer> unassignedByIndex = new LinkedHashMap<>();
        final Set<String> excludedNames;
        /** Whether the idleness snapshot behind this tick is a genuinely new observation (finding N-4). */
        final boolean idlenessSnapshotAdvanced;
        int unassignedShardCount;
        int blockedUnassignedShardCount;

        RoleAccumulator(Set<String> excludedNames, boolean idlenessSnapshotAdvanced) {
            this.excludedNames = excludedNames;
            this.idlenessSnapshotAdvanced = idlenessSnapshotAdvanced;
        }

        RoleCapacitySignal toSignal(
            SustainedCandidateTracker<String> drainTracker,
            int sustainedPressureTicks,
            List<String> previousDrainCandidates
        ) {
            List<NodeCapacityEntry> entries = new ArrayList<>(nodes.size());
            List<String> nodeIds = new ArrayList<>(nodes.keySet());
            for (String nodeId : nodeIds) {
                NodeAccumulator acc = nodes.get(nodeId);
                boolean allIdle = acc.assignedShardCount > 0 && acc.hasNonIdleShard == false;
                boolean draining = excludedNames.contains(acc.nodeName);
                entries.add(
                    new NodeCapacityEntry(
                        nodeId,
                        acc.nodeName,
                        acc.assignedShardCount,
                        allIdle,
                        acc.idleShardCount,
                        acc.hotAffinityShardCount,
                        draining
                    )
                );
            }
            // Finding N-4: only let the streak advance on a genuinely new idleness observation.
            // On a repeat of the same snapshot the previously computed candidate list is reused
            // unchanged, so hysteresis measures distinct measurements rather than elapsed ticks.
            if (idlenessSnapshotAdvanced == false) {
                return new RoleCapacitySignal(
                    entries,
                    unassignedShardCount,
                    blockedUnassignedShardCount,
                    previousDrainCandidates,
                    sustainedPressureTicks,
                    unassignedByIndex
                );
            }
            List<String> drainCandidates = drainTracker.filterSustained(
                nodeIds,
                id -> id,
                id -> nodes.get(id).assignedShardCount > 0 && nodes.get(id).hasNonIdleShard == false
            );
            return new RoleCapacitySignal(
                entries,
                unassignedShardCount,
                blockedUnassignedShardCount,
                drainCandidates,
                sustainedPressureTicks,
                unassignedByIndex
            );
        }
    }

    private static final class NodeAccumulator {
        final String nodeId;
        final String nodeName;
        int assignedShardCount;
        int idleShardCount;
        int hotAffinityShardCount;
        boolean hasNonIdleShard;

        NodeAccumulator(String nodeId, String nodeName) {
            this.nodeId = nodeId;
            this.nodeName = nodeName;
        }
    }
}
