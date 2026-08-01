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
        this.readerCacheAffinityTtlMillis = readerCacheAffinityTtlMillis;
        this.drainCoordinator = new DrainCoordinator(clusterService);
        this.warmupCoordinator = new NodeWarmupCoordinator(clusterService);
        this.writerDrainTracker = new SustainedCandidateTracker<>(requiredConsecutiveDrainTicks);
        this.readerDrainTracker = new SustainedCandidateTracker<>(requiredConsecutiveDrainTicks);
        this.task = threadPool.scheduleWithFixedDelay(this::evaluateSafely, interval, ThreadPool.Names.GENERIC);
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

        Map<String, ScaleToZeroCandidateEntry> idleByShard = new HashMap<>();
        for (ScaleToZeroCandidateEntry entry : scaleToZeroTask.latestCandidates()) {
            idleByShard.put(entry.indexUuid() + "/" + entry.shardId(), entry);
        }

        Set<String> excludedNames = DrainCoordinator.currentlyExcludedNames(state);
        RoleAccumulator writer = new RoleAccumulator(excludedNames);
        RoleAccumulator reader = new RoleAccumulator(excludedNames);
        long now = System.currentTimeMillis();

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
                        acc.unassignedShardCount++;
                        acc.unassignedByIndex.merge(indexName, 1, Integer::sum);
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
            writerSustainedPressureTicks.updateAndGet(t -> writer.unassignedShardCount > 0 ? t + 1 : 0)
        );
        RoleCapacitySignal readerSignal = reader.toSignal(
            readerDrainTracker,
            readerSustainedPressureTicks.updateAndGet(t -> reader.unassignedShardCount > 0 ? t + 1 : 0)
        );
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
        drainCoordinator.removeStaleNames(
            stale,
            org.opensearch.core.action.ActionListener.wrap(
                response -> logger.debug("removed stale drain exclude entries: {}", stale),
                e -> logger.warn("failed to remove stale drain exclude entries {}, will retry next tick", stale, e)
            )
        );
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
        warmupCoordinator.removeStaleNames(
            stale,
            org.opensearch.core.action.ActionListener.wrap(
                response -> logger.debug("removed stale warming entries: {}", stale),
                e -> logger.warn("failed to remove stale warming entries {}, will retry next tick", stale, e)
            )
        );
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
        int unassignedShardCount;

        RoleAccumulator(Set<String> excludedNames) {
            this.excludedNames = excludedNames;
        }

        RoleCapacitySignal toSignal(SustainedCandidateTracker<String> drainTracker, int sustainedPressureTicks) {
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
            List<String> drainCandidates = drainTracker.filterSustained(
                nodeIds,
                id -> id,
                id -> nodes.get(id).assignedShardCount > 0 && nodes.get(id).hasNonIdleShard == false
            );
            return new RoleCapacitySignal(entries, unassignedShardCount, drainCandidates, sustainedPressureTicks, unassignedByIndex);
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
