/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.action.index.NodeMappingRefreshAction;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.metadata.GatedIndexRelease;
import org.opensearch.cluster.metadata.IndexCreationStrategyRegistry;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ComputedShardRouting;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RecoverySource.Type;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.ShardLockObtainFailedException;
import org.opensearch.gateway.GatewayService;
import org.opensearch.index.IndexComponent;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.MergedSegmentWarmerFactory;
import org.opensearch.index.remote.RemoteStoreStatsTrackerFactory;
import org.opensearch.index.seqno.GlobalCheckpointSyncAction;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLeaseSyncer;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardRelocatedException;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.shard.PrimaryReplicaSyncer;
import org.opensearch.index.shard.PrimaryReplicaSyncer.ResyncTask;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.PeerRecoverySourceService;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.indices.recovery.RecoveryListener;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.SegmentReplicationSourceService;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.indices.replication.checkpoint.MergedSegmentPublisher;
import org.opensearch.indices.replication.checkpoint.ReferencedSegmentsPublisher;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.opensearch.indices.replication.common.ReplicationState;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.search.SearchService;
import org.opensearch.snapshots.SnapshotShardsService;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.CLOSED;
import static org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED;
import static org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.FAILURE;
import static org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.NO_LONGER_ASSIGNED;
import static org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.REOPENED;

/**
 * Service to update the cluster state for multiple indices
 *
 * @opensearch.internal
 */
public class IndicesClusterStateService extends AbstractLifecycleComponent implements ClusterStateApplier {
    private static final Logger logger = LogManager.getLogger(IndicesClusterStateService.class);

    final AllocatedIndices<? extends Shard, ? extends AllocatedIndex<? extends Shard>> indicesService;
    /**
     * The residency bookkeeping for indices this node opened on demand -- what it holds, the ceiling, the
     * sweeps, and the shared close path. Extracted to {@link GatedIndexResidency}; this class constructs
     * it, drives its sweeps from the timer in {@link #doStart}, and consults it from the appliers and the
     * on-demand opener, but owns none of its state.
     */
    private final GatedIndexResidency gatedResidency;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final PeerRecoveryTargetService recoveryTargetService;
    private final ShardStateAction shardStateAction;
    private final NodeMappingRefreshAction nodeMappingRefreshAction;

    private static final ActionListener<Void> SHARD_STATE_ACTION_LISTENER = ActionListener.wrap(() -> {});

    private final Settings settings;
    // a list of shards that failed during recovery
    // we keep track of these shards in order to prevent repeated recovery of these shards on each cluster state update
    final ConcurrentMap<ShardId, ShardRouting> failedShardsCache = ConcurrentCollections.newConcurrentMap();
    private final RepositoriesService repositoriesService;

    private final FailedShardHandler failedShardHandler = new FailedShardHandler();

    private final boolean sendRefreshMapping;
    private final List<IndexEventListener> builtInIndexListener;
    private final PrimaryReplicaSyncer primaryReplicaSyncer;
    private final Consumer<ShardId> globalCheckpointSyncer;
    private final RetentionLeaseSyncer retentionLeaseSyncer;

    private final SegmentReplicationTargetService segmentReplicationTargetService;

    private final SegmentReplicationCheckpointPublisher checkpointPublisher;

    private final RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory;

    private final MergedSegmentWarmerFactory mergedSegmentWarmerFactory;

    private final MergedSegmentPublisher mergedSegmentPublisher;
    private final ReferencedSegmentsPublisher referencedSegmentsPublisher;

    @Inject
    public IndicesClusterStateService(
        final Settings settings,
        final IndicesService indicesService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final PeerRecoveryTargetService recoveryTargetService,
        final SegmentReplicationTargetService segmentReplicationTargetService,
        final SegmentReplicationSourceService segmentReplicationSourceService,
        final ShardStateAction shardStateAction,
        final NodeMappingRefreshAction nodeMappingRefreshAction,
        final RepositoriesService repositoriesService,
        final SearchService searchService,
        final PeerRecoverySourceService peerRecoverySourceService,
        final SnapshotShardsService snapshotShardsService,
        final PrimaryReplicaSyncer primaryReplicaSyncer,
        final GlobalCheckpointSyncAction globalCheckpointSyncAction,
        final RetentionLeaseSyncer retentionLeaseSyncer,
        final SegmentReplicationCheckpointPublisher checkpointPublisher,
        final RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory,
        final MergedSegmentWarmerFactory mergedSegmentWarmerFactory,
        final MergedSegmentPublisher mergedSegmentPublisher,
        final ReferencedSegmentsPublisher referencedSegmentsPublisher
    ) {
        this(
            settings,
            indicesService,
            clusterService,
            threadPool,
            checkpointPublisher,
            segmentReplicationTargetService,
            segmentReplicationSourceService,
            recoveryTargetService,
            shardStateAction,
            nodeMappingRefreshAction,
            repositoriesService,
            searchService,
            peerRecoverySourceService,
            snapshotShardsService,
            primaryReplicaSyncer,
            globalCheckpointSyncAction::updateGlobalCheckpointForShard,
            retentionLeaseSyncer,
            remoteStoreStatsTrackerFactory,
            mergedSegmentWarmerFactory,
            mergedSegmentPublisher,
            referencedSegmentsPublisher
        );
    }

    // for tests
    IndicesClusterStateService(
        final Settings settings,
        final AllocatedIndices<? extends Shard, ? extends AllocatedIndex<? extends Shard>> indicesService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final SegmentReplicationCheckpointPublisher checkpointPublisher,
        final SegmentReplicationTargetService segmentReplicationTargetService,
        final SegmentReplicationSourceService segmentReplicationSourceService,
        final PeerRecoveryTargetService recoveryTargetService,
        final ShardStateAction shardStateAction,
        final NodeMappingRefreshAction nodeMappingRefreshAction,
        final RepositoriesService repositoriesService,
        final SearchService searchService,
        final PeerRecoverySourceService peerRecoverySourceService,
        final SnapshotShardsService snapshotShardsService,
        final PrimaryReplicaSyncer primaryReplicaSyncer,
        final Consumer<ShardId> globalCheckpointSyncer,
        final RetentionLeaseSyncer retentionLeaseSyncer,
        final RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory,
        final MergedSegmentWarmerFactory mergedSegmentWarmerFactory,
        final MergedSegmentPublisher mergedSegmentPublisher,
        final ReferencedSegmentsPublisher referencedSegmentsPublisher
    ) {
        this.settings = settings;
        this.checkpointPublisher = checkpointPublisher;

        final List<IndexEventListener> indexEventListeners = new ArrayList<>(
            Arrays.asList(peerRecoverySourceService, recoveryTargetService, searchService, snapshotShardsService)
        );
        indexEventListeners.add(segmentReplicationTargetService);
        indexEventListeners.add(segmentReplicationSourceService);
        indexEventListeners.add(remoteStoreStatsTrackerFactory);
        this.segmentReplicationTargetService = segmentReplicationTargetService;
        this.builtInIndexListener = Collections.unmodifiableList(indexEventListeners);
        this.indicesService = indicesService;
        this.gatedResidency = new GatedIndexResidency(indicesService, clusterService);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.recoveryTargetService = recoveryTargetService;
        this.shardStateAction = shardStateAction;
        this.nodeMappingRefreshAction = nodeMappingRefreshAction;
        this.repositoriesService = repositoriesService;
        this.primaryReplicaSyncer = primaryReplicaSyncer;
        this.globalCheckpointSyncer = globalCheckpointSyncer;
        this.retentionLeaseSyncer = Objects.requireNonNull(retentionLeaseSyncer);
        this.sendRefreshMapping = settings.getAsBoolean("indices.cluster.send_refresh_mapping", true);
        this.remoteStoreStatsTrackerFactory = remoteStoreStatsTrackerFactory;
        this.mergedSegmentWarmerFactory = mergedSegmentWarmerFactory;
        this.mergedSegmentPublisher = mergedSegmentPublisher;
        this.referencedSegmentsPublisher = referencedSegmentsPublisher;
    }

    /*
     * The sweep interval, idle-eviction threshold, and
     * max-open ceiling that used to be hardcoded Setting fields here -- indices.gated.deleted_shard_sweep_
     * interval, indices.gated.idle_eviction_after, and indices.gated.max_open -- are now read from
     * IndexResidencyPolicyRegistry, which answers with a registered plugin's own values or with these exact
     * same numeric defaults if nothing is registered. See IndexResidencyPolicy's own javadoc for why the
     * settings moved but the mechanism reading them did not (the mechanism now lives beside this class in
     * GatedIndexResidency, which this class constructs and drives), and for the reasoning that used to live
     * in this class's own javadoc for each one (the CPU-starvation measurement behind the max-open
     * ceiling, and the asymmetric cost of evicting too eagerly vs. not evicting at all behind the
     * thirty-minute idle default). The 150,888-bytes-per-index heap measurement behind the derived max-open
     * default is a specific plugin's own number and lives with that plugin's IndexResidencyPolicy now,
     * reached through IndexResidencyPolicy#bytesPerOpenIndex().
     */

    private volatile Scheduler.Cancellable gatedSweep;

    @Override
    protected void doStart() {
        // Doesn't make sense to manage shards on non-master and non-data nodes
        if (DiscoveryNode.isDataNode(settings) || DiscoveryNode.isClusterManagerNode(settings)) {
            clusterService.addHighPriorityApplier(this);
            indicesService.setOnDemandShardOpener(this::openComputedShardsOnDemand);
            // The push half of the same lifecycle. The sweep below re-derives which gated indices are gone
            // on a timer; this lets whatever already knows say so at once. Both close a shard the same way.
            GatedIndexRelease.register(index -> gatedResidency.releaseGatedIndex(index, "gated index was deleted"));
            gatedResidency.start();
            TimeValue interval = IndexResidencyPolicyRegistry.sweepInterval();
            TimeValue idleAfter = IndexResidencyPolicyRegistry.idleEvictionAfter();
            if (interval.millis() > 0) {
                // Both halves of the on-demand lifecycle on one timer rather than two. They walk the same
                // set, they close through the same method, and running them apart would mean two schedules
                // to reason about for one question -- which of the indices this node opened should it still
                // be holding.
                gatedSweep = threadPool.scheduleWithFixedDelay(() -> {
                    gatedResidency.sweepDeletedGatedIndices();
                    if (idleAfter.millis() > 0) {
                        gatedResidency.evictIdleGatedIndices(idleAfter);
                    }
                }, interval, ThreadPool.Names.MANAGEMENT);
            }
        }
    }

    @Override
    protected void doStop() {
        if (DiscoveryNode.isDataNode(settings) || DiscoveryNode.isClusterManagerNode(settings)) {
            if (gatedSweep != null) {
                gatedSweep.cancel();
                gatedSweep = null;
            }
            GatedIndexRelease.register(null);
            indicesService.setOnDemandShardOpener(null);
            clusterService.removeApplier(this);
        }
    }

    @Override
    protected void doClose() {}

    @Override
    public synchronized void applyClusterState(final ClusterChangedEvent event) {
        if (!lifecycle.started()) {
            return;
        }

        final ClusterState state = event.state();

        // we need to clean the shards and indices we have on this node, since we
        // are going to recover them again once state persistence is disabled (no cluster-manager / not recovered)
        // TODO: feels hacky, a block disables state persistence, and then we clean the allocated shards, maybe another flag in blocks?
        if (state.blocks().disableStatePersistence()) {
            for (AllocatedIndex<? extends Shard> indexService : indicesService) {
                // also cleans shards
                indicesService.removeIndex(indexService.index(), NO_LONGER_ASSIGNED, "cleaning index (disabled block persistence)");
            }
            return;
        }

        // Derived once and threaded through every step below, rather than re-derived by each of them.
        //
        // RoutingNodes#localRoutingNode is a full scan of every shard of every index in the cluster -- that
        // is the deliberate trade it makes (see its own javadoc): it allocates only for this node's shards
        // instead of retaining the whole cluster's inverse index on the ClusterState, and pays a scan for
        // it. Six of the steps below wanted the same value for the same state, so the scan ran six times
        // per applied state where the structure it replaced was built once. Hoisting keeps the whole memory
        // win and cuts the CPU back to one scan: at 500,000 shards that is five avoided full walks of the
        // routing table on the cluster applier thread, per applied state.
        //
        // Safe because every one of them derived it from exactly this state and this node id: event.state()
        // is `state`, and state.nodes().getLocalNodeId() does not change within one application.
        final RoutingNode localRoutingNode = RoutingNodes.localRoutingNode(state, state.nodes().getLocalNodeId());

        updateFailedShardsCache(state, localRoutingNode);

        deleteIndices(event); // also deletes shards of deleted indices

        removeIndices(event, localRoutingNode); // also removes shards of removed indices

        failMissingShards(state, localRoutingNode);

        removeShards(state, localRoutingNode);   // removes any local shards that doesn't match what the cluster-manager expects

        updateIndices(event, localRoutingNode); // can also fail shards, but these are then guaranteed to be in failedShardsCache

        createIndices(state, localRoutingNode);

        createOrUpdateShards(state, localRoutingNode);
    }

    /**
     * Removes shard entries from the failed shards cache that are no longer allocated to this node by the master.
     * Sends shard failures for shards that are marked as actively allocated to this node but don't actually exist on the node.
     * Resends shard failures for shards that are still marked as allocated to this node but previously failed.
     *
     * @param state new cluster state
     * @param localRoutingNode this node's shards in {@code state}, computed once by the caller
     */
    private void updateFailedShardsCache(final ClusterState state, @Nullable final RoutingNode localRoutingNode) {
        if (localRoutingNode == null) {
            failedShardsCache.clear();
            return;
        }

        DiscoveryNode clusterManagerNode = state.nodes().getClusterManagerNode();

        // remove items from cache which are not in our routing table anymore and
        // resend failures that have not executed on cluster-manager yet
        for (Iterator<Map.Entry<ShardId, ShardRouting>> iterator = failedShardsCache.entrySet().iterator(); iterator.hasNext();) {
            ShardRouting failedShardRouting = iterator.next().getValue();
            ShardRouting matchedRouting = localRoutingNode.getByShardId(failedShardRouting.shardId());
            if (matchedRouting == null || matchedRouting.isSameAllocation(failedShardRouting) == false) {
                iterator.remove();
            } else {
                // TODO: can we remove this? Is resending shard failures the responsibility of shardStateAction?
                if (clusterManagerNode != null) {
                    String message = "cluster-manager "
                        + clusterManagerNode
                        + " has not removed previously failed shard. resending shard failure";
                    logger.trace("[{}] re-sending failed shard [{}], reason [{}]", matchedRouting.shardId(), matchedRouting, message);
                    shardStateAction.localShardFailed(matchedRouting, message, null, SHARD_STATE_ACTION_LISTENER, state);
                }
            }
        }
    }

    /**
     * Deletes indices (with shard data).
     *
     * @param event cluster change event
     */
    private void deleteIndices(final ClusterChangedEvent event) {
        final ClusterState previousState = event.previousState();
        final ClusterState state = event.state();
        final String localNodeId = state.nodes().getLocalNodeId();
        assert localNodeId != null;

        for (Index index : event.indicesDeleted()) {
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] cleaning index, no longer part of the metadata", index);
            }
            AllocatedIndex<? extends Shard> indexService = indicesService.indexService(index);
            final IndexSettings indexSettings;
            if (indexService != null) {
                indexSettings = indexService.getIndexSettings();
                indicesService.removeIndex(index, DELETED, "index no longer part of the metadata");
            } else if (previousState.metadata().hasIndex(index)) {
                // The deleted index was part of the previous cluster state, but not loaded on the local node
                final IndexMetadata metadata = previousState.metadata().index(index);
                indexSettings = new IndexSettings(metadata, settings);
                indicesService.deleteUnassignedIndex("deleted index was not assigned to local node", metadata, state);
            } else {
                // The previous cluster state's metadata also does not contain the index,
                // which is what happens on node startup when an index was deleted while the
                // node was not part of the cluster. In this case, try reading the index
                // metadata from disk. If its not there, there is nothing to delete.
                // First, though, verify the precondition for applying this case by
                // asserting that either this index is already in the graveyard, or the
                // previous cluster state is not initialized/recovered.
                assert state.metadata().indexGraveyard().containsIndex(index)
                    || previousState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
                final IndexMetadata metadata = indicesService.verifyIndexIsDeleted(index, event.state());
                if (metadata != null) {
                    indexSettings = new IndexSettings(metadata, settings);
                } else {
                    indexSettings = null;
                }
            }
            if (indexSettings != null) {
                threadPool.generic().execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(() -> new ParameterizedMessage("[{}] failed to complete pending deletion for index", index), e);
                    }

                    @Override
                    protected void doRun() throws Exception {
                        try {
                            // we are waiting until we can lock the index / all shards on the node and then we ack the delete of the store
                            // to the master. If we can't acquire the locks here immediately there might be a shard of this index still
                            // holding on to the lock due to a "currently canceled recovery" or so. The shard will delete itself BEFORE the
                            // lock is released so it's guaranteed to be deleted by the time we get the lock
                            indicesService.processPendingDeletes(index, indexSettings, new TimeValue(30, TimeUnit.MINUTES));
                        } catch (ShardLockObtainFailedException exc) {
                            logger.warn("[{}] failed to lock all shards for index - timed out after 30 seconds", index);
                        } catch (InterruptedException e) {
                            logger.warn("[{}] failed to lock all shards for index - interrupted", index);
                        }
                    }
                });
            }
        }
    }

    /**
     * Removes indices that have no shards allocated to this node or indices whose state has changed. This does not delete the shard data
     * as we wait for enough shard copies to exist in the cluster before deleting shard data (triggered by
     * {@link org.opensearch.indices.store.IndicesStore}).
     *
     * @param event the cluster changed event
     * @param localRoutingNode this node's shards in {@code event.state()}, computed once by the caller
     */
    private void removeIndices(final ClusterChangedEvent event, @Nullable final RoutingNode localRoutingNode) {
        final ClusterState state = event.state();
        final String localNodeId = state.nodes().getLocalNodeId();
        assert localNodeId != null;

        final Set<Index> indicesWithShards = new HashSet<>();
        if (localRoutingNode != null) { // null e.g. if we are not a data node
            for (ShardRouting shardRouting : localRoutingNode) {
                indicesWithShards.add(shardRouting.index());
            }
        }

        for (AllocatedIndex<? extends Shard> indexService : indicesService) {
            final Index index = indexService.index();
            if (gatedResidency.heldOnDemand(index)) {
                // Not in cluster state and not supposed to be. Every test below reads absence from
                // metadata or from the routing node as "the cluster manager took this away", and for an
                // index the cluster manager never knew about, absence is its ordinary condition.
                continue;
            }
            if (gatedResidency.forget(index)) {
                // Was held on demand and is not any more, which heldOnDemand has just decided. Removed
                // here rather than fallen through to the checks below, because the assertion in them
                // states that an index absent from cluster state must have been deleted or the cluster
                // must be new. Neither is true of this one: it was never in cluster state to be deleted.
                logger.debug("{} releasing index opened on demand, nothing supplies its descriptor now", index);
                indicesService.removeIndex(index, NO_LONGER_ASSIGNED, "removing index (no descriptor supplier)");
                continue;
            }
            // IndexCreationStrategyRegistry.skipsClusterState(...)
            // replaces the plugin-owned static registry's identical predicate here -- the same answer,
            // discovered through the SPI instead of the static registry, matching the same delegation
            // MetadataCreateIndexService's own two skipsClusterState/claims sites use. A pure predicate
            // swap: heldOnDemand, the eviction/idle-sweep machinery, and the openedOnDemand
            // bookkeeping (all owned by GatedIndexResidency now) are completely untouched.
            if (IndexCreationStrategyRegistry.skipsClusterState(indexService.getIndexSettings().getIndexMetadata())) {
                // Live, gated, and not in openedOnDemand -- which is a bookkeeping race rather than a state
                // worth asserting about. Both orderings of "claim and close" have one: claiming before the
                // close leaves a window where this node still holds an index nothing has claimed, and
                // claiming after it leaves a window where a request that re-opened the index in the meantime
                // has its entry erased by the release that was finishing.
                //
                // Deciding from the index's own metadata closes both, because the answer stops depending on
                // a set that two threads are editing. An index that says it skips cluster state was never in
                // cluster state, so the assertion below -- that an absent index must have been deleted or
                // the cluster must be new -- is asking the wrong question about it.
                //
                // Left open rather than closed here: the sweep and the on-demand opener between them own
                // this index's lifecycle, and the next pass reconciles it.
                continue;
            }
            if (indicesService.indexService(index) == null) {
                // Already closed by something else while this loop was walking its snapshot. Iterating
                // indicesService takes the index map as it was when the loop started, and a gated index can
                // be closed concurrently by the idle sweep or the ceiling eviction, neither of which holds
                // this instance's monitor -- deliberately, because closing is slow and holding it would
                // stall cluster state application behind a request.
                //
                // The close removes the index from the map first and clears its openedOnDemand entry after,
                // so an eviction that lands mid-iteration leaves exactly this state: an entry in this
                // loop's stale snapshot, no longer held on demand, and no longer there to remove. Every
                // check below then reads that as "the cluster manager took this index away" and the
                // assertion demands it have been deleted or the cluster be new, neither of which is true of
                // one that was never published in the first place.
                //
                // The gated guard above catches most of this, but it asks IndexCreationStrategyRegistry,
                // which (through SupplierBackedIndexCreationStrategy) still ultimately asks
                // DescriptorOnlyCreation's own gate -- and that registration goes away when the node's gate
                // uninstalls, while the indices it opened are still resident. This one cannot: an index
                // that is not in the map is not this loop's to reason about, whoever closed it and whatever
                // is registered.
                continue;
            }
            final IndexMetadata indexMetadata = state.metadata().index(index);
            final IndexMetadata existingMetadata = indexService.getIndexSettings().getIndexMetadata();

            AllocatedIndices.IndexRemovalReason reason = null;
            if (indexMetadata != null && indexMetadata.getState() != existingMetadata.getState()) {
                reason = indexMetadata.getState() == IndexMetadata.State.CLOSE ? CLOSED : REOPENED;
            } else if (indicesWithShards.contains(index) == false) {
                // if the cluster change indicates a brand new cluster, we only want
                // to remove the in-memory structures for the index and not delete the
                // contents on disk because the index will later be re-imported as a
                // dangling index
                assert indexMetadata != null || event.isNewCluster() : "index "
                    + index
                    + " does not exist in the cluster state, it should either "
                    + "have been deleted or the cluster must be new";
                reason = indexMetadata != null && indexMetadata.getState() == IndexMetadata.State.CLOSE ? CLOSED : NO_LONGER_ASSIGNED;
            }

            if (reason != null) {
                logger.debug("{} removing index ({})", index, reason);
                indicesService.removeIndex(index, reason, "removing index (" + reason + ")");
            }
        }
    }

    /**
     * Notifies cluster-manager about shards that don't exist but are supposed to be active on this node.
     *
     * @param state new cluster state
     * @param localRoutingNode this node's shards in {@code state}, computed once by the caller
     */
    private void failMissingShards(final ClusterState state, @Nullable final RoutingNode localRoutingNode) {
        if (localRoutingNode == null) {
            return;
        }
        for (final ShardRouting shardRouting : localRoutingNode) {
            ShardId shardId = shardRouting.shardId();
            if (shardRouting.initializing() == false
                && failedShardsCache.containsKey(shardId) == false
                && indicesService.getShardOrNull(shardId) == null) {
                // the cluster-manager thinks we are active, but we don't have this shard at all, mark it as failed
                sendFailShard(
                    shardRouting,
                    "master marked shard as active, but shard has not been created, mark shard as failed",
                    null,
                    state
                );
            }
        }
    }

    /**
     * Removes shards that are currently loaded by indicesService but have disappeared from the routing table of the current node.
     * This method does not delete the shard data.
     *
     * @param state new cluster state
     * @param localRoutingNode this node's shards in {@code state}, computed once by the caller
     */
    private void removeShards(final ClusterState state, @Nullable final RoutingNode localRoutingNode) {
        final String localNodeId = state.nodes().getLocalNodeId();
        assert localNodeId != null;

        // remove shards based on routing nodes (no deletion of data)
        for (AllocatedIndex<? extends Shard> indexService : indicesService) {
            if (gatedResidency.heldOnDemand(indexService.index())) {
                // A shard opened from a descriptor is in no routing node, so the loop below would read it
                // as unallocated and remove it on the first cluster state applied after the write that
                // opened it. Nothing published it, so nothing can un-publish it.
                continue;
            }
            for (Shard shard : indexService) {
                ShardRouting currentRoutingEntry = shard.routingEntry();
                ShardId shardId = currentRoutingEntry.shardId();
                ShardRouting newShardRouting = localRoutingNode == null ? null : localRoutingNode.getByShardId(shardId);
                if (newShardRouting == null) {
                    // we can just remove the shard without cleaning it locally, since we will clean it in IndicesStore
                    // once all shards are allocated
                    logger.debug("{} removing shard (not allocated)", shardId);
                    indexService.removeShard(shardId.id(), "removing shard (not allocated)");
                } else if (newShardRouting.isSameAllocation(currentRoutingEntry) == false) {
                    logger.debug(
                        "{} removing shard (stale allocation id, stale {}, new {})",
                        shardId,
                        currentRoutingEntry,
                        newShardRouting
                    );
                    indexService.removeShard(shardId.id(), "removing shard (stale copy)");
                } else if (newShardRouting.initializing() && currentRoutingEntry.active()) {
                    // this can happen if the node was isolated/gc-ed, rejoins the cluster and a new shard with the same allocation id
                    // is assigned to it. Batch cluster state processing or if shard fetching completes before the node gets a new cluster
                    // state may result in a new shard being initialized while having the same allocation id as the currently started shard.
                    logger.debug("{} removing shard (not active, current {}, new {})", shardId, currentRoutingEntry, newShardRouting);
                    indexService.removeShard(shardId.id(), "removing shard (stale copy)");
                } else if (newShardRouting.primary() && currentRoutingEntry.primary() == false && newShardRouting.initializing()) {
                    assert currentRoutingEntry.initializing() : currentRoutingEntry; // see above if clause
                    // this can happen when cluster state batching batches activation of the shard, closing an index, reopening it
                    // and assigning an initializing primary to this node
                    logger.debug("{} removing shard (not active, current {}, new {})", shardId, currentRoutingEntry, newShardRouting);
                    indexService.removeShard(shardId.id(), "removing shard (stale copy)");
                }
            }
        }
    }

    /** One gated index this node currently holds shards for, opened on demand rather than by cluster state's own diff. */
    public record OnDemandOpenIndex(String indexUuid, int numberOfShards) {
    }

    /**
     * Every gated index this node currently holds shards for -- the same bounded, per-node working set
     * {@link GatedIndexResidency} already tracks for {@link #openComputedShardsOnDemand}'s own use, exposed
     * here for a second consumer with the identical reason to want it: a gated index has no cluster
     * state entry to enumerate from (that is what gating means), so anything that needs to know "which
     * gated indices are relevant to this node right now" has nowhere else to ask that is not a
     * population-proportional read of the descriptor store. A plugin's {@code ReaderShardPreWarmCoordinator}
     * is that second consumer -- see its own javadoc on why its original cluster-state enumeration never
     * saw a gated index at all.
     *
     * <p>A snapshot, not a live view: computed fresh from the residency bookkeeping on each call, safe to
     * iterate without synchronizing against concurrent opens or closes racing after it returns.
     *
     * @return each on-demand-opened index's UUID and its real, currently-open shard count. An index that
     * closed between the map read and the {@code indexService} lookup below is simply omitted, the same
     * "best-effort, not authoritative" contract every other consumer of this best-effort registry
     * already has.
     */
    public List<OnDemandOpenIndex> onDemandOpenIndices() {
        return gatedResidency.onDemandOpenIndices();
    }

    /**
     * The names of every gated index this node currently holds on-demand-opened shards for -- the
     * name-level companion to {@link #onDemandOpenIndices()}, for consumers (a plugin's descriptor
     * pre-warmer) that key their caches by index name rather than uuid.
     */
    public List<String> onDemandOpenIndexNames() {
        return gatedResidency.onDemandOpenIndexNames();
    }

    /**
     * The second trigger: open a gated index's shards here because a request arrived for one, not because
     * a cluster state said to.
     *
     * <p>Everything above this method is driven by a cluster state
     * diff, and a gated index never appears in one, so no node ever built its shard: a write that survived
     * every coordinator-side residency site arrived at a data node and died in {@code indexServiceSafe}.
     * Placement had already chosen this node and marked the primary STARTED; nothing had told the node.
     *
     * <p><b>Why here rather than in a parallel path.</b> {@code IndicesService.createShard} takes fifteen
     * collaborators -- the checkpoint publisher, the peer recovery target service, the recovery listener,
     * the repositories service, the shard failure and global checkpoint consumers, the retention lease
     * syncer, the stats tracker factory, the discovery nodes, the warmer factory -- and this class holds
     * every one of them, along with the recovery wiring and the failure handling. A second lifecycle that
     * had to agree with this one would be a worse problem than the one being solved.
     *
     * <p>The descriptor read happens outside the lock and the shard construction inside it. That split is
     * not tidiness: {@link #applyClusterState} is synchronized on this instance, so holding the lock across
     * a remote descriptor GET would stall cluster state application behind a request, which is the
     * measured deadlock shape that forced descriptor resolution off cluster-state threads everywhere.
     *
     * <p>Does nothing at all unless a descriptor supplier is installed, so an ordinary cluster reaches the
     * {@code IndexNotFoundException} it always reached, one map lookup later.
     */
    public void openComputedShardsOnDemand(Index index) {
        ClusterState state = clusterService.state();
        if (state.metadata().index(index) != null) {
            // Published, so the ordinary path owns it and is either mid-flight or has already failed it.
            return;
        }
        IndexMetadata indexMetadata = state.metadata().indexOrResolved(index);
        if (indexMetadata == null) {
            return;
        }
        // state.getIndexRoutingTable(...) replaces the static registry's
        // AbsentIndexRoutingSuppliers.supply(...) here -- same resolution (index is already confirmed
        // unpublished via state.metadata().index(index) == null above, so the redundant re-check inside
        // getIndexRoutingTable costs one extra map lookup, not a behavior change). Only this one resolution
        // call is migrated -- the surrounding on-demand
        // shard-opening lifecycle/locking is completely untouched.
        IndexRoutingTable computed = state.getIndexRoutingTable(index.getName());
        if (computed == null) {
            // No placement means no opinion about where this index lives, which is what an unconfigured
            // cluster answers. Opening a shard here on a guess is how an index ends up served from two
            // nodes that each think they hold the primary.
            return;
        }
        String localNodeId = state.nodes().getLocalNodeId();
        List<ShardRouting> mine = new ArrayList<>();
        for (IndexShardRoutingTable shardTable : computed) {
            for (ShardRouting candidate : shardTable) {
                if (localNodeId.equals(candidate.currentNodeId())) {
                    mine.add(candidate);
                }
            }
        }
        if (mine.isEmpty()) {
            return;
        }

        // Before the lock, because closing an index is slow and this instance's monitor is the one
        // applyClusterState takes -- see GatedIndexResidency#makeRoomFor, which owns the eviction and the
        // rest of that reasoning.
        gatedResidency.makeRoomFor(index);

        List<ShardId> opening = new ArrayList<>();
        synchronized (this) {
            AllocatedIndex<? extends Shard> indexService = indicesService.indexService(index);
            if (indexService == null) {
                try {
                    // writeDanglingIndices is false, and that is a correctness argument rather than a
                    // saving. Writing this index's metadata to disk makes it a dangling index, and a
                    // dangling index is imported into cluster state on the next restart, which would
                    // un-gate it exactly the way auto-creation used to.
                    indexService = indicesService.createIndex(indexMetadata, builtInIndexListener, false);
                    gatedResidency.recordOpened(index);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("[{}] failed to open gated index on demand", index), e);
                    return;
                }
            }
            for (ShardRouting placed : mine) {
                if (indexService.getShardOrNull(placed.id()) != null) {
                    continue;
                }
                if (failedShardsCache.containsKey(placed.shardId())) {
                    continue;
                }
                createShard(state.nodes(), state.routingTable(), openable(placed, indexMetadata), state);
                opening.add(placed.shardId());
            }
        }
        gatedResidency.awaitStarted(index, opening);
    }

    /**
     * The same computed shard, as the entry a data node can act on: INITIALIZING, and carrying a recovery
     * source chosen here rather than inherited.
     *
     * <p><b>The recovery source is the one thing on this path that fails silently.</b>
     * {@code ComputedRoutingTable} states {@code ExistingStoreRecoverySource} for every computed shard, and
     * it is right to: for a shard that has data, inferring the source picks "empty store" whenever
     * {@code inSyncAllocationIds} is absent, which under computed placement is always, and the index comes
     * back blank while looking healthy. That is the A5 trap, and its own comment says so.
     *
     * <p>A shard that has never been built is the case that comment does not cover. It has no store to
     * recover, so {@code ExistingStoreRecoverySource} fails it outright with "shard allocated for local
     * recovery, should exist, but doesn't". So the source is decided here from the one fact that
     * distinguishes the two and that no shared function can know: whether this node already holds data for
     * this shard. {@link #withNodeLocalRecoverySource} makes the same call in the opposite direction for
     * shards that come from placement, from the same fact, for the same reason.
     *
     * <p>What this does <em>not</em> make safe is placement moving a shard to a node that has no copy of
     * its data, which would read as "never built" here and recover empty. That hazard is held off by
     * the plugin's {@code ComputedPlacementMembership}, which keeps the
     * node list stable while a node is merely restarting. It is a real limit rather than a solved problem,
     * and it is recorded in the design notes rather than only here.
     */
    private ShardRouting openable(ShardRouting placed, IndexMetadata indexMetadata) {
        String customDataPath = IndexMetadata.INDEX_DATA_PATH_SETTING.get(indexMetadata.getSettings());
        boolean hasData = indicesService.hasExistingShardData(placed.shardId(), customDataPath);
        RecoverySource recoverySource = hasData
            ? RecoverySource.ExistingStoreRecoverySource.INSTANCE
            : RecoverySource.EmptyStoreRecoverySource.INSTANCE;
        logger.debug("{} opening gated shard on demand, recovering from {} store", placed.shardId(), hasData ? "the existing" : "an empty");
        // Rebuilt rather than mutated, because the entry placement produced is STARTED and createShard
        // takes only an initializing one. The allocation id is carried across unchanged: it is what the
        // coordinator put in the request and what AsyncPrimaryAction checks the shard against, so minting
        // a fresh one here would fail every write with "expected allocation id [x] but found [y]".
        return ShardRouting.newUnassigned(
            placed.shardId(),
            placed.primary(),
            placed.isSearchOnly(),
            recoverySource,
            new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, "opened on demand from descriptor")
        ).initialize(placed.currentNodeId(), placed.allocationId().getId(), ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    private void createIndices(final ClusterState state, @Nullable final RoutingNode localRoutingNode) {
        // we only create indices for shards that are allocated
        if (localRoutingNode == null) {
            return;
        }
        // create map of indices to create with shards to fail if index creation fails
        final Map<Index, List<ShardRouting>> indicesToCreate = new HashMap<>();
        for (ShardRouting shardRouting : localRoutingNode) {
            if (failedShardsCache.containsKey(shardRouting.shardId()) == false) {
                final Index index = shardRouting.index();
                if (indicesService.indexService(index) == null) {
                    indicesToCreate.computeIfAbsent(index, k -> new ArrayList<>()).add(shardRouting);
                }
            }
        }

        for (Map.Entry<Index, List<ShardRouting>> entry : indicesToCreate.entrySet()) {
            final Index index = entry.getKey();
            final IndexMetadata indexMetadata = state.metadata().index(index);
            logger.debug("[{}] creating index", index);

            AllocatedIndex<? extends Shard> indexService = null;
            try {
                List<IndexEventListener> updatedIndexEventListeners = new ArrayList<>(builtInIndexListener);
                if (entry.getValue().size() > 0
                    && entry.getValue().get(0).recoverySource().getType() == Type.SNAPSHOT
                    && indexMetadata.getSettings().getAsBoolean(SETTING_REMOTE_STORE_ENABLED, false)) {
                    final IndexEventListener refreshListenerAfterSnapshotRestore = new IndexEventListener() {
                        @Override
                        public void afterIndexShardStarted(IndexShard indexShard) {
                            indexShard.refresh("refresh to upload metadata to remote store");
                        }
                    };
                    updatedIndexEventListeners.add(refreshListenerAfterSnapshotRestore);
                }
                indexService = indicesService.createIndex(indexMetadata, updatedIndexEventListeners, true);
                if (indexService.updateMapping(null, indexMetadata) && sendRefreshMapping) {
                    nodeMappingRefreshAction.nodeMappingRefresh(
                        state.nodes().getClusterManagerNode(),
                        new NodeMappingRefreshAction.NodeMappingRefreshRequest(
                            indexMetadata.getIndex().getName(),
                            indexMetadata.getIndexUUID(),
                            state.nodes().getLocalNodeId()
                        )
                    );
                }
            } catch (Exception e) {
                final String failShardReason;
                if (indexService == null) {
                    failShardReason = "failed to create index";
                } else {
                    failShardReason = "failed to update mapping for index";
                    indicesService.removeIndex(index, FAILURE, "removing index (mapping update failed)");
                }
                for (ShardRouting shardRouting : entry.getValue()) {
                    sendFailShard(shardRouting, failShardReason, e, state);
                }
            }
        }
    }

    private void updateIndices(ClusterChangedEvent event, @Nullable final RoutingNode localRoutingNode) {
        if (!event.metadataChanged()) {
            return;
        }
        final ClusterState state = event.state();
        for (AllocatedIndex<? extends Shard> indexService : indicesService) {
            final Index index = indexService.index();
            if (gatedResidency.heldOnDemand(index)) {
                // The assertion below is the point of this guard: an on-demand index is absent from
                // metadata by design, and asserting it was removed by deleteIndices would fire on the
                // first metadata change after a gated write. Its settings come from the descriptor and
                // change when the descriptor does, not when cluster state does.
                continue;
            }
            final IndexMetadata currentIndexMetadata = indexService.getIndexSettings().getIndexMetadata();
            final IndexMetadata newIndexMetadata = state.metadata().index(index);
            assert newIndexMetadata != null : "index " + index + " should have been removed by deleteIndices";
            if (ClusterChangedEvent.indexMetadataChanged(currentIndexMetadata, newIndexMetadata)) {
                String reason = null;
                try {
                    reason = "metadata update failed";
                    try {
                        indexService.updateMetadata(currentIndexMetadata, newIndexMetadata);
                    } catch (Exception e) {
                        assert false : e;
                        throw e;
                    }

                    reason = "mapping update failed";
                    if (indexService.updateMapping(currentIndexMetadata, newIndexMetadata) && sendRefreshMapping) {
                        nodeMappingRefreshAction.nodeMappingRefresh(
                            state.nodes().getClusterManagerNode(),
                            new NodeMappingRefreshAction.NodeMappingRefreshRequest(
                                newIndexMetadata.getIndex().getName(),
                                newIndexMetadata.getIndexUUID(),
                                state.nodes().getLocalNodeId()
                            )
                        );
                    }
                } catch (Exception e) {
                    indicesService.removeIndex(indexService.index(), FAILURE, "removing index (" + reason + ")");

                    // fail shards that would be created or updated by createOrUpdateShards
                    if (localRoutingNode != null) {
                        for (final ShardRouting shardRouting : localRoutingNode) {
                            if (shardRouting.index().equals(index) && failedShardsCache.containsKey(shardRouting.shardId()) == false) {
                                sendFailShard(shardRouting, "failed to update index (" + reason + ")", e, state);
                            }
                        }
                    }
                }
            }
        }
    }

    private void createOrUpdateShards(final ClusterState state, @Nullable final RoutingNode localRoutingNode) {
        if (localRoutingNode == null) {
            return;
        }

        DiscoveryNodes nodes = state.nodes();
        RoutingTable routingTable = state.routingTable();

        for (ShardRouting shardRouting : localRoutingNode) {
            ShardId shardId = shardRouting.shardId();
            if (failedShardsCache.containsKey(shardId) == false) {
                AllocatedIndex<? extends Shard> indexService = indicesService.indexService(shardId.getIndex());
                assert indexService != null : "index " + shardId.getIndex() + " should have been created by createIndices";
                Shard shard = indexService.getShardOrNull(shardId.id());
                if (shard == null) {
                    assert shardRouting.initializing() : shardRouting + " should have been removed by failMissingShards";
                    createShard(nodes, routingTable, shardRouting, state);
                } else {
                    updateShard(nodes, startComputedShardLocally(state, shardRouting, shard), shard, routingTable, state);
                }
            }
        }
    }

    /**
     * Moves a recovered computed shard to STARTED without asking the cluster manager.
     *
     * <p>A shard that finishes recovery sits in POST_RECOVERY until the cluster manager marks it started.
     * For a computed shard that never happens: {@code ShardStateAction}'s started-shard executor looks
     * the shard up by allocation id in the published routing table, finds nothing, and marks the task
     * successful. So the transition has to be local, which is the same conclusion the rest of this area
     * reaches for placement. Nothing here needs agreeing, because nothing here is shared.
     *
     * <p>Only for shards of an index with no published routing entry. Where the cluster manager does own
     * the transition, it keeps owning it, and this returns the entry untouched.
     */
    private ShardRouting startComputedShardLocally(ClusterState state, ShardRouting shardRouting, Shard shard) {
        if (shardRouting.initializing() == false || state.routingTable().hasIndex(shardRouting.index())) {
            return shardRouting;
        }
        IndexShardState shardState = shard.state();
        if (shardState == IndexShardState.POST_RECOVERY || shardState == IndexShardState.STARTED) {
            return shardRouting.moveToStarted();
        }
        return shardRouting;
    }

    private void createShard(DiscoveryNodes nodes, RoutingTable routingTable, ShardRouting shardRouting, ClusterState state) {
        shardRouting = withNodeLocalRecoverySource(state, shardRouting);
        assert shardRouting.initializing() : "only allow shard creation for initializing shard but was " + shardRouting;

        DiscoveryNode sourceNode = null;
        if (shardRouting.recoverySource().getType() == Type.PEER) {
            sourceNode = findSourceNodeForPeerRecovery(logger, routingTable, nodes, shardRouting);
            if (sourceNode == null) {
                logger.trace("ignoring initializing shard {} - no source node can be found.", shardRouting.shardId());
                return;
            }
        }

        try {
            final IndexMetadata indexMetadata = state.metadata().indexOrResolved(shardRouting.index());
            if (indexMetadata == null) {
                failAndRemoveShard(shardRouting, true, "failed to create shard", new IndexNotFoundException(shardRouting.index()), state);
                return;
            }
            final long primaryTerm = indexMetadata.primaryTerm(shardRouting.id());
            logger.debug("{} creating shard with primary term [{}]", shardRouting.shardId(), primaryTerm);
            indicesService.createShard(
                shardRouting,
                checkpointPublisher,
                recoveryTargetService,
                new RecoveryListener(shardRouting, primaryTerm, this),
                repositoriesService,
                failedShardHandler,
                globalCheckpointSyncer,
                retentionLeaseSyncer,
                nodes.getLocalNode(),
                sourceNode,
                remoteStoreStatsTrackerFactory,
                nodes,
                mergedSegmentWarmerFactory,
                mergedSegmentPublisher,
                referencedSegmentsPublisher
            );
        } catch (Exception e) {
            failAndRemoveShard(shardRouting, true, "failed to create shard", e, state);
        }
    }

    private void updateShard(
        DiscoveryNodes nodes,
        ShardRouting shardRouting,
        Shard shard,
        RoutingTable routingTable,
        ClusterState clusterState
    ) {
        final ShardRouting currentRoutingEntry = shard.routingEntry();
        assert currentRoutingEntry.isSameAllocation(shardRouting)
            : "local shard has a different allocation id but wasn't cleaned by removeShards. "
                + "cluster state: "
                + shardRouting
                + " local: "
                + currentRoutingEntry;

        final long primaryTerm;
        try {
            final IndexMetadata indexMetadata = clusterState.metadata().index(shard.shardId().getIndex());
            primaryTerm = indexMetadata.primaryTerm(shard.shardId().id());
            final Set<String> inSyncIds = computedAwareInSyncIds(clusterState, shardRouting, indexMetadata);
            // Resolved rather than read. shardRoutingTable throws IndexNotFoundException for an index
            // with no published entry, and the catch below fails and removes the shard, so a computed
            // shard would be destroyed on the first cluster state applied after it was created.
            final IndexShardRoutingTable indexShardRoutingTable = computedAwareShardRoutingTable(clusterState, shardRouting);
            shard.updateShardState(
                shardRouting,
                primaryTerm,
                primaryReplicaSyncer::resync,
                clusterState.version(),
                inSyncIds,
                indexShardRoutingTable,
                nodes
            );
            updateShardIngestionState(shard, indexMetadata, shardRouting);

        } catch (Exception e) {
            failAndRemoveShard(shardRouting, true, "failed updating shard routing entry", e, clusterState);
            return;
        }

        final IndexShardState state = shard.state();
        if (shardRouting.initializing() && (state == IndexShardState.STARTED || state == IndexShardState.POST_RECOVERY)) {
            // the cluster-manager thinks we are initializing, but we are already started or on POST_RECOVERY and waiting
            // for cluster-manager to confirm a shard started message (either cluster-manager failover, or a cluster event before
            // we managed to tell the cluster-manager we started), mark us as started
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "{} cluster-manager marked shard as initializing, but shard has state [{}], resending shard started to {}",
                    shardRouting.shardId(),
                    state,
                    nodes.getClusterManagerNode()
                );
            }
            if (nodes.getClusterManagerNode() != null) {
                shardStateAction.shardStarted(
                    shardRouting,
                    primaryTerm,
                    "master "
                        + nodes.getClusterManagerNode()
                        + " marked shard as initializing, but shard state is ["
                        + state
                        + "], mark shard as started",
                    SHARD_STATE_ACTION_LISTENER,
                    clusterState
                );
            }
        }
    }

    /**
     * Corrects a computed shard's recovery source using something only this node knows: whether it
     * already holds the data.
     *
     * <p>A computed entry's recovery source comes from the placement function, which every node
     * evaluates identically from the same inputs. That is the point of the design and it is also why the
     * function cannot answer this question: it does not know which node has a store. Stating
     * EMPTY_STORE is right when the index is being created and catastrophic on restart, where it means
     * the shard recovers from an empty store and the index comes back blank while looking healthy.
     *
     * <p>This is the A5 trap in its own habitat. A5 was a recovery source derived from absent in-sync
     * ids silently becoming EMPTY_STORE; here it is stated rather than derived, and is silently wrong
     * for the same reason. Both fail the same way, which is why the assertion that caught it is a
     * document count rather than a shard state: a blank index is STARTED and green.
     *
     * <p>Only ever upgrades EMPTY_STORE to EXISTING_STORE, and only for an index with no published
     * routing entry. Anything the cluster manager allocates keeps the source it was given.
     */
    private ShardRouting withNodeLocalRecoverySource(ClusterState state, ShardRouting shardRouting) {
        if (shardRouting.recoverySource() == null
            || shardRouting.recoverySource().getType() != Type.EMPTY_STORE
            || state.routingTable().hasIndex(shardRouting.index())) {
            return shardRouting;
        }
        IndexMetadata indexMetadata = state.metadata().index(shardRouting.index());
        if (indexMetadata == null) {
            return shardRouting;
        }
        String customDataPath = IndexMetadata.INDEX_DATA_PATH_SETTING.get(indexMetadata.getSettings());
        if (indicesService.hasExistingShardData(shardRouting.shardId(), customDataPath) == false) {
            return shardRouting;
        }
        logger.debug("{} computed shard has existing data on this node, recovering from the existing store", shardRouting.shardId());
        return ComputedShardRouting.initializing(
            shardRouting.shardId(),
            shardRouting.currentNodeId(),
            RecoverySource.ExistingStoreRecoverySource.INSTANCE
        );
    }

    /**
     * The in-sync allocation ids for a shard, from index metadata or from the computed entry.
     *
     * <p>{@code inSyncAllocationIds} is maintained by the cluster manager as shards start and fail, and
     * a computed index never goes through that: its shards are not in the published routing table, so
     * the started-shard task finds nothing to record. The set stays empty, {@code ReplicationTracker}
     * never tracks the primary's own allocation id, primary mode never activates, and every write is
     * rejected with "shard is not in primary mode".
     *
     * <p>So for a computed index the in-sync set is read from the placement rather than from metadata.
     * That is the same trade the whole area makes: state that is a pure function of inputs every node
     * has does not need to be published, and here the placement function is what defines the replication
     * group.
     *
     * <p>This is the A5 trap's neighbourhood and worth naming as such. A5 was a recovery source silently
     * derived from an absent in-sync set; this is primary mode silently denied by the same absence. Both
     * come from treating cluster-manager-maintained state as if it were always there.
     */
    private static Set<String> computedAwareInSyncIds(ClusterState state, ShardRouting shardRouting, IndexMetadata indexMetadata) {
        Set<String> published = indexMetadata.inSyncAllocationIds(shardRouting.id());
        if (published.isEmpty() == false || state.routingTable().hasIndex(shardRouting.index())) {
            return published;
        }
        // state.resolveShard(...) replaces the static registry's
        // the since-deleted AbsentIndexRoutingSuppliers.resolveShard(...) here -- same composition, discovered through the
        // resolver attached to this state's own routing table.
        IndexShardRoutingTable computed = state.resolveShard(shardRouting.shardId());
        if (computed == null) {
            return published;
        }
        Set<String> ids = new HashSet<>();
        for (ShardRouting candidate : computed) {
            if (candidate.active() && candidate.allocationId() != null) {
                ids.add(candidate.allocationId().getId());
            }
        }
        return ids.isEmpty() ? published : ids;
    }

    /**
     * The shard's routing table, from the published entry or the computed one.
     *
     * <p>Falls back to a single-shard table built from the entry the node is applying, for the case
     * where nothing is published and no supplier answers. That is the honest answer rather than a
     * convenient one: the node is being told to hold this shard, so a table containing exactly that
     * shard describes the state it is in.
     */
    private static IndexShardRoutingTable computedAwareShardRoutingTable(ClusterState state, ShardRouting shardRouting) {
        // state.resolveShard(...) replaces the static registry's
        // the since-deleted AbsentIndexRoutingSuppliers.resolveShard(...) here -- same composition, discovered through the
        // resolver attached to this state's own routing table.
        IndexShardRoutingTable resolved = state.resolveShard(shardRouting.shardId());
        if (resolved != null) {
            return resolved;
        }
        return new IndexShardRoutingTable.Builder(shardRouting.shardId()).addShard(shardRouting).build();
    }

    /**
     * Update the ingestion state for the shard using the new metadata.
     */
    private void updateShardIngestionState(Shard shard, IndexMetadata indexMetadata, ShardRouting shardRouting) {
        try {
            boolean isPrimaryOrAllActiveShard = shardRouting.primary() || indexMetadata.isAllActiveIngestionEnabled();
            if (indexMetadata.useIngestionSource() && isPrimaryOrAllActiveShard) {
                shard.updateShardIngestionState(indexMetadata);
            }
        } catch (Exception e) {
            logger.error("Failed to update shard ingestion state", e);
            throw e;
        }
    }

    /**
     * Finds the routing source node for peer recovery, return null if its not found. Note, this method expects the shard
     * routing to *require* peer recovery, use {@link ShardRouting#recoverySource()} to check if its needed or not.
     */
    private static DiscoveryNode findSourceNodeForPeerRecovery(
        Logger logger,
        RoutingTable routingTable,
        DiscoveryNodes nodes,
        ShardRouting shardRouting
    ) {
        DiscoveryNode sourceNode = null;
        if (!shardRouting.primary()) {
            ShardRouting primary = routingTable.shardRoutingTable(shardRouting.shardId()).primaryShard();
            // only recover from started primary, if we can't find one, we will do it next round
            if (primary.active()) {
                sourceNode = nodes.get(primary.currentNodeId());
                if (sourceNode == null) {
                    logger.trace("can't find replica source node because primary shard {} is assigned to an unknown node.", primary);
                }
            } else {
                logger.trace("can't find replica source node because primary shard {} is not active.", primary);
            }
        } else if (shardRouting.relocatingNodeId() != null) {
            sourceNode = nodes.get(shardRouting.relocatingNodeId());
            if (sourceNode == null) {
                logger.trace(
                    "can't find relocation source node for shard {} because it is assigned to an unknown node [{}].",
                    shardRouting.shardId(),
                    shardRouting.relocatingNodeId()
                );
            }
        } else {
            throw new IllegalStateException(
                "trying to find source node for peer recovery when routing state means no peer recovery: " + shardRouting
            );
        }
        return sourceNode;
    }

    // package-private for testing
    public synchronized void handleRecoveryFailure(ShardRouting shardRouting, boolean sendShardFailure, Exception failure) {
        failAndRemoveShard(shardRouting, sendShardFailure, "failed recovery", failure, clusterService.state());
    }

    public void handleRecoveryDone(ReplicationState state, ShardRouting shardRouting, long primaryTerm) {
        RecoveryState recoveryState = (RecoveryState) state;
        if (startComputedShardAfterRecovery(shardRouting, primaryTerm)) {
            return;
        }
        shardStateAction.shardStarted(shardRouting, primaryTerm, "after " + recoveryState.getRecoverySource(), SHARD_STATE_ACTION_LISTENER);
    }

    /**
     * Starts a recovered computed shard here, instead of asking the cluster manager to do it.
     *
     * <p>Telling the cluster manager does nothing for a computed shard: the started-shard task looks it
     * up by allocation id in the published routing table, finds no entry, logs, and reports success. The
     * shard would sit in POST_RECOVERY forever.
     *
     * <p>It has to happen here rather than on the next applied cluster state, and that distinction was
     * measured rather than reasoned. {@code updateShard} runs only when the shard already exists, which
     * means on a state applied after the one that created it, and an idle cluster publishes no such
     * state for a computed index. A probe placed on that path printed nothing at all.
     *
     * @return true when this shard was started locally and the cluster manager should not be told
     */
    private boolean startComputedShardAfterRecovery(ShardRouting shardRouting, long primaryTerm) {
        ClusterState state = clusterService.state();
        if (state.routingTable().hasIndex(shardRouting.index())) {
            return false;
        }
        AllocatedIndex<? extends Shard> indexService = indicesService.indexService(shardRouting.index());
        if (indexService == null) {
            return false;
        }
        Shard shard = indexService.getShardOrNull(shardRouting.id());
        if (shard == null) {
            return false;
        }
        IndexMetadata indexMetadata = state.metadata().indexOrResolved(shardRouting.index());
        if (indexMetadata == null) {
            // The index was deleted while this shard was recovering. There is nothing to start and the
            // deletion path will remove the shard. For a gated index the descriptor answers this rather
            // than cluster state, and a tombstoned descriptor gives the same null, which is the same
            // answer for the same reason.
            return false;
        }
        ShardRouting started = shardRouting.moveToStarted();
        Set<String> inSyncIds = computedAwareInSyncIds(state, started, indexMetadata);
        if (inSyncIds.isEmpty()) {
            // Never hand the tracker an empty in-sync set. It would clear every checkpoint while setting
            // a routing table that names an active allocation, and the next invariant check fails with
            // "local checkpoints {} not in-sync with routing table", which fails and removes the shard.
            //
            // Empty means the placement could not be resolved: the supplier declined, or the index is
            // being deleted underneath a recovery that is still finishing. Both are cases where not
            // starting the shard is the correct answer, and both are reachable in production rather than
            // only in tests.
            logger.debug("{} computed shard has no resolvable in-sync set, leaving it unstarted", shardRouting.shardId());
            return false;
        }
        try {
            shard.updateShardState(
                started,
                primaryTerm,
                primaryReplicaSyncer::resync,
                state.version(),
                inSyncIds,
                computedAwareShardRoutingTable(state, started),
                state.nodes()
            );
        } catch (Exception e) {
            failAndRemoveShard(shardRouting, false, "failed to start computed shard after recovery", e, state);
            return true;
        }
        return true;
    }

    private void failAndRemoveShard(
        ShardRouting shardRouting,
        boolean sendShardFailure,
        String message,
        @Nullable Exception failure,
        ClusterState state
    ) {
        try {
            AllocatedIndex<? extends Shard> indexService = indicesService.indexService(shardRouting.shardId().getIndex());
            if (indexService != null) {
                Shard shard = indexService.getShardOrNull(shardRouting.shardId().id());
                if (shard != null && shard.routingEntry().isSameAllocation(shardRouting)) {
                    indexService.removeShard(shardRouting.shardId().id(), message);
                }
            }
        } catch (ShardNotFoundException e) {
            // the node got closed on us, ignore it
        } catch (Exception inner) {
            inner.addSuppressed(failure);
            logger.warn(
                () -> new ParameterizedMessage(
                    "[{}][{}] failed to remove shard after failure ([{}])",
                    shardRouting.getIndexName(),
                    shardRouting.getId(),
                    message
                ),
                inner
            );
        }
        if (sendShardFailure) {
            sendFailShard(shardRouting, message, failure, state);
        }
    }

    private void sendFailShard(ShardRouting shardRouting, String message, @Nullable Exception failure, ClusterState state) {
        try {
            logger.warn(
                () -> new ParameterizedMessage("{} marking and sending shard failed due to [{}]", shardRouting.shardId(), message),
                failure
            );
            failedShardsCache.put(shardRouting.shardId(), shardRouting);
            shardStateAction.localShardFailed(shardRouting, message, failure, SHARD_STATE_ACTION_LISTENER, state);
        } catch (Exception inner) {
            if (failure != null) inner.addSuppressed(failure);
            logger.warn(
                () -> new ParameterizedMessage(
                    "[{}][{}] failed to mark shard as failed (because of [{}])",
                    shardRouting.getIndexName(),
                    shardRouting.getId(),
                    message
                ),
                inner
            );
        }
    }

    private class FailedShardHandler implements Consumer<IndexShard.ShardFailure> {
        @Override
        public void accept(final IndexShard.ShardFailure shardFailure) {
            final ShardRouting shardRouting = shardFailure.routing;
            threadPool.generic().execute(() -> {
                synchronized (IndicesClusterStateService.this) {
                    failAndRemoveShard(
                        shardRouting,
                        true,
                        "shard failure, reason [" + shardFailure.reason + "]",
                        shardFailure.cause,
                        clusterService.state()
                    );
                }
            });
        }
    }

    /**
     * A shard
     *
     * @opensearch.internal
     */
    public interface Shard {

        /**
         * Returns the shard id of this shard.
         */
        ShardId shardId();

        /**
         * Returns the latest cluster routing entry received with this shard.
         */
        ShardRouting routingEntry();

        /**
         * Returns the latest internal shard state.
         */
        IndexShardState state();

        /**
         * Returns the recovery state associated with this shard.
         */
        RecoveryState recoveryState();

        /**
         * How long since anything read from or wrote to this shard, in milliseconds.
         *
         * <p>On the interface rather than reached through a cast because the only caller,
         * {@link GatedIndexResidency#evictIdleGatedIndices}, sees shards through this type and a cast
         * would make a test double impossible to write. Defaulted to zero -- never idle -- so an
         * implementation that cannot answer keeps its shards rather than losing them to a default that
         * happened to look cold.
         */
        default long idleMillis() {
            return 0L;
        }

        /**
         * Updates the shard state based on an incoming cluster state:
         * - Updates and persists the new routing value.
         * - Updates the primary term if this shard is a primary.
         * - Updates the allocation ids that are tracked by the shard if it is a primary.
         *   See {@link ReplicationTracker#updateFromClusterManager(long, Set, IndexShardRoutingTable)} for details.
         *
         * @param shardRouting                the new routing entry
         * @param primaryTerm                 the new primary term
         * @param primaryReplicaSyncer        the primary-replica resync action to trigger when a term is increased on a primary
         * @param applyingClusterStateVersion the cluster state version being applied when updating the allocation IDs from the master
         * @param inSyncAllocationIds         the allocation ids of the currently in-sync shard copies
         * @param routingTable                the shard routing table
         * @throws IndexShardRelocatedException if shard is marked as relocated and relocation aborted
         * @throws IOException                  if shard state could not be persisted
         */
        void updateShardState(
            ShardRouting shardRouting,
            long primaryTerm,
            BiConsumer<IndexShard, ActionListener<ResyncTask>> primaryReplicaSyncer,
            long applyingClusterStateVersion,
            Set<String> inSyncAllocationIds,
            IndexShardRoutingTable routingTable,
            DiscoveryNodes discoveryNodes
        ) throws IOException;

        default void updateShardIngestionState(IndexMetadata indexMetadata) {};
    }

    /**
     * An allocated index
     *
     * @opensearch.internal
     */
    public interface AllocatedIndex<T extends Shard> extends Iterable<T>, IndexComponent {

        /**
         * Returns the index settings of this index.
         */
        IndexSettings getIndexSettings();

        /**
         * Updates the metadata of this index. Changes become visible through {@link #getIndexSettings()}.
         *
         * @param currentIndexMetadata the current index metadata
         * @param newIndexMetadata the new index metadata
         */
        void updateMetadata(IndexMetadata currentIndexMetadata, IndexMetadata newIndexMetadata);

        /**
         * Checks if index requires refresh from master.
         */
        boolean updateMapping(IndexMetadata currentIndexMetadata, IndexMetadata newIndexMetadata) throws IOException;

        /**
         * Returns shard with given id.
         */
        @Nullable
        T getShardOrNull(int shardId);

        /**
         * Removes shard with given id.
         */
        void removeShard(int shardId, String message);
    }

    /**
     * Allocated indices
     *
     * @opensearch.internal
     */
    public interface AllocatedIndices<T extends Shard, U extends AllocatedIndex<T>> extends Iterable<U> {

        /**
         * Installs the on-demand opener, which is asked for an index that is not here before the absence
         * is reported as an error.
         *
         * <p>An instance method rather than one of this area's static registries, and that is the whole
         * reason it is on this interface. The opener belongs to one node's shard lifecycle, and a static
         * registry in a test JVM would hand every node the last one to start.
         *
         * <p>A no-op by default, so an implementation that holds no shards on demand -- which includes
         * every test double -- needs to know nothing about this.
         */
        default void setOnDemandShardOpener(java.util.function.Consumer<Index> opener) {}

        /**
         * Creates a new {@link IndexService} for the given metadata.
         *
         * @param indexMetadata          the index metadata to create the index for
         * @param builtInIndexListener   a list of built-in lifecycle {@link IndexEventListener} that should should be used along side with
         *                               the per-index listeners
         * @param writeDanglingIndices   whether dangling indices information should be written
         * @throws ResourceAlreadyExistsException if the index already exists.
         */
        U createIndex(IndexMetadata indexMetadata, List<IndexEventListener> builtInIndexListener, boolean writeDanglingIndices)
            throws IOException;

        /**
         * Verify that the contents on disk for the given index is deleted; if not, delete the contents.
         * This method assumes that an index is already deleted in the cluster state and/or explicitly
         * through index tombstones.
         * @param index {@code Index} to make sure its deleted from disk
         * @param clusterState {@code ClusterState} to ensure the index is not part of it
         * @return IndexMetadata for the index loaded from disk
         */
        IndexMetadata verifyIndexIsDeleted(Index index, ClusterState clusterState);

        /**
         * Deletes an index that is not assigned to this node. This method cleans up all disk folders relating to the index
         * but does not deal with in-memory structures. For those call {@link #removeIndex(Index, IndexRemovalReason, String)}
         */
        void deleteUnassignedIndex(String reason, IndexMetadata metadata, ClusterState clusterState);

        /**
         * Removes the given index from this service and releases all associated resources. Persistent parts of the index
         * like the shards files, state and transaction logs are kept around in the case of a disaster recovery.
         * @param index the index to remove
         * @param reason the reason to remove the index
         * @param extraInfo extra information that will be used for logging and reporting
         */
        void removeIndex(Index index, IndexRemovalReason reason, String extraInfo);

        /**
         * Returns an IndexService for the specified index if exists otherwise returns <code>null</code>.
         */
        @Nullable
        U indexService(Index index);

        /**
         * Creates a shard for the specified shard routing and starts recovery.
         *
         * @param shardRouting           the shard routing
         * @param recoveryTargetService  recovery service for the target
         * @param recoveryListener       a callback when recovery changes state (finishes or fails)
         * @param repositoriesService    service responsible for snapshot/restore
         * @param onShardFailure         a callback when this shard fails
         * @param globalCheckpointSyncer a callback when this shard syncs the global checkpoint
         * @param retentionLeaseSyncer   a callback when this shard syncs retention leases
         * @param targetNode             the node where this shard will be recovered
         * @param sourceNode             the source node to recover this shard from (it might be null)
         * @param remoteStoreStatsTrackerFactory factory for remote store stats trackers
         * @param mergedSegmentWarmerFactory factory for merged segment warmer
         * @param mergedSegmentPublisher merged segment publisher
         * @return a new shard
         * @throws IOException if an I/O exception occurs when creating the shard
         */
        T createShard(
            ShardRouting shardRouting,
            SegmentReplicationCheckpointPublisher checkpointPublisher,
            PeerRecoveryTargetService recoveryTargetService,
            RecoveryListener recoveryListener,
            RepositoriesService repositoriesService,
            Consumer<IndexShard.ShardFailure> onShardFailure,
            Consumer<ShardId> globalCheckpointSyncer,
            RetentionLeaseSyncer retentionLeaseSyncer,
            DiscoveryNode targetNode,
            @Nullable DiscoveryNode sourceNode,
            RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory,
            DiscoveryNodes discoveryNodes,
            MergedSegmentWarmerFactory mergedSegmentWarmerFactory,
            MergedSegmentPublisher mergedSegmentPublisher,
            ReferencedSegmentsPublisher referencedSegmentsPublisher
        ) throws IOException;

        /**
         * Returns shard for the specified id if it exists otherwise returns <code>null</code>.
         */
        default T getShardOrNull(ShardId shardId) {
            U indexRef = indexService(shardId.getIndex());
            if (indexRef != null) {
                return indexRef.getShardOrNull(shardId.id());
            }
            return null;
        }

        /**
         * Whether this node already holds data for the shard.
         *
         * <p>Only the node can answer this, which is the whole reason it is asked here. A computed
         * shard's routing entry is derived from a placement function that every node evaluates
         * identically, so it cannot know which node has the data, and a recovery source taken from it is
         * therefore right at creation and wrong at restart.
         *
         * <p>Defaulted to false so that implementations which do not manage local storage, including the
         * test doubles, are unaffected and behave exactly as before.
         */
        default boolean hasExistingShardData(ShardId shardId, String customDataPath) {
            return false;
        }

        void processPendingDeletes(Index index, IndexSettings indexSettings, TimeValue timeValue) throws IOException, InterruptedException,
            ShardLockObtainFailedException;

        /**
         * Why the index was removed
         *
         * @opensearch.api
         */
        @PublicApi(since = "1.0.0")
        enum IndexRemovalReason {
            /**
             * Shard of this index were previously assigned to this node but all shards have been relocated.
             * The index should be removed and all associated resources released. Persistent parts of the index
             * like the shards files, state and transaction logs are kept around in the case of a disaster recovery.
             */
            NO_LONGER_ASSIGNED,
            /**
             * The index is deleted. Persistent parts of the index  like the shards files, state and transaction logs are removed once
             * all resources are released.
             */
            DELETED,

            /**
             * The index has been closed. The index should be removed and all associated resources released. Persistent parts of the index
             * like the shards files, state and transaction logs are kept around in the case of a disaster recovery.
             */
            CLOSED,

            /**
             * Something around index management has failed and the index should be removed.
             * Persistent parts of the index like the shards files, state and transaction logs are kept around in the
             * case of a disaster recovery.
             */
            FAILURE,

            /**
             * The index has been reopened. The index should be removed and all associated resources released. Persistent parts of the index
             * like the shards files, state and transaction logs are kept around in the case of a disaster recovery.
             */
            REOPENED,
        }
    }
}
