/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.storage.allocation.SuspendedShardsMetadata;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * The actual work behind {@link ReactivateShardsAction}: clears every suspended-shard marker on one
 * index and triggers the resulting reroute, always executed on the elected cluster-manager node --
 * see {@link ReactivateShardsRequest}'s own javadoc for why that indirection is required here (a
 * plain {@code ClusterService#submitStateUpdateTask} call throws {@code NotClusterManagerException}
 * when invoked from a node that isn't currently the cluster-manager, a real bug this action's own
 * predecessor code hit and this class exists specifically to fix).
 */
public final class TransportReactivateShardsAction extends TransportClusterManagerNodeAction<
    ReactivateShardsRequest,
    AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportReactivateShardsAction.class);

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link TransportClusterManagerNodeAction} to register this action.
     * @param clusterService reads and mutates cluster state, and triggers the resulting reroute.
     * @param threadPool used by {@link TransportClusterManagerNodeAction}'s own base machinery.
     * @param actionFilters applied by {@link TransportClusterManagerNodeAction} around every request.
     * @param indexNameExpressionResolver required by {@link TransportClusterManagerNodeAction}'s constructor, unused here.
     */
    @Inject
    public TransportReactivateShardsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ReactivateShardsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ReactivateShardsRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(ReactivateShardsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        ReactivateShardsRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        String indexName = request.indexName();
        boolean reader = request.reader();
        if (reactivateGated(state, indexName, reader)) {
            // A gated index has nothing in cluster state to rewrite, so there is no update to submit: the
            // suspension lives in this node's own registry and clearing it is the whole reactivation.
            // Answered here rather than falling through, because the task below would find no metadata,
            // return the state unchanged, and report success for a wake that never happened -- which is
            // exactly how a suspended gated shard came to be unreachable forever.
            listener.onResponse(new AcknowledgedResponse(true));
            return;
        }
        ReactivateTask task = new ReactivateTask(indexName, reader, listener);
        clusterService.submitStateUpdateTask(
            "serverless-storage-reactivate-shards",
            task,
            ClusterStateTaskConfig.build(Priority.URGENT),
            reactivateExecutor,
            task
        );
    }

    /**
     * Wakes a gated index's sleeping shards on this node, and reports whether this was a gated index at
     * all.
     *
     * <p><b>Why this exists on the cluster-manager specifically.</b> The gated suspension record is
     * node-local and, in practice, written only by {@code ShardSuspensionCoordinator}, which runs only on
     * the elected cluster manager. So the node whose placement a gated suspension actually affects is this
     * one, and this action is already the thing every reactivation path forwards to the cluster manager.
     * The forwarding that was built for cluster-state mutation turns out to be exactly the routing this
     * needs, for a reactivation that mutates no cluster state at all.
     *
     * <p>Resolved through {@link org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers}, the way
     * the rest of the plugin resolves a gated index, rather than through {@code metadata().index(name)}:
     * the defining property of a gated index is that the second one answers null for it, so a reactivation
     * path built on it could never wake one.
     *
     * @return true when this was a gated index and there is therefore no cluster-state work to do.
     */
    private static boolean reactivateGated(ClusterState state, String indexName, boolean reader) {
        if (state.metadata().index(indexName) != null) {
            return false;
        }
        org.opensearch.serverless.storage.scaletozero.GatedShardSuspensionRegistry suspensions =
            org.opensearch.serverless.storage.scaletozero.GatedShardSuspensionRegistry.installed();
        if (suspensions == null) {
            return false; // not a gated cluster; let the ordinary path answer for whatever this name is.
        }
        IndexMetadata gated = org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers.metadataOrDescriptor(
            state.metadata(),
            indexName
        );
        if (gated == null) {
            return false; // no such index anywhere: the ordinary path's own no-op answer is the right one.
        }
        if (suspensions.reactivateAll(gated.getIndexUUID(), reader)) {
            logger.info(
                "reactivated gated {} shard(s) of index [{}] without a cluster state update",
                reader ? "reader" : "writer",
                indexName
            );
        }
        return true;
    }

    /**
     * One reactivation request, as a batchable unit.
     *
     * <p>G1 measured a plain submission at 6 to 13 ms of publication, paid once per request. A burst of
     * wakes, which is exactly what a returning tenant population looks like, was that cost multiplied by
     * the burst. Folded, the burst is one publication.
     *
     * <p>{@code reactivated} exists for the same reason as the suspend path's flag: a batch reports one
     * pair of states to every task in it, so whether this particular index was changed has to be recorded
     * while its transform runs. Without it a batch containing one real reactivation would log and reroute
     * for every request in the batch.
     *
     * <p>The listener answers per task rather than per batch. A caller that never gets a response is a
     * wake that appears to hang, which is worse than a slow one.
     */
    private final class ReactivateTask implements ClusterStateTaskListener {
        private final String indexName;
        private final boolean reader;
        private final ActionListener<AcknowledgedResponse> listener;
        private volatile boolean reactivated;

        ReactivateTask(String indexName, boolean reader, ActionListener<AcknowledgedResponse> listener) {
            this.indexName = indexName;
            this.reader = reader;
            this.listener = listener;
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            if (reactivated) {
                logger.info(
                    "reactivating suspended serverless-storage " + (reader ? "reader" : "writer") + " shard(s) of index [" + indexName + "]"
                );
                // Still one reroute per changed task rather than one per batch. BatchedRerouteService
                // already collapses concurrent requests, so the saving from doing it here as well would
                // be small and the behaviour change would not be measured.
                clusterService.getRerouteService()
                    .reroute(
                        "serverless-storage reactivate " + indexName,
                        Priority.URGENT,
                        ActionListener.wrap(
                            s -> {},
                            e -> logger.warn(
                                "reroute after reactivating [" + indexName + "] failed, a later reroute will still pick it up",
                                e
                            )
                        )
                    );
            }
            listener.onResponse(new AcknowledgedResponse(true));
        }

        @Override
        public void onFailure(String source, Exception e) {
            logger.warn(
                "failed to reactivate suspended serverless-storage "
                    + (reader ? "reader" : "writer")
                    + " shard(s) of index ["
                    + indexName
                    + "]",
                e
            );
            listener.onFailure(e);
        }
    }

    private final ClusterStateTaskExecutor<ReactivateTask> reactivateExecutor = (currentState, tasks) -> {
        ClusterStateTaskExecutor.ClusterTasksResult.Builder<ReactivateTask> builder = ClusterStateTaskExecutor.ClusterTasksResult.builder();
        ClusterState state = currentState;
        for (ReactivateTask task : tasks) {
            try {
                ClusterState next = reactivate(state, task.indexName, task.reader, threadPool.absoluteTimeInMillis());
                task.reactivated = next != state;
                state = next;
                builder.success(task);
            } catch (Exception e) {
                task.reactivated = false;
                builder.failure(task, e);
            }
        }
        return builder.build(state);
    };

    /**
     * Clears one index's suspended markers for the given role and, if the index has no
     * {@link org.opensearch.cluster.routing.IndexRoutingTable} entry, recreates it -- in one state.
     *
     * <p>Package-private and static so the transition itself can be tested without standing up a
     * transport action; {@link #clusterManagerOperation} is the only production caller.
     */
    static ClusterState reactivate(ClusterState currentState, String indexName, boolean reader, long nowMillis) {
        IndexMetadata indexMetadata = currentState.metadata().index(indexName);
        if (indexMetadata == null) {
            return currentState;
        }
        IndexMetadata updated = reader
            ? SuspendedShardsMetadata.withAllReaderShardsReactivated(indexMetadata, nowMillis)
            : SuspendedShardsMetadata.withAllShardsReactivated(indexMetadata, nowMillis);

        // ShardSuspensionCoordinator removes a fully cold index's routing entry, so
        // reactivating one has to put it back -- and in this same update, not a later one.
        // Two things depend on that being atomic. ShardReactivationActionFilter reads
        // absent-routing-with-present-metadata as "not started yet" and holds the search;
        // if the marker cleared first and the entry appeared later, an index with no
        // search-only replicas would short-circuit that check and proceed against nothing.
        // And a reader-role reactivation of a writer-cold index must not resurrect only
        // half the shards. addAsRecovery is the right constructor here rather than
        // addAsNew: the shard data exists in the object store, and ExistingStoreRecoverySource
        // is what ServerlessStorageExistingShardsAllocator is written to handle.
        //
        // Not gated on the coordinator's prune setting on purpose. An entry pruned while
        // pruning was enabled must still come back if the setting is turned off afterwards,
        // otherwise disabling the feature strands every already-cold index.
        boolean routingMissing = currentState.routingTable().hasIndex(indexName) == false;
        if (updated == indexMetadata && routingMissing == false) {
            return currentState;
        }
        ClusterState.Builder builder = ClusterState.builder(currentState);
        if (updated != indexMetadata) {
            builder.metadata(Metadata.builder(currentState.metadata()).put(updated, true));
        }
        if (routingMissing) {
            builder.routingTable(RoutingTable.builder(currentState.routingTable()).add(coldIndexRoutingTable(updated)).build());
        }
        return builder.build();
    }

    /**
     * Builds the routing entry for a reactivating cold index, with every primary recovering from its
     * existing store.
     *
     * <p><b>Not {@code RoutingTable.Builder#addAsRecovery}, and the difference is data loss.</b> That
     * helper picks the recovery source from {@code inSyncAllocationIds}, falling back to {@code
     * EmptyStoreRecoverySource} when the set is empty -- and it is empty here, because the eviction
     * that made this index cold went through {@code CancelAllocationCommand}, which calls {@code
     * RoutingAllocation#removeAllocationId} on the cancelled copy. So the obvious helper would have
     * resurrected a scaled-to-zero index as a brand new empty one, discarding its contents, and it
     * would have done so silently. A unit test asserting the recovery source caught it; nothing about
     * the call site looked wrong.
     *
     * <p>{@code ExistingStoreRecoverySource} is correct despite the empty in-sync set because for
     * this engine the shard's data lives in the object store rather than on any node, and {@code
     * ServerlessStorageExistingShardsAllocator} exists precisely to allocate such a shard -- core's
     * default gateway allocator would answer {@code NO_VALID_SHARD_COPY} for it.
     */
    private static IndexRoutingTable coldIndexRoutingTable(IndexMetadata indexMetadata) {
        Index index = indexMetadata.getIndex();
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index);
        UnassignedInfo unassignedInfo = new UnassignedInfo(
            UnassignedInfo.Reason.CLUSTER_RECOVERED,
            "serverless-storage scale-to-zero reactivation"
        );
        for (int shardNumber = 0; shardNumber < indexMetadata.getNumberOfShards(); shardNumber++) {
            ShardId shardId = new ShardId(index, shardNumber);
            IndexShardRoutingTable.Builder shardBuilder = new IndexShardRoutingTable.Builder(shardId);
            shardBuilder.addShard(
                ShardRouting.newUnassigned(shardId, true, RecoverySource.ExistingStoreRecoverySource.INSTANCE, unassignedInfo)
            );
            for (int replica = 0; replica < indexMetadata.getNumberOfReplicas(); replica++) {
                shardBuilder.addShard(
                    ShardRouting.newUnassigned(shardId, false, RecoverySource.PeerRecoverySource.INSTANCE, unassignedInfo)
                );
            }
            for (int searchReplica = 0; searchReplica < indexMetadata.getNumberOfSearchOnlyReplicas(); searchReplica++) {
                shardBuilder.addShard(
                    ShardRouting.newUnassigned(shardId, false, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE, unassignedInfo)
                );
            }
            builder.addIndexShard(shardBuilder.build());
        }
        return builder.build();
    }
}
