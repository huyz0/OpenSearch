/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.SplitShardsMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Set;

/**
 * The actual work behind {@link CancelInPlaceSplitAction} -- see that class's own javadoc for why an
 * operator cancel path has to exist (finding R-11).
 *
 * <p>The state transition performed here is deliberately identical to the one core's own {@code
 * MetadataInPlaceSplitShardCommitService#applyCancel} performs when a child exhausts its allocation
 * retries: mark the split cancelled in {@link SplitShardsMetadata} and, in the <em>same</em>
 * cluster-state update, drop the reserved children's routing entries. Doing both in one update is
 * what makes it safe -- there is never a published state in which the metadata says "no split in
 * progress" while routing entries for phantom children still exist. What is different is only the
 * trigger: an explicit operator request instead of a retry counter.
 */
public class TransportCancelInPlaceSplitAction extends TransportClusterManagerNodeAction<CancelInPlaceSplitRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportCancelInPlaceSplitAction.class);

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link TransportClusterManagerNodeAction} to register this action.
     * @param clusterService reads and mutates cluster state.
     * @param threadPool used by {@link TransportClusterManagerNodeAction}'s own base machinery.
     * @param actionFilters applied by {@link TransportClusterManagerNodeAction} around every request.
     * @param indexNameExpressionResolver required by {@link TransportClusterManagerNodeAction}'s constructor, unused here.
     */
    @Inject
    public TransportCancelInPlaceSplitAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            CancelInPlaceSplitAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            CancelInPlaceSplitRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(CancelInPlaceSplitRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        CancelInPlaceSplitRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        String indexName = request.indexName();
        int parentShardId = request.shardId();
        IndexMetadata indexMetadata = state.metadata().index(indexName);
        if (indexMetadata == null) {
            listener.onFailure(new IllegalArgumentException("index [" + indexName + "] does not exist"));
            return;
        }
        if (indexMetadata.getSplitShardsMetadata().isSplitOfShardInProgress(parentShardId) == false) {
            listener.onFailure(
                new IllegalArgumentException(
                    "shard [" + parentShardId + "] of index [" + indexName + "] has no in-progress in-place split to cancel"
                )
            );
            return;
        }
        clusterService.submitStateUpdateTask(
            "serverless-storage-cancel-in-place-split",
            // URGENT because the whole point of this action is to release a block that is currently
            // rejecting every write to a live shard: queueing behind ordinary cluster-state work
            // would extend exactly the outage the operator invoked it to end.
            new ClusterStateUpdateTask(Priority.URGENT) {

                // Distinguishes a real cancel from the benign "someone else already resolved it"
                // race, so the log line says which happened.
                private boolean cancelled = false;

                @Override
                public ClusterState execute(ClusterState currentState) {
                    IndexMetadata current = currentState.metadata().index(indexName);
                    if (current == null || current.getSplitShardsMetadata().isSplitOfShardInProgress(parentShardId) == false) {
                        // The split committed, or core cancelled it, between the pre-check above and
                        // this task running. Return the same reference so no no-op state is published.
                        return currentState;
                    }
                    Set<Integer> childIds = current.getSplitShardsMetadata().getChildShardIdsOfParent(parentShardId);
                    SplitShardsMetadata.Builder splitBuilder = new SplitShardsMetadata.Builder(current.getSplitShardsMetadata());
                    splitBuilder.cancelSplit(parentShardId);
                    Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata())
                        .put(IndexMetadata.builder(current).splitShardsMetadata(splitBuilder.build()));

                    Index index = current.getIndex();
                    IndexRoutingTable currentIndexRoutingTable = currentState.routingTable().index(indexName);
                    RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                    if (currentIndexRoutingTable != null) {
                        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
                        for (IndexShardRoutingTable shardTable : currentIndexRoutingTable) {
                            if (childIds.contains(shardTable.shardId().id()) == false) {
                                indexRoutingTableBuilder.addIndexShard(shardTable);
                            }
                        }
                        routingTableBuilder.add(indexRoutingTableBuilder);
                    }
                    cancelled = true;
                    return ClusterState.builder(currentState).metadata(metadataBuilder).routingTable(routingTableBuilder.build()).build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    if (cancelled) {
                        logger.info(
                            "cancelled in-place split of shard [{}] of index [{}] on operator request -- writes to the parent "
                                + "shard are accepted again",
                            parentShardId,
                            indexName
                        );
                    } else {
                        logger.info(
                            "in-place split of shard [{}] of index [{}] was already resolved before the cancel applied -- no-op",
                            parentShardId,
                            indexName
                        );
                    }
                    listener.onResponse(new AcknowledgedResponse(true));
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn("failed to cancel in-place split of shard [" + parentShardId + "] of index [" + indexName + "]", e);
                    listener.onFailure(e);
                }
            }
        );
    }
}
