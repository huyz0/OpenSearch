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
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
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
        String role = reader ? "reader" : "writer";
        clusterService.submitStateUpdateTask("serverless-storage-reactivate-shards", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                IndexMetadata indexMetadata = currentState.metadata().index(indexName);
                if (indexMetadata == null) {
                    return currentState;
                }
                IndexMetadata updated = reader
                    ? SuspendedShardsMetadata.withAllReaderShardsReactivated(indexMetadata)
                    : SuspendedShardsMetadata.withAllShardsReactivated(indexMetadata);
                if (updated == indexMetadata) {
                    return currentState;
                }
                return ClusterState.builder(currentState).metadata(Metadata.builder(currentState.metadata()).put(updated, true)).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (oldState != newState) {
                    logger.info("reactivating suspended serverless-storage " + role + " shard(s) of index [" + indexName + "]");
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
                logger.warn("failed to reactivate suspended serverless-storage " + role + " shard(s) of index [" + indexName + "]", e);
                listener.onFailure(e);
            }
        });
    }
}
