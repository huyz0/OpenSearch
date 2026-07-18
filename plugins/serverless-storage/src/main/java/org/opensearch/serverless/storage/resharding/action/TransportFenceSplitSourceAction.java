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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.serverless.storage.resharding.SourceSplitFenceMetadata;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * The actual work behind {@link FenceSplitSourceAction} -- see that class's and {@link
 * org.opensearch.serverless.storage.resharding.SourceSplitFenceMetadata}'s own javadoc for the full
 * design rationale.
 */
public class TransportFenceSplitSourceAction extends TransportClusterManagerNodeAction<FenceSplitSourceRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportFenceSplitSourceAction.class);

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
    public TransportFenceSplitSourceAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            FenceSplitSourceAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            FenceSplitSourceRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(FenceSplitSourceRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void clusterManagerOperation(FenceSplitSourceRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        String sourceIndexName = request.sourceIndexName();
        String supersedingAliasName = request.supersedingAliasName();
        if (state.metadata().index(sourceIndexName) == null) {
            listener.onFailure(new IllegalArgumentException("source index [" + sourceIndexName + "] does not exist"));
            return;
        }
        clusterService.submitStateUpdateTask(
            "serverless-storage-fence-split-source",
            new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    IndexMetadata sourceMetadata = currentState.metadata().index(sourceIndexName);
                    if (sourceMetadata == null) {
                        return currentState;
                    }
                    Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                    metadataBuilder.put(SourceSplitFenceMetadata.withFence(sourceMetadata, supersedingAliasName), true);
                    return ClusterState.builder(currentState).metadata(metadataBuilder).build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.info("fenced split source [" + sourceIndexName + "], superseded by alias [" + supersedingAliasName + "]");
                    listener.onResponse(new AcknowledgedResponse(true));
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn("failed to fence split source [" + sourceIndexName + "]", e);
                    listener.onFailure(e);
                }
            }
        );
    }
}
