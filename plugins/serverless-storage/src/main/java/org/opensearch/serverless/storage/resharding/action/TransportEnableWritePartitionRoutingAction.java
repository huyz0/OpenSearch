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
import org.opensearch.serverless.storage.resharding.WritePartitionRoutingMetadata;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * The actual work behind {@link EnableWritePartitionRoutingAction} -- see that class's own javadoc
 * for the full design rationale.
 */
public class TransportEnableWritePartitionRoutingAction extends TransportClusterManagerNodeAction<
    EnableWritePartitionRoutingRequest,
    AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportEnableWritePartitionRoutingAction.class);

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
    public TransportEnableWritePartitionRoutingAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            EnableWritePartitionRoutingAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            EnableWritePartitionRoutingRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(EnableWritePartitionRoutingRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        EnableWritePartitionRoutingRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        String aliasName = request.aliasName();
        List<String> targetIndexNames = request.targetIndexNames();
        int numPartitions = targetIndexNames.size();
        for (String targetIndexName : targetIndexNames) {
            if (state.metadata().index(targetIndexName) == null) {
                listener.onFailure(
                    new IllegalArgumentException(
                        "write-routing target index [" + targetIndexName + "] does not exist -- refusing to route writes to it"
                    )
                );
                return;
            }
        }
        clusterService.submitStateUpdateTask(
            "serverless-storage-enable-write-partition-routing",
            new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                    for (int partitionIndex = 0; partitionIndex < numPartitions; partitionIndex++) {
                        String targetIndexName = targetIndexNames.get(partitionIndex);
                        IndexMetadata targetMetadata = currentState.metadata().index(targetIndexName);
                        if (targetMetadata == null) {
                            continue;
                        }
                        metadataBuilder.put(
                            WritePartitionRoutingMetadata.withAssignment(targetMetadata, aliasName, partitionIndex, numPartitions),
                            true
                        );
                    }
                    return ClusterState.builder(currentState).metadata(metadataBuilder).build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.info(
                        "enabled write-partition-routing for alias ["
                            + aliasName
                            + "] across "
                            + numPartitions
                            + " target(s): "
                            + targetIndexNames
                    );
                    listener.onResponse(new AcknowledgedResponse(true));
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn("failed to enable write-partition-routing for alias [" + aliasName + "]", e);
                    listener.onFailure(e);
                }
            }
        );
    }
}
