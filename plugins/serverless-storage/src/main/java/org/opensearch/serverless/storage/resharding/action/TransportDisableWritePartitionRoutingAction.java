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
import java.util.ArrayList;
import java.util.List;

/**
 * The actual work behind {@link DisableWritePartitionRoutingAction} -- see that class's own javadoc
 * for the full design rationale.
 */
public class TransportDisableWritePartitionRoutingAction extends TransportClusterManagerNodeAction<
    DisableWritePartitionRoutingRequest,
    AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDisableWritePartitionRoutingAction.class);

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
    public TransportDisableWritePartitionRoutingAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            DisableWritePartitionRoutingAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DisableWritePartitionRoutingRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(DisableWritePartitionRoutingRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        DisableWritePartitionRoutingRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        List<String> targetIndexNames = request.targetIndexNames();
        List<String> missingUpFront = new ArrayList<>();
        for (String targetIndexName : targetIndexNames) {
            if (state.metadata().index(targetIndexName) == null) {
                missingUpFront.add(targetIndexName);
            }
        }
        if (missingUpFront.isEmpty() == false) {
            listener.onFailure(
                new IllegalArgumentException(
                    "target index(es) do not exist, refusing to report a false acknowledgement: " + missingUpFront
                )
            );
            return;
        }
        clusterService.submitStateUpdateTask(
            "serverless-storage-disable-write-partition-routing",
            new ClusterStateUpdateTask(Priority.URGENT) {
                // Tracks any target that vanished between the pre-check above and this task
                // actually running -- the same race TransportFenceSplitSourceAction's own fix
                // closes, applied here so a concurrent deletion can't make this report a false
                // acknowledgement either.
                private final List<String> missingDuringExecute = new ArrayList<>();

                @Override
                public ClusterState execute(ClusterState currentState) {
                    Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                    for (String targetIndexName : targetIndexNames) {
                        IndexMetadata targetMetadata = currentState.metadata().index(targetIndexName);
                        if (targetMetadata == null) {
                            missingDuringExecute.add(targetIndexName);
                            continue;
                        }
                        metadataBuilder.put(WritePartitionRoutingMetadata.withoutAssignment(targetMetadata), true);
                    }
                    return ClusterState.builder(currentState).metadata(metadataBuilder).build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    if (missingDuringExecute.isEmpty() == false) {
                        logger.warn(
                            "target index(es) were deleted concurrently with disabling write-partition-routing, "
                                + "reporting failure rather than a false acknowledgement: "
                                + missingDuringExecute
                        );
                        listener.onFailure(
                            new IllegalStateException(
                                "target index(es) were deleted before write-partition-routing could be disabled: " + missingDuringExecute
                            )
                        );
                        return;
                    }
                    logger.info("disabled write-partition-routing for target(s): " + targetIndexNames);
                    listener.onResponse(new AcknowledgedResponse(true));
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn("failed to disable write-partition-routing for target(s): " + targetIndexNames, e);
                    listener.onFailure(e);
                }
            }
        );
    }
}
