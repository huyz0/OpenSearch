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
import org.opensearch.serverless.storage.resharding.WritePartitionRoutingMetadata;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
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

    /**
     * Whether {@code targetMetadata} is safe to assign as a write-routing target of {@code
     * requestedAliasName}: it must not already be a write-routing target of a <em>different</em>
     * alias (silent reassignment would leave the old alias with an unrouteable hole at that
     * partition index -- see {@code WritePartitionRoutingActionFilter#resolvePartitionTarget}), and
     * it must not already be fenced as a split source (reassigning a fenced source as a write
     * target would undermine the fencing guarantee {@link SourceSplitFenceMetadata}'s own javadoc
     * describes). Re-running the identical assignment (same alias) is left permitted, matching this
     * action's existing idempotent-retry contract.
     *
     * @return {@code null} if assignable, otherwise the exception to fail the request with.
     */
    private static IllegalArgumentException validateTargetAssignable(IndexMetadata targetMetadata, String requestedAliasName) {
        String existingAlias = WritePartitionRoutingMetadata.writeRoutingAlias(targetMetadata);
        if (existingAlias != null && existingAlias.equals(requestedAliasName) == false) {
            return new IllegalArgumentException(
                "write-routing target index ["
                    + targetMetadata.getIndex().getName()
                    + "] is already a write-routing target of a different alias ["
                    + existingAlias
                    + "] -- refusing to reassign it to ["
                    + requestedAliasName
                    + "]"
            );
        }
        if (SourceSplitFenceMetadata.isFencedSource(targetMetadata)) {
            return new IllegalArgumentException(
                "write-routing target index ["
                    + targetMetadata.getIndex().getName()
                    + "] is fenced as a split source -- refusing to assign it as a write-routing target"
            );
        }
        return null;
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
            IndexMetadata targetMetadata = state.metadata().index(targetIndexName);
            if (targetMetadata == null) {
                listener.onFailure(
                    new IllegalArgumentException(
                        "write-routing target index [" + targetIndexName + "] does not exist -- refusing to route writes to it"
                    )
                );
                return;
            }
            IllegalArgumentException conflict = validateTargetAssignable(targetMetadata, aliasName);
            if (conflict != null) {
                listener.onFailure(conflict);
                return;
            }
        }
        clusterService.submitStateUpdateTask(
            "serverless-storage-enable-write-partition-routing",
            new ClusterStateUpdateTask(Priority.URGENT) {
                // Tracks any target that vanished between the pre-check above and this task
                // actually running -- the same race TransportFenceSplitSourceAction/
                // TransportDisableWritePartitionRoutingAction's own fixes close, applied here so a
                // concurrent deletion can't make this silently assign fewer than numPartitions
                // targets while still reporting a full success.
                private final List<String> missingDuringExecute = new ArrayList<>();
                private final List<String> conflictingDuringExecute = new ArrayList<>();

                @Override
                public ClusterState execute(ClusterState currentState) {
                    Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                    for (int partitionIndex = 0; partitionIndex < numPartitions; partitionIndex++) {
                        String targetIndexName = targetIndexNames.get(partitionIndex);
                        IndexMetadata targetMetadata = currentState.metadata().index(targetIndexName);
                        if (targetMetadata == null) {
                            missingDuringExecute.add(targetIndexName);
                            continue;
                        }
                        // Re-check the same conflict the pre-check above validated -- a concurrent
                        // enable/fence call for a DIFFERENT alias could have raced in between,
                        // silently stealing this target out from under its current alias or
                        // reassigning a fenced split source if left unchecked here.
                        if (validateTargetAssignable(targetMetadata, aliasName) != null) {
                            conflictingDuringExecute.add(targetIndexName);
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
                    if (missingDuringExecute.isEmpty() == false) {
                        logger.warn(
                            "target index(es) were deleted concurrently with enabling write-partition-routing for alias ["
                                + aliasName
                                + "], reporting failure rather than a false acknowledgement: "
                                + missingDuringExecute
                        );
                        listener.onFailure(
                            new IllegalStateException(
                                "target index(es) were deleted before write-partition-routing could be enabled for alias ["
                                    + aliasName
                                    + "]: "
                                    + missingDuringExecute
                            )
                        );
                        return;
                    }
                    if (conflictingDuringExecute.isEmpty() == false) {
                        logger.warn(
                            "target index(es) were concurrently assigned elsewhere (a different alias, or fenced as a split "
                                + "source) while enabling write-partition-routing for alias ["
                                + aliasName
                                + "], reporting failure rather than a false acknowledgement: "
                                + conflictingDuringExecute
                        );
                        listener.onFailure(
                            new IllegalStateException(
                                "target index(es) became unassignable before write-partition-routing could be enabled for alias ["
                                    + aliasName
                                    + "]: "
                                    + conflictingDuringExecute
                            )
                        );
                        return;
                    }
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
