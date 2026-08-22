/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The actual work behind {@link ShardSplitCandidatesAction}: fans {@link NodeRequest} out to every
 * data node (each node answering with its own {@code ShardActivityRegistry#snapshotWritesPerMinute()}),
 * then lets {@link ShardSplitCandidatesResponse}'s own constructor merge, join with cluster
 * metadata, and threshold-evaluate the results -- mirrors {@code
 * org.opensearch.serverless.storage.scaleup.action.TransportScaleUpCandidatesAction}.
 *
 * <p>No I/O on the node-local side, same "safe to answer directly" reasoning as that action --
 * {@link ThreadPool.Names#SAME} is used for both the per-node operation and final response
 * assembly.
 */
public class TransportShardSplitCandidatesAction extends TransportNodesAction<
    ShardSplitCandidatesRequest,
    ShardSplitCandidatesResponse,
    TransportShardSplitCandidatesAction.NodeRequest,
    NodeShardSplitCandidatesResponse> {

    private final ServerlessStoragePlugin plugin;

    /**
     * Creates the transport action.
     *
     * @param threadPool used by {@link TransportNodesAction} to dispatch the fan-out.
     * @param clusterService resolves the cluster's data nodes/name, and (in {@link #newResponse})
     *                        the current cluster state's index metadata the merge needs.
     * @param transportService used by {@link TransportNodesAction} to register this action.
     * @param actionFilters applied by {@link TransportNodesAction} around every request.
     * @param plugin resolves this node's {@link ServerlessStoragePlugin#shardActivityRegistry()}
     *               and this node's configured default threshold.
     */
    @Inject
    public TransportShardSplitCandidatesAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin
    ) {
        super(
            ShardSplitCandidatesAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ShardSplitCandidatesRequest::new,
            NodeRequest::new,
            ThreadPool.Names.SAME,
            NodeShardSplitCandidatesResponse.class
        );
        this.plugin = plugin;
    }

    /**
     * @param request the original cluster-wide request, carrying any caller-supplied threshold override.
     * @param responses every node's raw {@link NodeShardSplitCandidatesResponse}.
     * @param failures any per-node failures encountered while fanning out.
     * @return the merged, threshold-evaluated {@link ShardSplitCandidatesResponse}.
     */
    @Override
    protected ShardSplitCandidatesResponse newResponse(
        ShardSplitCandidatesRequest request,
        List<NodeShardSplitCandidatesResponse> responses,
        List<FailedNodeException> failures
    ) {
        long writesPerMinuteThreshold = request.writesPerMinuteThreshold() == ShardSplitCandidatesRequest.USE_DEFAULT_WPM_THRESHOLD
            ? plugin.reshardingSplitCandidateWritesPerMinuteThreshold()
            : request.writesPerMinuteThreshold();
        long sizeThresholdBytes = request.sizeThresholdBytes() == ShardSplitCandidatesRequest.USE_DEFAULT_SIZE_THRESHOLD_BYTES
            ? plugin.reshardingSplitCandidateSizeThresholdBytes()
            : request.sizeThresholdBytes();
        return new ShardSplitCandidatesResponse(
            clusterService.getClusterName(),
            responses,
            failures,
            writesPerMinuteThreshold,
            sizeThresholdBytes,
            clusterService.state().metadata()
        );
    }

    @Override
    protected NodeRequest newNodeRequest(ShardSplitCandidatesRequest request) {
        return new NodeRequest();
    }

    @Override
    protected NodeShardSplitCandidatesResponse newNodeResponse(StreamInput in) throws IOException {
        return new NodeShardSplitCandidatesResponse(in);
    }

    /**
     * @param request unused -- this action takes no per-node parameters.
     * @return this node's raw writes-per-minute snapshot, unfiltered and unevaluated.
     */
    @Override
    protected NodeShardSplitCandidatesResponse nodeOperation(NodeRequest request) {
        Map<String, Long> wpmSnapshot = plugin.shardActivityRegistry().snapshotWritesPerMinute();
        Map<String, Long> sizeSnapshot = plugin.shardActivityRegistry().snapshotShardSizes();
        List<ShardWriteRateEntry> entries = new ArrayList<>(wpmSnapshot.size());
        for (Map.Entry<String, Long> entry : wpmSnapshot.entrySet()) {
            int separator = entry.getKey().lastIndexOf('/');
            String indexUuid = entry.getKey().substring(0, separator);
            int shardId = Integer.parseInt(entry.getKey().substring(separator + 1));
            long shardSizeInBytes = sizeSnapshot.getOrDefault(entry.getKey(), 0L);
            entries.add(new ShardWriteRateEntry(indexUuid, shardId, entry.getValue(), shardSizeInBytes));
        }
        return new NodeShardSplitCandidatesResponse(clusterService.localNode(), entries);
    }

    /**
     * Inner node request; carries no fields of its own since {@link #nodeOperation} needs nothing
     * beyond the receiving node's own local registry.
     */
    public static class NodeRequest extends TransportRequest {

        /**
         * Deserializes an (empty) node request.
         *
         * @param in stream positioned at a previously-{@link #writeTo}-written (empty) {@link NodeRequest}.
         */
        public NodeRequest(StreamInput in) throws IOException {
            super(in);
        }

        /** Creates an (empty) node request. */
        NodeRequest() {}

        /** @param out stream to write this (empty) request to. */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }
}
