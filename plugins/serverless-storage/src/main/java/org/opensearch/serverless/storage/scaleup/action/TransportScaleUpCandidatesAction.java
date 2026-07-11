/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaleup.action;

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
 * The actual work behind {@link ScaleUpCandidatesAction}: fans {@link NodeRequest} out to every
 * data node (each node answering with its own {@code ReaderShardActivityRegistry#snapshotQueriesPerMinute()}),
 * then lets {@link ScaleUpCandidatesResponse}'s own constructor merge, join with cluster metadata,
 * and threshold-evaluate the results -- mirrors {@code
 * org.opensearch.serverless.storage.scaletozero.action.TransportScaleToZeroCandidatesAction}.
 *
 * <p>No I/O on the node-local side, same "safe to answer directly" reasoning as that action --
 * {@link ThreadPool.Names#SAME} is used for both the per-node operation and final response
 * assembly.
 */
public class TransportScaleUpCandidatesAction extends TransportNodesAction<
    ScaleUpCandidatesRequest,
    ScaleUpCandidatesResponse,
    TransportScaleUpCandidatesAction.NodeRequest,
    NodeScaleUpCandidatesResponse> {

    private final ServerlessStoragePlugin plugin;

    /**
     * Creates the transport action.
     *
     * @param threadPool used by {@link TransportNodesAction} to dispatch the fan-out.
     * @param clusterService resolves the cluster's data nodes/name, and (in {@link #newResponse})
     *                        the current cluster state's index metadata the merge needs.
     * @param transportService used by {@link TransportNodesAction} to register this action.
     * @param actionFilters applied by {@link TransportNodesAction} around every request.
     * @param plugin resolves this node's {@link ServerlessStoragePlugin#readerShardActivityRegistry()}
     *               and this node's configured default thresholds.
     */
    @Inject
    public TransportScaleUpCandidatesAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin
    ) {
        super(
            ScaleUpCandidatesAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ScaleUpCandidatesRequest::new,
            NodeRequest::new,
            ThreadPool.Names.SAME,
            NodeScaleUpCandidatesResponse.class
        );
        this.plugin = plugin;
    }

    /**
     * @param request the original cluster-wide request, carrying any caller-supplied threshold overrides.
     * @param responses every node's raw {@link NodeScaleUpCandidatesResponse}.
     * @param failures any per-node failures encountered while fanning out.
     * @return the merged, threshold-evaluated {@link ScaleUpCandidatesResponse}.
     */
    @Override
    protected ScaleUpCandidatesResponse newResponse(
        ScaleUpCandidatesRequest request,
        List<NodeScaleUpCandidatesResponse> responses,
        List<FailedNodeException> failures
    ) {
        long qpmThreshold = request.qpmThreshold() == ScaleUpCandidatesRequest.USE_DEFAULT_QPM_THRESHOLD
            ? plugin.scaleUpQpmThreshold()
            : request.qpmThreshold();
        int maxSearchReplicas = request.maxSearchReplicas() == ScaleUpCandidatesRequest.USE_DEFAULT_MAX_SEARCH_REPLICAS
            ? plugin.scaleUpMaxSearchReplicas()
            : request.maxSearchReplicas();
        return new ScaleUpCandidatesResponse(
            clusterService.getClusterName(),
            responses,
            failures,
            qpmThreshold,
            maxSearchReplicas,
            clusterService.state().metadata()
        );
    }

    @Override
    protected NodeRequest newNodeRequest(ScaleUpCandidatesRequest request) {
        return new NodeRequest();
    }

    @Override
    protected NodeScaleUpCandidatesResponse newNodeResponse(StreamInput in) throws IOException {
        return new NodeScaleUpCandidatesResponse(in);
    }

    /**
     * @param request unused -- this action takes no per-node parameters.
     * @return this node's raw queries-per-minute snapshot, unfiltered and unevaluated.
     */
    @Override
    protected NodeScaleUpCandidatesResponse nodeOperation(NodeRequest request) {
        Map<String, Long> qpmSnapshot = plugin.readerShardActivityRegistry().snapshotQueriesPerMinute();
        List<ShardQueryRateEntry> entries = new ArrayList<>(qpmSnapshot.size());
        for (Map.Entry<String, Long> entry : qpmSnapshot.entrySet()) {
            int separator = entry.getKey().lastIndexOf('/');
            String indexUuid = entry.getKey().substring(0, separator);
            int shardId = Integer.parseInt(entry.getKey().substring(separator + 1));
            entries.add(new ShardQueryRateEntry(indexUuid, shardId, entry.getValue()));
        }
        return new NodeScaleUpCandidatesResponse(clusterService.localNode(), entries);
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
