/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.nodecapacity.NodeCapacitySignal;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * The actual work behind {@link NodeCapacityAction}: always executes on the elected cluster-manager
 * (core's {@link TransportClusterManagerNodeReadAction} retries/forwards as needed on failover), and
 * simply reads whatever {@code NodeCapacitySignalService} last computed -- no work happens inline on
 * the request path, keeping this safe to poll frequently from an external control plane.
 */
public class TransportNodeCapacityAction extends TransportClusterManagerNodeReadAction<NodeCapacityRequest, NodeCapacityResponse> {

    private final ServerlessStoragePlugin plugin;

    /**
     * Creates the transport action.
     *
     * @param threadPool used by {@link TransportClusterManagerNodeReadAction} to dispatch the operation.
     * @param clusterService resolves the elected cluster-manager node.
     * @param transportService used by {@link TransportClusterManagerNodeReadAction} to register this action.
     * @param actionFilters applied by {@link TransportClusterManagerNodeReadAction} around every request.
     * @param indexNameExpressionResolver required by the base class; unused since this action takes no index parameter.
     * @param plugin resolves this node's {@code NodeCapacitySignalService}.
     */
    @Inject
    public TransportNodeCapacityAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ServerlessStoragePlugin plugin
    ) {
        super(
            NodeCapacityAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            NodeCapacityRequest::new,
            indexNameExpressionResolver,
            false
        );
        this.plugin = plugin;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected NodeCapacityResponse read(StreamInput in) throws IOException {
        return new NodeCapacityResponse(in);
    }

    @Override
    protected void clusterManagerOperation(NodeCapacityRequest request, ClusterState state, ActionListener<NodeCapacityResponse> listener) {
        NodeCapacitySignal signal = plugin.nodeCapacitySignalService() == null
            ? NodeCapacitySignal.empty()
            : plugin.nodeCapacitySignalService().latestSignal();
        listener.onResponse(new NodeCapacityResponse(signal));
    }

    @Override
    protected ClusterBlockException checkBlock(NodeCapacityRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
