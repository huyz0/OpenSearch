/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.serverless.storage.nodecapacity.NodeWarmupCoordinator;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * The actual work behind {@link NodeWarmupAction}: resolves the requested node id to its current
 * name, then marks or clears warming. Always executed on the elected cluster-manager node -- see
 * {@link NodeWarmupRequest}'s own javadoc for why that indirection is required ({@link
 * NodeWarmupCoordinator} mutates cluster state via a plain {@code
 * ClusterService#submitStateUpdateTask} call, which throws {@code NotClusterManagerException} when
 * invoked from a node that isn't currently the cluster-manager).
 */
public class TransportNodeWarmupAction extends TransportClusterManagerNodeAction<NodeWarmupRequest, AcknowledgedResponse> {

    private final NodeWarmupCoordinator warmupCoordinator;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link TransportClusterManagerNodeAction} to register this action.
     * @param clusterService resolves the requested node id to its current name, and constructs this
     *                       node's {@link NodeWarmupCoordinator}.
     * @param threadPool used by {@link TransportClusterManagerNodeAction}'s own base machinery.
     * @param actionFilters applied by {@link TransportClusterManagerNodeAction} around every request.
     * @param indexNameExpressionResolver required by {@link TransportClusterManagerNodeAction}'s constructor, unused here.
     */
    @Inject
    public TransportNodeWarmupAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            NodeWarmupAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            NodeWarmupRequest::new,
            indexNameExpressionResolver
        );
        this.warmupCoordinator = new NodeWarmupCoordinator(clusterService);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(NodeWarmupRequest request, ClusterState state) {
        return null;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void clusterManagerOperation(NodeWarmupRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        DiscoveryNode node = state.nodes().get(request.nodeId());
        if (node == null) {
            listener.onFailure(new IllegalArgumentException("no such node: " + request.nodeId()));
            return;
        }
        ActionListener<Void> ackListener = ActionListener.wrap(response -> listener.onResponse(new AcknowledgedResponse(true)), listener::onFailure);
        if (request.warming()) {
            warmupCoordinator.markWarming(node.getName(), ackListener);
        } else {
            warmupCoordinator.clearWarming(node.getName(), ackListener);
        }
    }
}
