/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.nodecapacity.NodeWarmupCoordinator;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * The actual work behind {@link NodeWarmupAction}: resolves the requested node id to its current
 * name, then marks or clears warming. Mirrors {@link TransportNodeDrainAction}'s shape -- not
 * routed through a cluster-manager-only base class, since {@link NodeWarmupCoordinator} dispatches
 * a {@code ClusterUpdateSettingsRequest}, which core already forwards to the cluster-manager itself.
 */
public class TransportNodeWarmupAction extends HandledTransportAction<NodeWarmupRequest, AcknowledgedResponse> {

    private final ClusterService clusterService;
    private final NodeWarmupCoordinator warmupCoordinator;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param clusterService resolves the requested node id to its current name.
     * @param client used to construct this node's {@link NodeWarmupCoordinator}.
     */
    @Inject
    public TransportNodeWarmupAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client
    ) {
        super(NodeWarmupAction.NAME, transportService, actionFilters, NodeWarmupRequest::new);
        this.clusterService = clusterService;
        this.warmupCoordinator = new NodeWarmupCoordinator(client);
    }

    @Override
    protected void doExecute(Task task, NodeWarmupRequest request, ActionListener<AcknowledgedResponse> listener) {
        ClusterState state = clusterService.state();
        DiscoveryNode node = state.nodes().get(request.nodeId());
        if (node == null) {
            listener.onFailure(new IllegalArgumentException("no such node: " + request.nodeId()));
            return;
        }
        ActionListener<org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse> ackListener = ActionListener.wrap(
            response -> listener.onResponse(new AcknowledgedResponse(true)),
            listener::onFailure
        );
        if (request.warming()) {
            warmupCoordinator.markWarming(state, node.getName(), ackListener);
        } else {
            warmupCoordinator.clearWarming(state, node.getName(), ackListener);
        }
    }
}
