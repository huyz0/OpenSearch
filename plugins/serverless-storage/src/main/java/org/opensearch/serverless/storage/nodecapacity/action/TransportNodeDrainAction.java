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
import org.opensearch.serverless.storage.nodecapacity.DrainCoordinator;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * The actual work behind {@link NodeDrainAction}: resolves the requested node id to its current
 * name (core's exclude filter matches on name, not id -- see {@link DrainCoordinator}), then starts
 * or cancels a drain. Not routed through a cluster-manager-only base class -- {@link
 * DrainCoordinator} dispatches a {@code ClusterUpdateSettingsRequest}, which core already forwards
 * to the cluster-manager itself, so a second layer of forwarding here would be redundant.
 */
public class TransportNodeDrainAction extends HandledTransportAction<NodeDrainRequest, AcknowledgedResponse> {

    private final ClusterService clusterService;
    private final DrainCoordinator drainCoordinator;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param clusterService resolves the requested node id to its current name.
     * @param client used to construct this node's {@link DrainCoordinator}.
     */
    @Inject
    public TransportNodeDrainAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client
    ) {
        super(NodeDrainAction.NAME, transportService, actionFilters, NodeDrainRequest::new);
        this.clusterService = clusterService;
        this.drainCoordinator = new DrainCoordinator(client);
    }

    @Override
    protected void doExecute(Task task, NodeDrainRequest request, ActionListener<AcknowledgedResponse> listener) {
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
        if (request.drain()) {
            drainCoordinator.drain(state, node.getName(), ackListener);
        } else {
            drainCoordinator.cancelDrain(state, node.getName(), ackListener);
        }
    }
}
