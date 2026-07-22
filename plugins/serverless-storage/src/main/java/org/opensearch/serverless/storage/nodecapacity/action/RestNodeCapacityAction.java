/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity.action;

import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler.ServerlessScope;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * {@code GET /_plugins/_serverless/storage/node_capacity} -- the REST surface for {@link
 * NodeCapacityAction}, meant to be polled by an external node-autoscaling control plane (node
 * autoscaling design doc part 1). Read-only and safe to poll frequently: it only ever reads the
 * cluster-manager's already-computed {@code NodeCapacitySignalService} cache.
 */
public class RestNodeCapacityAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestNodeCapacityAction() {}

    @Override
    public String getName() {
        return "serverless_storage_node_capacity";
    }

    @Override
    public ServerlessScope serverlessScope() {
        return ServerlessScope.AVAILABLE;
    }

    /** The single route this handler serves. */
    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_plugins/_serverless/storage/node_capacity"));
    }

    /**
     * @param request the incoming REST request; carries no parameters.
     * @param client used to dispatch the {@link NodeCapacityRequest}.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return channel -> client.execute(NodeCapacityAction.INSTANCE, new NodeCapacityRequest(), new RestToXContentListener<>(channel));
    }
}
