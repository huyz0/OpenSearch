/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity.action;

import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler.ApiAvailabilityScope;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.DELETE;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * {@code POST/DELETE /_plugins/_serverless/storage/nodes/{node_id}/warming} -- the REST surface for
 * {@link NodeWarmupAction} (node autoscaling design doc part 2, "pre-warm before rotation").
 * POST marks a node warming, DELETE clears it; both are idempotent.
 */
public class RestNodeWarmupAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestNodeWarmupAction() {}

    @Override
    public String getName() {
        return "serverless_storage_node_warmup";
    }

    @Override
    public ApiAvailabilityScope apiAvailabilityScope() {
        return ApiAvailabilityScope.AVAILABLE;
    }

    /** The two routes this handler serves. */
    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_plugins/_serverless/storage/nodes/{node_id}/warming"),
            new Route(DELETE, "/_plugins/_serverless/storage/nodes/{node_id}/warming")
        );
    }

    /**
     * @param request the incoming REST request; carries {@code node_id} as a path parameter.
     * @param client used to dispatch the parsed {@link NodeWarmupRequest}.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String nodeId = request.param("node_id");
        boolean warming = request.method() == POST;
        NodeWarmupRequest warmupRequest = new NodeWarmupRequest(nodeId, warming);
        return channel -> client.execute(NodeWarmupAction.INSTANCE, warmupRequest, new RestToXContentListener<>(channel));
    }
}
