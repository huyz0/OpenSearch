/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security.action;

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
 * {@code GET /_plugins/_serverless/storage/_object_store_request_stats} -- the REST surface for
 * {@link NodeObjectStoreRequestStatsAction}. No path or body parameters: reports the real
 * object-store request counts tallied on whichever node answers the request, same "answers only
 * from the receiving node" contract as {@code RestNodeCacheStatsAction}.
 */
public class RestNodeObjectStoreRequestStatsAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestNodeObjectStoreRequestStatsAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_node_object_store_request_stats";
    }

    /**
     * Every action this plugin exposes is meaningful only when serverless storage is opted into
     * for the target index, so every one of its REST handlers declares itself available under
     * serverless mode (rfc-serverless-opensearch.md &sect;11).
     */
    @Override
    public ServerlessScope serverlessScope() {
        return ServerlessScope.AVAILABLE;
    }

    /** The single route this handler serves. */
    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_plugins/_serverless/storage/_object_store_request_stats"));
    }

    /**
     * @param request the incoming REST request; unused beyond routing, this action takes no parameters.
     * @param client used to dispatch the parsed {@link NodeObjectStoreRequestStatsRequest} locally.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return channel -> client.executeLocally(
            NodeObjectStoreRequestStatsAction.INSTANCE,
            new NodeObjectStoreRequestStatsRequest(),
            new RestToXContentListener<>(channel)
        );
    }
}
