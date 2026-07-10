/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.action;

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
 * {@code GET /_plugins/_serverless/storage/_manifest_lag} -- the REST surface for {@link
 * NodeManifestLagAction}. No path or body parameters: lists every reader shard's
 * manifest-generation lag tracked on whichever node answers the request, same "answers only from
 * the receiving node" contract as {@code
 * org.opensearch.serverless.storage.writerengine.action.RestNodeIdleShardsAction}.
 */
public class RestNodeManifestLagAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestNodeManifestLagAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_node_manifest_lag";
    }

    /**
     * Every action this plugin exposes is meaningful only when serverless storage is opted into
     * for the target index, so every one of its REST handlers declares itself available under
     * serverless mode (rfc-serverless-opensearch.md &sect;11) -- unlike, say, {@code _forcemerge}
     * or shard-store APIs, nothing here assumes local-disk shard state that disaggregated storage
     * invalidates.
     */
    @Override
    public ServerlessScope serverlessScope() {
        return ServerlessScope.AVAILABLE;
    }

    /** The single route this handler serves. */
    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_plugins/_serverless/storage/_manifest_lag"));
    }

    /**
     * @param request the incoming REST request; unused beyond routing, this action takes no parameters.
     * @param client used to dispatch the parsed {@link NodeManifestLagRequest} locally.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return channel -> client.executeLocally(
            NodeManifestLagAction.INSTANCE,
            new NodeManifestLagRequest(),
            new RestToXContentListener<>(channel)
        );
    }
}
