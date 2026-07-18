/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler.ServerlessScope;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * {@code POST /_plugins/_serverless/storage/_resharding/_fence_split_source/{source}?superseding_alias=alias}
 * -- the REST surface for {@link FenceSplitSourceAction}. Query parameter, not a request body, same
 * shape {@code RestCutoverSplitRoutingAction}'s own alias parameter uses.
 */
public class RestFenceSplitSourceAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestFenceSplitSourceAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_fence_split_source";
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

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_plugins/_serverless/storage/_resharding/_fence_split_source/{source}"));
    }

    /**
     * @param request the incoming REST request, naming the source index via a path parameter and
     *                the superseding alias via a query parameter.
     * @param client used to dispatch the parsed {@link FenceSplitSourceRequest}.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String sourceIndexName = request.param("source");
        String supersedingAliasName = request.param("superseding_alias");
        FenceSplitSourceRequest fenceRequest = new FenceSplitSourceRequest(sourceIndexName, supersedingAliasName);
        return channel -> client.execute(FenceSplitSourceAction.INSTANCE, fenceRequest, new RestToXContentListener<>(channel));
    }
}
