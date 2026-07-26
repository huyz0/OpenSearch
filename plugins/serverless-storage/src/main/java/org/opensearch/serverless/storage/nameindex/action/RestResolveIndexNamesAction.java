/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex.action;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler.ServerlessScope;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Resolves index expressions through the name index rather than through cluster state.
 *
 * <p>Exists mainly so the tier is observable: comparing this against what core resolves for the same
 * expression is how the switchover gets validated, and a resolver that cannot be queried directly can
 * only be compared by inference.
 */
public class RestResolveIndexNamesAction extends BaseRestHandler {

    @Override
    public ServerlessScope serverlessScope() {
        return ServerlessScope.AVAILABLE;
    }

    @Override
    public String getName() {
        return "serverless_storage_resolve_index_names";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_plugins/_serverless/storage/name_index/_resolve"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String[] expressions = request.paramAsStringArrayOrEmptyIfAll("index");
        IndicesOptions options = IndicesOptions.fromRequest(request, IndicesOptions.strictExpandOpen());
        return channel -> client.execute(
            ResolveIndexNamesAction.INSTANCE,
            new ResolveIndexNamesRequest(options, expressions),
            new RestToXContentListener<>(channel)
        );
    }
}
