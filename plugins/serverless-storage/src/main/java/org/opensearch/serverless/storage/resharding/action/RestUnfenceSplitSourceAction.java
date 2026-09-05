/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler.ApiAvailabilityScope;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.DELETE;

/**
 * {@code DELETE /_plugins/_serverless/storage/_resharding/_fence_split_source/{source}} -- the REST
 * surface for {@link UnfenceSplitSourceAction}. Deliberately the {@code DELETE} verb on the exact
 * same path {@link RestFenceSplitSourceAction} exposes for {@code POST}, so the escape hatch is
 * discoverable from the thing that created the state it undoes.
 */
public class RestUnfenceSplitSourceAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestUnfenceSplitSourceAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_unfence_split_source";
    }

    /**
     * Every action this plugin exposes is meaningful only when serverless storage is opted into
     * for the target index, so every one of its REST handlers declares itself available under
     * serverless mode (rfc-serverless-opensearch.md &sect;11).
     */
    @Override
    public ApiAvailabilityScope apiAvailabilityScope() {
        return ApiAvailabilityScope.AVAILABLE;
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(DELETE, "/_plugins/_serverless/storage/_resharding/_fence_split_source/{source}"));
    }

    /**
     * @param request the incoming REST request, naming the source index via a path parameter.
     * @param client used to dispatch the parsed {@link UnfenceSplitSourceRequest}.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        UnfenceSplitSourceRequest unfenceRequest = new UnfenceSplitSourceRequest(request.param("source"));
        return channel -> client.execute(UnfenceSplitSourceAction.INSTANCE, unfenceRequest, new RestToXContentListener<>(channel));
    }
}
