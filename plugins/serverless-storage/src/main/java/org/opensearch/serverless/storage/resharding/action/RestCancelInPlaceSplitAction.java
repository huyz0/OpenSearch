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
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * {@code POST /_plugins/_serverless/storage/_resharding/_cancel_in_place_split/{index}/{shard}} --
 * the REST surface for {@link CancelInPlaceSplitAction}, and the only way an operator can release a
 * parent shard whose split will never complete (see that action's javadoc, finding R-11).
 */
public class RestCancelInPlaceSplitAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestCancelInPlaceSplitAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_cancel_in_place_split";
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
        return singletonList(new Route(POST, "/_plugins/_serverless/storage/_resharding/_cancel_in_place_split/{index}/{shard}"));
    }

    /**
     * @param request the incoming REST request, naming the index and parent shard via path parameters.
     * @param client used to dispatch the parsed {@link CancelInPlaceSplitRequest}.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        CancelInPlaceSplitRequest cancelRequest = new CancelInPlaceSplitRequest(request.param("index"), request.paramAsInt("shard", -1));
        return channel -> client.execute(CancelInPlaceSplitAction.INSTANCE, cancelRequest, new RestToXContentListener<>(channel));
    }
}
