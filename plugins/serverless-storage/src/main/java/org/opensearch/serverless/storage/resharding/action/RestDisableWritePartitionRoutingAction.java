/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler.ApiAvailabilityScope;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * {@code POST /_plugins/_serverless/storage/_resharding/_disable_write_routing?target_indices=a,b,c}
 * -- the REST surface for {@link DisableWritePartitionRoutingAction}. Query parameter, not a request
 * body, same shape {@code RestEnableWritePartitionRoutingAction} already uses.
 */
public class RestDisableWritePartitionRoutingAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestDisableWritePartitionRoutingAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_disable_write_partition_routing";
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
        return singletonList(new Route(POST, "/_plugins/_serverless/storage/_resharding/_disable_write_routing"));
    }

    /**
     * @param request the incoming REST request, naming the target indices via a query parameter.
     * @param client used to dispatch the parsed {@link DisableWritePartitionRoutingRequest}.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] targetIndices = Strings.splitStringByCommaToArray(request.param("target_indices"));
        DisableWritePartitionRoutingRequest disableRequest = new DisableWritePartitionRoutingRequest(Arrays.asList(targetIndices));
        return channel -> client.execute(
            DisableWritePartitionRoutingAction.INSTANCE,
            disableRequest,
            new RestToXContentListener<>(channel)
        );
    }
}
