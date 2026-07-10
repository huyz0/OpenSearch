/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero.action;

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
 * {@code GET /_plugins/_serverless/storage/_scale_to_zero/candidates} -- the REST surface for
 * {@link ScaleToZeroCandidatesAction}. Optional query parameters {@code idle_threshold} (a {@link
 * org.opensearch.common.unit.TimeValue}-parseable string, e.g. {@code "5m"}) and {@code
 * lag_threshold} (a non-negative integer) override this node's configured defaults for one
 * request, matching how {@link ScaleToZeroCandidatesRequest}'s own overrides work.
 */
public class RestScaleToZeroCandidatesAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestScaleToZeroCandidatesAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_scale_to_zero_candidates";
    }

    /**
     * Every action this plugin exposes is meaningful only when serverless storage is opted into
     * for the target index, so every one of its REST handlers declares itself available under
     * serverless mode (rfc-serverless-opensearch.md &sect;11) -- this one reads only the plugin's
     * own node-shared registries, never local-disk shard state.
     */
    @Override
    public ServerlessScope serverlessScope() {
        return ServerlessScope.AVAILABLE;
    }

    /** The single route this handler serves. */
    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_plugins/_serverless/storage/_scale_to_zero/candidates"));
    }

    /**
     * @param request the incoming REST request; may carry {@code idle_threshold}/{@code lag_threshold} overrides.
     * @param client used to dispatch the parsed {@link ScaleToZeroCandidatesRequest} across the cluster.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        long idleThresholdMillis = request.paramAsTime(
            "idle_threshold",
            org.opensearch.common.unit.TimeValue.timeValueMillis(ScaleToZeroCandidatesRequest.USE_DEFAULT_IDLE_THRESHOLD)
        ).millis();
        long lagThreshold = request.paramAsLong("lag_threshold", ScaleToZeroCandidatesRequest.USE_DEFAULT_LAG_THRESHOLD);
        ScaleToZeroCandidatesRequest scaleToZeroRequest = new ScaleToZeroCandidatesRequest(idleThresholdMillis, lagThreshold);
        return channel -> client.execute(ScaleToZeroCandidatesAction.INSTANCE, scaleToZeroRequest, new RestToXContentListener<>(channel));
    }
}
