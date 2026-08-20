/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaleup.action;

import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler.ApiAvailabilityScope;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * {@code GET /_plugins/_serverless/storage/_scale_up/candidates} -- the REST surface for {@link
 * ScaleUpCandidatesAction}. Optional query parameters {@code qpm_threshold} (a non-negative
 * integer) and {@code max_search_replicas} (a non-negative integer) override this node's
 * configured defaults for one request, mirroring {@code
 * org.opensearch.serverless.storage.scaletozero.action.RestScaleToZeroCandidatesAction}'s own overrides.
 */
public class RestScaleUpCandidatesAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestScaleUpCandidatesAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_scale_up_candidates";
    }

    /**
     * Every action this plugin exposes is meaningful only when serverless storage is opted into
     * for the target index, so every one of its REST handlers declares itself available under
     * serverless mode -- this one reads only the plugin's own node-shared registry plus cluster
     * metadata, never local-disk shard state.
     */
    @Override
    public ApiAvailabilityScope apiAvailabilityScope() {
        return ApiAvailabilityScope.AVAILABLE;
    }

    /** The single route this handler serves. */
    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_plugins/_serverless/storage/_scale_up/candidates"));
    }

    /**
     * @param request the incoming REST request; may carry {@code qpm_threshold}/{@code max_search_replicas} overrides.
     * @param client used to dispatch the parsed {@link ScaleUpCandidatesRequest} across the cluster.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        long qpmThreshold = request.paramAsLong("qpm_threshold", ScaleUpCandidatesRequest.USE_DEFAULT_QPM_THRESHOLD);
        int maxSearchReplicas = request.paramAsInt("max_search_replicas", ScaleUpCandidatesRequest.USE_DEFAULT_MAX_SEARCH_REPLICAS);
        ScaleUpCandidatesRequest scaleUpRequest = new ScaleUpCandidatesRequest(qpmThreshold, maxSearchReplicas);
        return channel -> client.execute(ScaleUpCandidatesAction.INSTANCE, scaleUpRequest, new RestToXContentListener<>(channel));
    }
}
