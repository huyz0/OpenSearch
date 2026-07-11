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
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * {@code GET /_plugins/_serverless/storage/_resharding/split_candidates} -- the REST surface for
 * {@link ShardSplitCandidatesAction}. Optional query parameter {@code writes_per_minute_threshold}
 * (a non-negative integer) overrides this node's configured default for one request, mirroring
 * {@code org.opensearch.serverless.storage.scaleup.action.RestScaleUpCandidatesAction}'s own override.
 */
public class RestShardSplitCandidatesAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestShardSplitCandidatesAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_resharding_split_candidates";
    }

    /**
     * Every action this plugin exposes is meaningful only when serverless storage is opted into
     * for the target index, so every one of its REST handlers declares itself available under
     * serverless mode -- this one reads only the plugin's own node-shared registry plus cluster
     * metadata, never local-disk shard state.
     */
    @Override
    public ServerlessScope serverlessScope() {
        return ServerlessScope.AVAILABLE;
    }

    /** The single route this handler serves. */
    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_plugins/_serverless/storage/_resharding/split_candidates"));
    }

    /**
     * @param request the incoming REST request; may carry a {@code writes_per_minute_threshold} override.
     * @param client used to dispatch the parsed {@link ShardSplitCandidatesRequest} across the cluster.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        long writesPerMinuteThreshold = request.paramAsLong(
            "writes_per_minute_threshold",
            ShardSplitCandidatesRequest.USE_DEFAULT_WPM_THRESHOLD
        );
        ShardSplitCandidatesRequest splitCandidatesRequest = new ShardSplitCandidatesRequest(writesPerMinuteThreshold);
        return channel -> client.execute(
            ShardSplitCandidatesAction.INSTANCE,
            splitCandidatesRequest,
            new RestToXContentListener<>(channel)
        );
    }
}
