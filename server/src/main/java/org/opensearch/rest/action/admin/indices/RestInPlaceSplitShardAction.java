/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.split.InPlaceSplitShardAction;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST action for {@link InPlaceSplitShardAction}: operator-triggered in-place shard split.
 * Deliberately operator-only, no invented auto-triggering
 * policy -- matches this fork's own established discipline for early-phase resharding actions (see
 * {@code TransportOrchestrateShardSplitAction}'s own javadoc for the same "operator always supplies
 * the split" rule applied to the separate, older split mechanism).
 *
 * @opensearch.experimental
 */
public class RestInPlaceSplitShardAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_split_in_place/{shard_id}"));
    }

    @Override
    public String getName() {
        return "in_place_split_shard_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String index = request.param("index");
        // paramAsInt rather than a bare Integer.parseInt so a non-numeric shard id surfaces as a 400
        // (IllegalArgumentException) instead of an unhandled NumberFormatException 500. The path param is
        // required by the route, so the default is unreachable; -1 fails request validation if it ever isn't.
        int shardId = request.paramAsInt("shard_id", -1);
        int splitInto = request.paramAsInt("split_into", 2);

        InPlaceSplitShardAction.Request splitRequest = new InPlaceSplitShardAction.Request(index, shardId, splitInto);
        splitRequest.timeout(request.paramAsTime("timeout", splitRequest.ackTimeout()));
        splitRequest.clusterManagerNodeTimeout(request.paramAsTime("cluster_manager_timeout", splitRequest.clusterManagerNodeTimeout()));

        return channel -> client.execute(InPlaceSplitShardAction.INSTANCE, splitRequest, new RestToXContentListener<>(channel));
    }
}
