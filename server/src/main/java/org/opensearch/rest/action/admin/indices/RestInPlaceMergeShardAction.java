/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.split.InPlaceMergeShardAction;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST action for {@link InPlaceMergeShardAction}: operator-triggered in-place shard merge
 * -- the reverse of {@link RestInPlaceSplitShardAction}.
 * Deliberately operator-only, no invented auto-triggering policy, matching the split action's own
 * discipline.
 *
 * @opensearch.experimental
 */
public class RestInPlaceMergeShardAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_merge_in_place/{parent_shard_id}"));
    }

    @Override
    public String getName() {
        return "in_place_merge_shard_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String index = request.param("index");
        // paramAsInt rather than a bare Integer.parseInt so a non-numeric parent shard id surfaces as a 400
        // (IllegalArgumentException) instead of an unhandled NumberFormatException 500. The path param is
        // required by the route, so the default is unreachable; -1 fails request validation if it ever isn't.
        int parentShardId = request.paramAsInt("parent_shard_id", -1);

        InPlaceMergeShardAction.Request mergeRequest = new InPlaceMergeShardAction.Request(index, parentShardId);
        mergeRequest.timeout(request.paramAsTime("timeout", mergeRequest.ackTimeout()));
        mergeRequest.clusterManagerNodeTimeout(request.paramAsTime("cluster_manager_timeout", mergeRequest.clusterManagerNodeTimeout()));

        return channel -> client.execute(InPlaceMergeShardAction.INSTANCE, mergeRequest, new RestToXContentListener<>(channel));
    }
}
