/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine.action;

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
 * {@code GET /_plugins/_serverless/storage/{index_uuid}/{shard_id}/_idle_time} -- the REST surface
 * for {@link ShardIdleTimeAction} (rfc-serverless-opensearch.md &sect;16 Phase 4). Path parameters,
 * not a request body, since this is a pure read with no other configuration -- matching how a
 * {@code GET} with only identifying path segments is expected to look, unlike {@code
 * RestCompactionTriggerAction}'s {@code POST} (which triggers a side effect and so takes a body by
 * this codebase's own convention for that shape).
 *
 * <p>Dispatched to whichever node the request lands on, exactly like {@link
 * org.opensearch.serverless.storage.compaction.action.RestCompactionTriggerAction} -- but unlike
 * that action, this one can only ever answer meaningfully from the node actually hosting the
 * shard's writer engine (see {@link TransportShardIdleTimeAction}'s own javadoc); a request that
 * lands elsewhere gets {@link ShardIdleTimeResponse#notTracked()} back, not an error, since {@code
 * client.executeLocally} has no cross-node routing of its own to fall back on.
 */
public class RestShardIdleTimeAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestShardIdleTimeAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_shard_idle_time";
    }

    /** The single route this handler serves. */
    /**
     * Every action this plugin exposes is meaningful only when serverless storage is opted into
     * for the target index, so every one of its REST handlers declares itself available under
     * serverless mode (rfc-serverless-opensearch.md &sect;11) -- unlike, say, {@code _forcemerge}
     * or shard-store APIs, nothing here assumes local-disk shard state that disaggregated storage
     * invalidates.
     */
    @Override
    public ApiAvailabilityScope apiAvailabilityScope() {
        return ApiAvailabilityScope.AVAILABLE;
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_plugins/_serverless/storage/{index_uuid}/{shard_id}/_idle_time"));
    }

    /**
     * @param request the incoming REST request, whose path names the shard to query.
     * @param client used to dispatch the parsed {@link ShardIdleTimeRequest} locally.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String indexUuid = request.param("index_uuid");
        int shardId = Integer.parseInt(request.param("shard_id"));
        ShardIdleTimeRequest idleTimeRequest = new ShardIdleTimeRequest(indexUuid, shardId);
        return channel -> client.executeLocally(ShardIdleTimeAction.INSTANCE, idleTimeRequest, new RestToXContentListener<>(channel));
    }
}
