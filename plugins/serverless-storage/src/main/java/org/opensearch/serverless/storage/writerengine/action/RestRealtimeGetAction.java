/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine.action;

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
 * {@code GET /_plugins/_serverless/storage/{index_uuid}/{shard_id}/_realtime_get/{id}} -- the REST surface
 * for {@link RealtimeGetAction}, rfc-serverless-opensearch.md &sect;8's "{@code _get} by document
 * id can optionally route to the writer shard for true realtime gets, controlled per request."
 * Must be sent directly to a node already known (from the routing table) to hold the shard's
 * writer engine -- same "answers only from the receiving node" contract as {@code
 * RestShardIdleTimeAction}.
 */
public class RestRealtimeGetAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestRealtimeGetAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_realtime_get";
    }

    /**
     * Every action this plugin exposes is meaningful only when serverless storage is opted into
     * for the target index, so every one of its REST handlers declares itself available under
     * serverless mode (rfc-serverless-opensearch.md &sect;11).
     */
    @Override
    public ServerlessScope serverlessScope() {
        return ServerlessScope.AVAILABLE;
    }

    /** The single route this handler serves. */
    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_plugins/_serverless/storage/{index_uuid}/{shard_id}/_realtime_get/{id}"));
    }

    /**
     * @param request path params {@code index_uuid} (the index UUID), {@code shard_id} (the shard number), and {@code id} (the document id).
     * @param client used to dispatch the parsed {@link RealtimeGetRequest} locally.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String indexUuid = request.param("index_uuid");
        int shardId = Integer.parseInt(request.param("shard_id"));
        String id = request.param("id");
        RealtimeGetRequest getRequest = new RealtimeGetRequest(indexUuid, shardId, id);
        return channel -> client.executeLocally(RealtimeGetAction.INSTANCE, getRequest, new RestToXContentListener<>(channel));
    }
}
