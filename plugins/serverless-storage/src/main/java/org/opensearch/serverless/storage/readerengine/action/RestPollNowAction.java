/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.action;

import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler.ServerlessScope;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * {@code POST /_plugins/_serverless/storage/{index}/{shard}/_poll_now} -- the REST surface for
 * {@link PollNowAction}, rfc-serverless-opensearch.md &sect;8's publication notification
 * mechanism. Must be sent directly to a node already known to hold a reader copy of the shard --
 * same "answers only from the receiving node" contract as {@code RestWaitForGenerationAction}.
 */
public class RestPollNowAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestPollNowAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_poll_now";
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
        return singletonList(new Route(POST, "/_plugins/_serverless/storage/{index}/{shard}/_poll_now"));
    }

    /**
     * @param request path params {@code index} (the index UUID) and {@code shard} (the shard number).
     * @param client used to dispatch the parsed {@link PollNowRequest} locally.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String indexUuid = request.param("index");
        int shardId = Integer.parseInt(request.param("shard"));
        PollNowRequest pollRequest = new PollNowRequest(indexUuid, shardId);
        return channel -> client.executeLocally(PollNowAction.INSTANCE, pollRequest, new RestToXContentListener<>(channel));
    }
}
