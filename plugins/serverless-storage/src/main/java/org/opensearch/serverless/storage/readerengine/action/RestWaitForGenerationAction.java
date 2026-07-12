/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.action;

import org.opensearch.common.unit.TimeValue;
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
 * {@code GET /_plugins/_serverless/storage/{index}/{shard}/_wait_for_generation} -- the REST
 * surface for {@link WaitForGenerationAction}, rfc-serverless-opensearch.md &sect;8's
 * read-after-write mechanism. Must be sent directly to a node already known (from the routing
 * table) to hold the reader shard copy being waited on -- same "answers only from the receiving
 * node" contract as {@code RestNodeManifestLagAction}, since this blocks on that specific node's
 * own local reader engine instance, not a cluster-wide view.
 */
public class RestWaitForGenerationAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestWaitForGenerationAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_wait_for_generation";
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
        return singletonList(new Route(GET, "/_plugins/_serverless/storage/{index}/{shard}/_wait_for_generation"));
    }

    /**
     * @param request path params {@code index} (the index UUID) and {@code shard} (the shard
     *                number), and query params {@code min_generation} (required) and {@code
     *                timeout} (defaults to {@code 30s}, matching most other blocking OpenSearch
     *                REST APIs' own default).
     * @param client used to dispatch the parsed {@link WaitForGenerationRequest} locally.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String indexUuid = request.param("index");
        int shardId = Integer.parseInt(request.param("shard"));
        long minGeneration = Long.parseLong(request.param("min_generation"));
        TimeValue timeout = request.paramAsTime("timeout", TimeValue.timeValueSeconds(30));
        WaitForGenerationRequest waitRequest = new WaitForGenerationRequest(indexUuid, shardId, minGeneration, timeout);
        return channel -> client.executeLocally(WaitForGenerationAction.INSTANCE, waitRequest, new RestToXContentListener<>(channel));
    }
}
