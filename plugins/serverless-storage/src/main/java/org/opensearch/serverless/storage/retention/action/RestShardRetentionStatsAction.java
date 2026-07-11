/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

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
 * {@code GET /_plugins/_serverless/storage/{index_uuid}/{shard_id}/_retention_stats} -- the REST
 * surface for {@link ShardRetentionStatsAction}. Read-only: reports manifest/bundle/pin counts and
 * this node's configured retention windows without deleting anything.
 */
public class RestShardRetentionStatsAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestShardRetentionStatsAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_shard_retention_stats";
    }

    /**
     * Every action this plugin exposes is meaningful only when serverless storage is opted into
     * for the target index, so every one of its REST handlers declares itself available under
     * serverless mode (rfc-serverless-opensearch.md &sect;11) -- this one reads only the shared
     * object store via {@code BlobContainer}s, never local-disk shard state.
     */
    @Override
    public ServerlessScope serverlessScope() {
        return ServerlessScope.AVAILABLE;
    }

    /** The single route this handler serves. */
    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_plugins/_serverless/storage/{index_uuid}/{shard_id}/_retention_stats"));
    }

    /**
     * @param request the incoming REST request; {@code index_uuid}/{@code shard_id} path parameters name the shard.
     * @param client used to dispatch the parsed {@link ShardRetentionStatsRequest} locally.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String indexUuid = request.param("index_uuid");
        int shardId = Integer.parseInt(request.param("shard_id"));
        ShardRetentionStatsRequest statsRequest = new ShardRetentionStatsRequest(indexUuid, shardId);
        return channel -> client.executeLocally(ShardRetentionStatsAction.INSTANCE, statsRequest, new RestToXContentListener<>(channel));
    }
}
