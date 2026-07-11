/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler.ServerlessScope;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * {@code POST /_plugins/_serverless/storage/_partition_rewrite} with a body of the shape {@code
 * {"index_uuid": "...", "shard_id": 0}} -- the REST surface for {@link ShardPartitionRewriteAction}
 * (rfc-serverless-opensearch.md &sect;16 Phase 5). Triggers one immediate attempt to physically
 * rewrite a split target's bundle down to just its own partition.
 */
public class RestShardPartitionRewriteAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestShardPartitionRewriteAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_shard_partition_rewrite";
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

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_plugins/_serverless/storage/_partition_rewrite"));
    }

    /**
     * @param request the incoming REST request, whose body names the shard to rewrite.
     * @param client used to dispatch the parsed {@link ShardPartitionRewriteRequest} locally.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        Map<String, Object> body;
        try (XContentParser parser = request.contentParser()) {
            body = parser.map();
        }
        Object indexUuid = body.get("index_uuid");
        Object shardId = body.get("shard_id");
        if (!(indexUuid instanceof String) || !(shardId instanceof Number)) {
            throw new IllegalArgumentException("request body must contain a string \"index_uuid\" and a numeric \"shard_id\"");
        }
        ShardPartitionRewriteRequest rewriteRequest = new ShardPartitionRewriteRequest((String) indexUuid, ((Number) shardId).intValue());
        return channel -> client.executeLocally(
            ShardPartitionRewriteAction.INSTANCE,
            rewriteRequest,
            new RestToXContentListener<>(channel)
        );
    }
}
