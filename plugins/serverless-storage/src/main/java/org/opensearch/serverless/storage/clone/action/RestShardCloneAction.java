/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.clone.action;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * {@code POST /_plugins/_serverless/storage/_clone} with a body of the shape
 * {@code {"source": {"index_uuid": "...", "shard_id": 0}, "target": {"index_uuid": "...", "shard_id": 0}}}
 * -- the REST surface for {@link ShardCloneAction} (rfc-serverless-opensearch.md &sect;14).
 */
public class RestShardCloneAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "serverless_storage_shard_clone";
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_plugins/_serverless/storage/_clone"));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        Map<String, Object> body;
        try (XContentParser parser = request.contentParser()) {
            body = parser.map();
        }
        ShardCloneRequest cloneRequest = new ShardCloneRequest(
            shardRef(body, "source").indexUuid(),
            shardRef(body, "source").shardId(),
            shardRef(body, "target").indexUuid(),
            shardRef(body, "target").shardId()
        );
        return channel -> client.executeLocally(ShardCloneAction.INSTANCE, cloneRequest, new RestToXContentListener<>(channel));
    }

    private record ShardRef(String indexUuid, int shardId) {
    }

    @SuppressWarnings("unchecked")
    private static ShardRef shardRef(Map<String, Object> body, String key) {
        Object raw = body.get(key);
        if (!(raw instanceof Map)) {
            throw new IllegalArgumentException("request body must contain a \"" + key + "\" object");
        }
        Map<String, Object> shard = (Map<String, Object>) raw;
        Object indexUuid = shard.get("index_uuid");
        Object shardId = shard.get("shard_id");
        if (!(indexUuid instanceof String) || !(shardId instanceof Number)) {
            throw new IllegalArgumentException("\"" + key + "\" must contain a string \"index_uuid\" and a numeric \"shard_id\"");
        }
        return new ShardRef((String) indexUuid, ((Number) shardId).intValue());
    }
}
