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
import org.opensearch.rest.RestHandler.ApiAvailabilityScope;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * {@code POST /_plugins/_serverless/storage/_shrink} with a body of the shape {@code
 * {"sources": [{"index_uuid": "...", "shard_id": 0}, {"index_uuid": "...", "shard_id": 0}],
 * "target": {"index_uuid": "...", "shard_id": 0}}} -- the REST surface for {@link ShardShrinkAction}
 * (rfc-serverless-opensearch.md &sect;16 Phase 5).
 */
public class RestShardShrinkAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestShardShrinkAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_shard_shrink";
    }

    /**
     * Every action this plugin exposes is meaningful only when serverless storage is opted into
     * for the target index, so every one of its REST handlers declares itself available under
     * serverless mode (rfc-serverless-opensearch.md &sect;11).
     */
    @Override
    public ApiAvailabilityScope apiAvailabilityScope() {
        return ApiAvailabilityScope.AVAILABLE;
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_plugins/_serverless/storage/_shrink"));
    }

    /**
     * @param request the incoming REST request, whose body names every source shard and the target.
     * @param client used to dispatch the parsed {@link ShardShrinkRequest} locally.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        Map<String, Object> body;
        try (XContentParser parser = request.contentParser()) {
            body = parser.map();
        }
        Object rawSources = body.get("sources");
        if (!(rawSources instanceof List)) {
            throw new IllegalArgumentException("request body must contain a \"sources\" array");
        }
        List<ShardRef> sources = new ArrayList<>();
        for (Object rawSource : (List<Object>) rawSources) {
            if (!(rawSource instanceof Map)) {
                throw new IllegalArgumentException("every entry in \"sources\" must be an object");
            }
            sources.add(shardRefFromMap((Map<String, Object>) rawSource, "sources[]"));
        }
        Object rawTarget = body.get("target");
        if (!(rawTarget instanceof Map)) {
            throw new IllegalArgumentException("request body must contain a \"target\" object");
        }
        ShardRef target = shardRefFromMap((Map<String, Object>) rawTarget, "target");
        ShardShrinkRequest shrinkRequest = new ShardShrinkRequest(sources, target.indexUuid(), target.shardId());
        return channel -> client.executeLocally(ShardShrinkAction.INSTANCE, shrinkRequest, new RestToXContentListener<>(channel));
    }

    private static ShardRef shardRefFromMap(Map<String, Object> shard, String key) {
        Object indexUuid = shard.get("index_uuid");
        Object shardId = shard.get("shard_id");
        if (!(indexUuid instanceof String) || !(shardId instanceof Number)) {
            throw new IllegalArgumentException("\"" + key + "\" must contain a string \"index_uuid\" and a numeric \"shard_id\"");
        }
        return new ShardRef((String) indexUuid, ((Number) shardId).intValue());
    }
}
