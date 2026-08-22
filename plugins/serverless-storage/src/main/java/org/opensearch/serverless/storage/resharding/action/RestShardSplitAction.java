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
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * {@code POST /_plugins/_serverless/storage/_split} with a body of the shape {@code
 * {"source": {"index_uuid": "...", "shard_id": 0}, "target": {"index_uuid": "...", "shard_id": 0},
 * "partition_index": 0, "num_partitions": 2}} -- the REST surface for {@link ShardSplitAction}
 * (rfc-serverless-opensearch.md &sect;16 Phase 5). One call creates one split target; splitting a
 * source {@code numPartitions} ways means calling this {@code numPartitions} times, one per target.
 */
public class RestShardSplitAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestShardSplitAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_shard_split";
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
        return singletonList(new Route(POST, "/_plugins/_serverless/storage/_split"));
    }

    /**
     * @param request the incoming REST request, whose body names a source and target shard plus the target's partition assignment.
     * @param client used to dispatch the parsed {@link ShardSplitRequest} locally.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        Map<String, Object> body;
        try (XContentParser parser = request.contentParser()) {
            body = parser.map();
        }
        ShardRef source = shardRef(body, "source");
        ShardRef target = shardRef(body, "target");
        Object partitionIndex = body.get("partition_index");
        Object numPartitions = body.get("num_partitions");
        if (!(partitionIndex instanceof Number) || !(numPartitions instanceof Number)) {
            throw new IllegalArgumentException("request body must contain numeric \"partition_index\" and \"num_partitions\"");
        }
        ShardSplitRequest splitRequest = new ShardSplitRequest(
            source.indexUuid(),
            source.shardId(),
            target.indexUuid(),
            target.shardId(),
            ((Number) partitionIndex).intValue(),
            ((Number) numPartitions).intValue()
        );
        return channel -> client.executeLocally(ShardSplitAction.INSTANCE, splitRequest, new RestToXContentListener<>(channel));
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
