/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction.action;

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
 * {@code POST /_plugins/_serverless/storage/_compact} with a body of the shape
 * {@code {"index_uuid": "...", "shard_id": 0}} -- the REST surface for {@link
 * CompactionTriggerAction} (rfc-serverless-opensearch.md &sect;7.4, &sect;16 Phase 4.5). Triggers
 * one immediate compaction-candidacy check and, if the shard is a candidate, one publish attempt
 * -- without waiting for {@code CompactionSchedulerTask}'s own background interval to elapse.
 */
public class RestCompactionTriggerAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestCompactionTriggerAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_compaction_trigger";
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
    public ServerlessScope serverlessScope() {
        return ServerlessScope.AVAILABLE;
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_plugins/_serverless/storage/_compact"));
    }

    /**
     * @param request the incoming REST request, whose body names the shard to compact.
     * @param client used to dispatch the parsed {@link CompactionTriggerRequest} locally.
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
        CompactionTriggerRequest triggerRequest = new CompactionTriggerRequest((String) indexUuid, ((Number) shardId).intValue());
        return channel -> client.executeLocally(CompactionTriggerAction.INSTANCE, triggerRequest, new RestToXContentListener<>(channel));
    }
}
