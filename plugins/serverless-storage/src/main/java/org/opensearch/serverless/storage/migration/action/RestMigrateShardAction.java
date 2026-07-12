/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.migration.action;

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
 * {@code POST /_plugins/_serverless/storage/{index_uuid}/{shard_id}/_migrate} -- the REST surface
 * for {@link MigrateShardAction} (rfc-serverless-opensearch.md &sect;16 Phase 6), same per-shard,
 * routed-to-the-hosting-node shape as {@code RestPollNowAction}/{@code RestWaitForGenerationAction}.
 */
public class RestMigrateShardAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestMigrateShardAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_migrate_shard";
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
        return singletonList(new Route(POST, "/_plugins/_serverless/storage/{index_uuid}/{shard_id}/_migrate"));
    }

    /**
     * @param request the incoming REST request, naming the shard to migrate via path parameters.
     * @param client used to dispatch the parsed {@link MigrateShardRequest} locally.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String indexUuid = request.param("index_uuid");
        int shardId = Integer.parseInt(request.param("shard_id"));
        MigrateShardRequest migrateRequest = new MigrateShardRequest(indexUuid, shardId);
        return channel -> client.executeLocally(MigrateShardAction.INSTANCE, migrateRequest, new RestToXContentListener<>(channel));
    }
}
