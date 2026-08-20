/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler.ApiAvailabilityScope;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * {@code POST /_plugins/_serverless/storage/_shrink/{source_index}/_retire?target_index_uuid=...&target_shard_id=...}
 * -- the REST surface for {@link RetireShrinkSourceAction} (rfc-serverless-opensearch.md &sect;16
 * Phase 5). Query parameters, not a request body: this is a small, entirely scalar request, the
 * same shape {@code RestMigrateShardAction}'s path-parameter-only request uses. The {@code
 * _shrink/} path segment (rather than starting directly with the wildcard, like {@code
 * RestMigrateShardAction}'s own {@code {index_uuid}}) is deliberate: two different routes cannot
 * both register a same-position wildcard segment under two different parameter names, confirmed
 * the hard way -- an earlier version of this route started directly with {@code {source_index}}
 * and collided with {@code RestMigrateShardAction}'s own {@code {index_uuid}} at that same
 * position, throwing {@code IllegalArgumentException: Trying to use conflicting wildcard names}.
 */
public class RestRetireShrinkSourceAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestRetireShrinkSourceAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_retire_shrink_source";
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
        return singletonList(new Route(POST, "/_plugins/_serverless/storage/_shrink/{source_index}/_retire"));
    }

    /**
     * @param request the incoming REST request, naming the source index via a path parameter and
     *                the shrink target it claims to have been merged into via query parameters.
     * @param client used to dispatch the parsed {@link RetireShrinkSourceRequest}.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String sourceIndexName = request.param("source_index");
        String targetIndexUuid = request.param("target_index_uuid");
        int targetShardId = Integer.parseInt(request.param("target_shard_id"));
        boolean acknowledgeUnfencedSource = request.paramAsBoolean("acknowledge_unfenced_source", false);
        RetireShrinkSourceRequest retireRequest = new RetireShrinkSourceRequest(
            sourceIndexName,
            targetIndexUuid,
            targetShardId,
            acknowledgeUnfencedSource
        );
        return channel -> client.execute(RetireShrinkSourceAction.INSTANCE, retireRequest, new RestToXContentListener<>(channel));
    }
}
