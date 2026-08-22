/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero.action;

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
 * {@code POST /_plugins/_serverless/storage/_reactivate?index=<name>[&reader=true]} -- the REST surface for
 * {@link ReactivateShardsAction}, letting an operator manually reactivate a suspended index rather
 * than only ever relying on {@code ShardReactivationActionFilter}'s automatic on-access trigger
 * (useful to warm a shard back up ahead of expected traffic, avoiding the first real request paying
 * the cold-start cost). Deliberately a query parameter, not a {@code {index_uuid}} path segment like
 * this plugin's other single-index routes -- every one of those addresses a shard by UUID, but this
 * route addresses an index by its ordinary (mutable) name, the same identifier a ReactivateShardsRequest
 * already takes; reusing the {@code {index_uuid}} path-wildcard name here would have collided with
 * those other routes at the same {@link org.opensearch.common.path.PathTrie} depth (caught by this
 * feature's own {@code ServerlessStorageShardSuspensionIT}, which failed node startup entirely with
 * {@code IllegalArgumentException: Trying to use conflicting wildcard names for same path} before
 * this route was changed from a path segment to a query parameter).
 */
public class RestReactivateShardsAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestReactivateShardsAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_reactivate_shards";
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

    /** The single route this handler serves. */
    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_plugins/_serverless/storage/_reactivate"));
    }

    /**
     * @param request the incoming REST request; the required {@code index} query parameter names
     *                the index to reactivate, and the optional {@code reader} boolean query
     *                parameter (default {@code false}) selects reader (search-only) copies instead
     *                of writer copies.
     * @param client used to dispatch the parsed {@link ReactivateShardsRequest}.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ReactivateShardsRequest reactivateRequest = new ReactivateShardsRequest(
            request.param("index"),
            request.paramAsBoolean("reader", false)
        );
        return channel -> client.execute(ReactivateShardsAction.INSTANCE, reactivateRequest, new RestToXContentListener<>(channel));
    }
}
