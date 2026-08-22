/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler.ApiAvailabilityScope;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * {@code POST /_plugins/_serverless/storage/_resharding/_orchestrate_split/{source}?target_indices=a,b&amp;routing_alias=alias&amp;enable_write_routing=true}
 * -- the REST surface for {@link OrchestrateShardSplitAction}. {@code routing_alias} defaults to
 * {@code source} itself if not given (the common case: reusing the source's own name as the
 * alias once {@link RetireShrinkSourceAction}'s "verify then delete" pattern later reclaims it,
 * mirroring how {@code RestCutoverSplitRoutingAction}'s own alias parameter is caller-chosen).
 *
 * <p><b>Before calling this: make sure the source index named in {@code {source}} is not
 * receiving direct writes.</b> This action never quiesces or fences the source -- it clones the
 * source's state as of a point-in-time generation, then adds an alias pointing at the split
 * targets. Any document written to the source under its own name after that clone point is not
 * reflected in the targets or visible through the new alias, and is lost for good once the source
 * is later retired via {@link RetireShrinkSourceAction}. Freeze application-level writes to the
 * source, or migrate writers onto an alias first, before triggering this action; nothing in this
 * plugin enforces that for you. See {@link TransportOrchestrateShardSplitAction}'s own javadoc and
 * rfc-serverless-opensearch.md &sect;16 Phase 4 for the full detail.
 */
public class RestOrchestrateShardSplitAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestOrchestrateShardSplitAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_orchestrate_shard_split";
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
        return singletonList(new Route(POST, "/_plugins/_serverless/storage/_resharding/_orchestrate_split/{source}"));
    }

    /**
     * @param request the incoming REST request, naming the source index via a path parameter and
     *                the target indices/routing alias/write-routing opt-in via query parameters.
     * @param client used to dispatch the parsed {@link OrchestrateShardSplitRequest}.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String sourceIndexName = request.param("source");
        String[] targetIndices = Strings.splitStringByCommaToArray(request.param("target_indices"));
        String routingAliasName = request.param("routing_alias", sourceIndexName);
        boolean enableWriteRouting = request.paramAsBoolean("enable_write_routing", false);
        OrchestrateShardSplitRequest orchestrateRequest = new OrchestrateShardSplitRequest(
            sourceIndexName,
            Arrays.asList(targetIndices),
            routingAliasName,
            enableWriteRouting
        );
        return channel -> client.execute(OrchestrateShardSplitAction.INSTANCE, orchestrateRequest, new RestToXContentListener<>(channel));
    }
}
