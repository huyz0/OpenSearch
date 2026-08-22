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
 * {@code POST /_plugins/_serverless/storage/_resharding/_provision_split_targets/{source}?target_indices=a,b}
 * -- the REST surface for {@link ProvisionSplitTargetsAction}. Mirrors core's own {@code
 * /{index}/_split/{target}} contract: the caller always names every target index explicitly, here
 * as a comma-separated list (one plugin call creates every partition's target at once, rather than
 * core's one-target-per-call shape, since a split here is always a fixed, known partition count
 * decided by the caller up front).
 */
public class RestProvisionSplitTargetsAction extends BaseRestHandler {

    /**
     * Ceiling on how many target index names one call may name. Every element becomes a real index
     * this call provisions, so an uncapped list turns a single request into arbitrarily much
     * cluster-state work; core caps a split's shard count for the same reason. Comfortably above
     * any real split (core's own default index shard ceiling is 1024), so this only ever refuses a
     * request that was never going to be a sane split in the first place -- and refuses it up front,
     * as a 400, rather than part-way through provisioning.
     */
    static final int MAX_TARGET_INDICES = 1024;

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestProvisionSplitTargetsAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_provision_split_targets";
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
        return singletonList(new Route(POST, "/_plugins/_serverless/storage/_resharding/_provision_split_targets/{source}"));
    }

    /**
     * @param request the incoming REST request, naming the source index via a path parameter and
     *                every target index name via a query parameter.
     * @param client used to dispatch the parsed {@link ProvisionSplitTargetsRequest}.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String sourceIndexName = request.param("source");
        String[] targetIndices = Strings.splitStringByCommaToArray(request.param("target_indices"));
        if (targetIndices.length > MAX_TARGET_INDICES) {
            throw new IllegalArgumentException(
                "\"target_indices\" names " + targetIndices.length + " indices, more than the maximum of " + MAX_TARGET_INDICES
            );
        }
        ProvisionSplitTargetsRequest provisionRequest = new ProvisionSplitTargetsRequest(sourceIndexName, Arrays.asList(targetIndices));
        return channel -> client.execute(ProvisionSplitTargetsAction.INSTANCE, provisionRequest, new RestToXContentListener<>(channel));
    }
}
