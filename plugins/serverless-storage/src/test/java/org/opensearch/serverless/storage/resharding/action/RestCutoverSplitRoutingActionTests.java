/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.HashMap;
import java.util.Map;

/**
 * Proves {@link RestCutoverSplitRoutingAction#prepareRequest} correctly parses a well-formed
 * query-param request and handles a missing {@code target_indices} param, mirroring {@code
 * RestShardCloneActionTests}'s own shape but adapted for this GET-style, query-param-only
 * endpoint (no JSON body). Deliberately doesn't exercise the actual dispatch through {@code
 * client.execute} (that requires a real {@link NodeClient}/transport, already covered end to end
 * by {@code ServerlessStorageCutoverSplitRoutingActionIT}); this only proves param-parsing
 * correctness in isolation.
 */
public class RestCutoverSplitRoutingActionTests extends OpenSearchTestCase {

    private final RestCutoverSplitRoutingAction action = new RestCutoverSplitRoutingAction();

    private static RestRequest requestWithParams(Map<String, String> params) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.POST)
            .withPath("/_plugins/_serverless/storage/_resharding/_cutover/my-alias")
            .withParams(params)
            .build();
    }

    public void testPrepareRequestParsesAWellFormedRequest() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("alias", "my-alias");
        params.put("target_indices", "target-1,target-2");
        RestRequest request = requestWithParams(params);
        // A non-null consumer proves parsing succeeded without throwing -- dispatch itself needs a
        // real NodeClient, out of scope here (see class javadoc).
        assertNotNull(action.prepareRequest(request, null));
    }

    public void testPrepareRequestWithMissingTargetIndicesParsesToEmptyList() throws Exception {
        // Strings.splitStringByCommaToArray(null) returns an empty array rather than throwing, so a
        // request with no target_indices param parses cleanly to an empty target-index list --
        // whether an empty cutover target list is itself a meaningful request is validated further
        // downstream (the transport action), not by this REST-layer parsing step.
        Map<String, String> params = new HashMap<>();
        params.put("alias", "my-alias");
        RestRequest request = requestWithParams(params);
        assertNotNull(action.prepareRequest(request, null));
    }

    public void testPrepareRequestWithEmptyTargetIndicesParsesToEmptyList() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("alias", "my-alias");
        params.put("target_indices", "");
        RestRequest request = requestWithParams(params);
        assertNotNull(action.prepareRequest(request, null));
    }

    /**
     * The list is uncapped input that turns into one alias action per element inside a single
     * cluster-state update, so an enormous list is one request's worth of authorisation buying
     * arbitrarily much cluster-manager work. It must be refused at the boundary, as a client error.
     */
    public void testPrepareRequestRejectsAnOverlongTargetIndicesList() {
        StringBuilder tooMany = new StringBuilder("target-0");
        for (int i = 1; i <= RestCutoverSplitRoutingAction.MAX_TARGET_INDICES; i++) {
            tooMany.append(",target-").append(i);
        }
        Map<String, String> params = new HashMap<>();
        params.put("alias", "my-alias");
        params.put("target_indices", tooMany.toString());
        RestRequest request = requestWithParams(params);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertTrue(e.getMessage(), e.getMessage().contains("more than the maximum"));
    }

    /** Exactly at the cap is still a legitimate request -- the bound is inclusive. */
    public void testPrepareRequestAcceptsTargetIndicesExactlyAtTheCap() throws Exception {
        StringBuilder atCap = new StringBuilder("target-0");
        for (int i = 1; i < RestCutoverSplitRoutingAction.MAX_TARGET_INDICES; i++) {
            atCap.append(",target-").append(i);
        }
        Map<String, String> params = new HashMap<>();
        params.put("alias", "my-alias");
        params.put("target_indices", atCap.toString());
        assertNotNull(action.prepareRequest(requestWithParams(params), null));
    }
}
