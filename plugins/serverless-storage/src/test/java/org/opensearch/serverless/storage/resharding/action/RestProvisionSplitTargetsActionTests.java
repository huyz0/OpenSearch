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
 * Proves {@link RestProvisionSplitTargetsAction#prepareRequest} correctly parses a well-formed
 * query-param request, mirroring {@code RestShardCloneActionTests}'s own shape but adapted for
 * this GET-style, query-param-only endpoint (no JSON body). Deliberately doesn't exercise the
 * actual dispatch through {@code client.execute} (that requires a real {@link NodeClient}
 * /transport, already covered end to end by {@code
 * ServerlessStorageProvisionSplitTargetsActionIT}); this only proves param-parsing correctness in
 * isolation.
 */
public class RestProvisionSplitTargetsActionTests extends OpenSearchTestCase {

    private final RestProvisionSplitTargetsAction action = new RestProvisionSplitTargetsAction();

    private static RestRequest requestWithParams(Map<String, String> params) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.POST)
            .withPath("/_plugins/_serverless/storage/_resharding/_provision_split_targets/my-source")
            .withParams(params)
            .build();
    }

    public void testPrepareRequestParsesAWellFormedRequest() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("source", "my-source");
        params.put("target_indices", "target-1,target-2");
        RestRequest request = requestWithParams(params);
        // A non-null consumer proves parsing succeeded without throwing -- dispatch itself needs a
        // real NodeClient, out of scope here (see class javadoc).
        assertNotNull(action.prepareRequest(request, null));
    }

    public void testPrepareRequestWithMissingTargetIndicesParsesToEmptyList() throws Exception {
        // Strings.splitStringByCommaToArray(null) returns an empty array rather than throwing, so a
        // request with no target_indices param parses cleanly to an empty target-index list --
        // whether an empty list is itself meaningful is validated further downstream (the
        // transport action), not by this REST-layer parsing step.
        Map<String, String> params = new HashMap<>();
        params.put("source", "my-source");
        RestRequest request = requestWithParams(params);
        assertNotNull(action.prepareRequest(request, null));
    }

    /**
     * The list is uncapped input that provisions one real index per element, so an enormous list is
     * one request's worth of authorisation buying arbitrarily much cluster-state work. It must be
     * refused at the boundary, as a client error, rather than part-way through provisioning.
     */
    public void testPrepareRequestRejectsAnOverlongTargetIndicesList() {
        StringBuilder tooMany = new StringBuilder("target-0");
        for (int i = 1; i <= RestProvisionSplitTargetsAction.MAX_TARGET_INDICES; i++) {
            tooMany.append(",target-").append(i);
        }
        Map<String, String> params = new HashMap<>();
        params.put("source", "my-source");
        params.put("target_indices", tooMany.toString());
        RestRequest request = requestWithParams(params);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertTrue(e.getMessage(), e.getMessage().contains("more than the maximum"));
    }

    /** Exactly at the cap is still a legitimate request -- the bound is inclusive. */
    public void testPrepareRequestAcceptsTargetIndicesExactlyAtTheCap() throws Exception {
        StringBuilder atCap = new StringBuilder("target-0");
        for (int i = 1; i < RestProvisionSplitTargetsAction.MAX_TARGET_INDICES; i++) {
            atCap.append(",target-").append(i);
        }
        Map<String, String> params = new HashMap<>();
        params.put("source", "my-source");
        params.put("target_indices", atCap.toString());
        assertNotNull(action.prepareRequest(requestWithParams(params), null));
    }
}
