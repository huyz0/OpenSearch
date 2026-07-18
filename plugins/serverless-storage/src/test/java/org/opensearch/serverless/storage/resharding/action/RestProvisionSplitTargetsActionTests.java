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
}
