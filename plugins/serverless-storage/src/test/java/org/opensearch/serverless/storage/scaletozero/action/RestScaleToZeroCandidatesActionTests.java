/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero.action;

import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.HashMap;
import java.util.Map;

/**
 * Proves {@link RestScaleToZeroCandidatesAction#prepareRequest} builds cleanly on a bare GET
 * request and on one carrying its optional {@code idle_threshold}/{@code lag_threshold} overrides,
 * and rejects malformed values for each -- this endpoint takes no request body, unlike {@code
 * RestShardCloneActionTests}. Deliberately doesn't exercise the actual dispatch through {@code
 * client.execute} (that requires a real {@link NodeClient}/transport, already covered end to end
 * by {@code ServerlessStorageScaleToZeroCandidatesActionIT}); this only proves request-building
 * correctness in isolation.
 */
public class RestScaleToZeroCandidatesActionTests extends OpenSearchTestCase {

    private final RestScaleToZeroCandidatesAction action = new RestScaleToZeroCandidatesAction();

    private static RestRequest requestWithParams(Map<String, String> params) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.GET)
            .withPath("/_plugins/_serverless/storage/_scale_to_zero/candidates")
            .withParams(params)
            .build();
    }

    public void testPrepareRequestBuildsOnABareGetRequest() throws Exception {
        RestRequest request = requestWithParams(new HashMap<>());
        // A non-null consumer proves the request built successfully without throwing -- dispatch
        // itself needs a real NodeClient, out of scope here (see class javadoc).
        assertNotNull(action.prepareRequest(request, null));
    }

    public void testPrepareRequestParsesOverrideParams() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("idle_threshold", "5m");
        params.put("lag_threshold", "10");
        RestRequest request = requestWithParams(params);
        assertNotNull(action.prepareRequest(request, null));
    }

    public void testPrepareRequestRejectsAnUnparseableIdleThreshold() {
        Map<String, String> params = new HashMap<>();
        params.put("idle_threshold", "not-a-duration");
        RestRequest request = requestWithParams(params);
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
    }

    public void testPrepareRequestRejectsANonNumericLagThreshold() {
        Map<String, String> params = new HashMap<>();
        params.put("lag_threshold", "not-a-number");
        RestRequest request = requestWithParams(params);
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
    }
}
