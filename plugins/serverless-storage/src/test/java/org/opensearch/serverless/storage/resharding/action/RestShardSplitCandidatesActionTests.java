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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Proves {@link RestShardSplitCandidatesAction#prepareRequest} correctly parses a request with no
 * overrides (defaults kick in) and one with explicit numeric overrides, and rejects a
 * non-numeric override, mirroring {@code RestShardCloneActionTests}'s own shape but adapted for
 * this GET-style, query-param-only endpoint (no JSON body). Deliberately doesn't exercise the
 * actual dispatch through {@code client.execute} (that requires a real {@link NodeClient}
 * /transport, already covered end to end by {@code ServerlessStorageShardSplitCandidatesIT});
 * this only proves param-parsing correctness in isolation.
 */
public class RestShardSplitCandidatesActionTests extends OpenSearchTestCase {

    private final RestShardSplitCandidatesAction action = new RestShardSplitCandidatesAction();

    private static RestRequest requestWithParams(Map<String, String> params) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.GET)
            .withPath("/_plugins/_serverless/storage/_resharding/split_candidates")
            .withParams(params)
            .build();
    }

    public void testPrepareRequestWithNoOverridesUsesDefaults() throws Exception {
        RestRequest request = requestWithParams(Collections.emptyMap());
        // A non-null consumer proves parsing succeeded without throwing -- dispatch itself needs a
        // real NodeClient, out of scope here (see class javadoc).
        assertNotNull(action.prepareRequest(request, null));
    }

    public void testPrepareRequestParsesExplicitOverrides() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("writes_per_minute_threshold", "1000");
        params.put("size_threshold_bytes", "2048");
        RestRequest request = requestWithParams(params);
        assertNotNull(action.prepareRequest(request, null));
    }

    public void testPrepareRequestRejectsANonNumericWritesPerMinuteThreshold() {
        Map<String, String> params = new HashMap<>();
        params.put("writes_per_minute_threshold", "not-a-number");
        RestRequest request = requestWithParams(params);
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
    }

    public void testPrepareRequestRejectsANonNumericSizeThresholdBytes() {
        Map<String, String> params = new HashMap<>();
        params.put("size_threshold_bytes", "not-a-number");
        RestRequest request = requestWithParams(params);
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
    }
}
