/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.action;

import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.HashMap;
import java.util.Map;

/**
 * Proves {@link RestWaitForGenerationAction#prepareRequest} correctly parses path and query
 * parameters from a well-formed request and rejects malformed ones, mirroring {@code
 * RestShardCloneActionTests}'s own shape. Deliberately doesn't exercise the actual dispatch
 * through {@code client.executeLocally} (that requires a real {@link NodeClient}/transport); no
 * dedicated {@code *IT} currently drives this endpoint's real dispatch (it's referenced only in
 * passing by {@code ServerlessStorageMigrateShardActionIT}'s javadoc), so this class is the only
 * coverage {@link RestWaitForGenerationAction} has today.
 */
public class RestWaitForGenerationActionTests extends OpenSearchTestCase {

    private final RestWaitForGenerationAction action = new RestWaitForGenerationAction();

    private static RestRequest requestWithParams(Map<String, String> params) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.GET)
            .withPath("/_plugins/_serverless/storage/{index_uuid}/{shard_id}/_wait_for_generation")
            .withParams(params)
            .build();
    }

    private static Map<String, String> wellFormedParams() {
        Map<String, String> params = new HashMap<>();
        params.put("index_uuid", "my-idx");
        params.put("shard_id", "0");
        params.put("min_generation", "5");
        return params;
    }

    public void testPrepareRequestParsesAWellFormedRequest() throws Exception {
        RestRequest request = requestWithParams(wellFormedParams());
        // A non-null consumer proves parsing succeeded without throwing -- dispatch itself needs a
        // real NodeClient, out of scope here (see class javadoc).
        assertNotNull(action.prepareRequest(request, null));
    }

    public void testPrepareRequestParsesAnExplicitTimeout() throws Exception {
        Map<String, String> params = wellFormedParams();
        params.put("timeout", "10s");
        RestRequest request = requestWithParams(params);
        assertNotNull(action.prepareRequest(request, null));
    }

    public void testPrepareRequestRejectsAMissingShardIdParam() {
        Map<String, String> params = wellFormedParams();
        params.remove("shard_id");
        RestRequest request = requestWithParams(params);
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
    }

    public void testPrepareRequestRejectsAMissingMinGenerationParam() {
        Map<String, String> params = wellFormedParams();
        params.remove("min_generation");
        RestRequest request = requestWithParams(params);
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
    }

    public void testPrepareRequestRejectsANonNumericMinGenerationParam() {
        Map<String, String> params = wellFormedParams();
        params.put("min_generation", "not-a-number");
        RestRequest request = requestWithParams(params);
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
    }

    public void testPrepareRequestRejectsAnUnparseableTimeout() {
        Map<String, String> params = wellFormedParams();
        params.put("timeout", "not-a-duration");
        RestRequest request = requestWithParams(params);
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
    }
}
