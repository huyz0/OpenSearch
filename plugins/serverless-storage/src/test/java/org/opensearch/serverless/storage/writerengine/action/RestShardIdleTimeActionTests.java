/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine.action;

import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.HashMap;
import java.util.Map;

/**
 * Proves {@link RestShardIdleTimeAction#prepareRequest} correctly parses path parameters from a
 * well-formed request and rejects a malformed one, mirroring {@code RestShardCloneActionTests}'s
 * own shape. Deliberately doesn't exercise the actual dispatch through {@code
 * client.executeLocally} (that requires a real {@link NodeClient}/transport, already covered end
 * to end by {@code ServerlessStorageShardIdleTimeActionIT}); this only proves path-parsing
 * correctness in isolation.
 */
public class RestShardIdleTimeActionTests extends OpenSearchTestCase {

    private final RestShardIdleTimeAction action = new RestShardIdleTimeAction();

    private static RestRequest requestWithParams(Map<String, String> params) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.GET)
            .withPath("/_plugins/_serverless/storage/{index_uuid}/{shard_id}/_idle_time")
            .withParams(params)
            .build();
    }

    private static Map<String, String> wellFormedParams() {
        Map<String, String> params = new HashMap<>();
        params.put("index_uuid", "my-idx");
        params.put("shard_id", "0");
        return params;
    }

    public void testPrepareRequestParsesAWellFormedRequest() throws Exception {
        RestRequest request = requestWithParams(wellFormedParams());
        // A non-null consumer proves parsing succeeded without throwing -- dispatch itself needs a
        // real NodeClient, out of scope here (see class javadoc).
        assertNotNull(action.prepareRequest(request, null));
    }

    public void testPrepareRequestRejectsAMissingShardIdParam() {
        Map<String, String> params = wellFormedParams();
        params.remove("shard_id");
        RestRequest request = requestWithParams(params);
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
    }

    public void testPrepareRequestRejectsANonNumericShardIdParam() {
        Map<String, String> params = wellFormedParams();
        params.put("shard_id", "zero");
        RestRequest request = requestWithParams(params);
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
    }
}
