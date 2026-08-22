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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Proves {@link RestReactivateShardsAction#prepareRequest} correctly parses query parameters from
 * a well-formed request, including its optional {@code reader} flag, mirroring {@code
 * RestShardCloneActionTests}'s own shape. This action reads {@code index} via a plain, un-required
 * {@link RestRequest#param}, so a missing {@code index} doesn't throw here -- it builds fine and
 * would surface as a downstream validation failure once dispatched (out of scope for this class;
 * see below). Deliberately doesn't exercise the actual dispatch through {@code client.execute}
 * (that requires a real {@link NodeClient}/transport, already covered end to end by {@code
 * ServerlessStorageShardSuspensionIT}); this only proves query-parsing correctness in isolation.
 */
public class RestReactivateShardsActionTests extends OpenSearchTestCase {

    private final RestReactivateShardsAction action = new RestReactivateShardsAction();

    private static RestRequest requestWithParams(Map<String, String> params) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.POST)
            .withPath("/_plugins/_serverless/storage/_reactivate")
            .withParams(params)
            .build();
    }

    public void testPrepareRequestParsesAWellFormedRequest() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("index", "my-index");
        params.put("reader", "true");
        RestRequest request = requestWithParams(params);
        // A non-null consumer proves parsing succeeded without throwing -- dispatch itself needs a
        // real NodeClient, out of scope here (see class javadoc).
        assertNotNull(action.prepareRequest(request, null));
    }

    public void testPrepareRequestDefaultsReaderToFalseWhenAbsent() throws Exception {
        RestRequest request = requestWithParams(Collections.singletonMap("index", "my-index"));
        assertNotNull(action.prepareRequest(request, null));
    }

    public void testPrepareRequestRejectsANonBooleanReaderParam() {
        Map<String, String> params = new HashMap<>();
        params.put("index", "my-index");
        params.put("reader", "not-a-boolean");
        RestRequest request = requestWithParams(params);
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
    }
}
