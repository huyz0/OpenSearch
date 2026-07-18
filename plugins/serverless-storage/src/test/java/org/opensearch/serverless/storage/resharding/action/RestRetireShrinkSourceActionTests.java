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
 * Proves {@link RestRetireShrinkSourceAction#prepareRequest} correctly parses a well-formed
 * query-param request and rejects a non-numeric {@code target_shard_id}, mirroring {@code
 * RestShardCloneActionTests}'s own shape but adapted for this GET-style, query-param-only
 * endpoint (no JSON body). Deliberately doesn't exercise the actual dispatch through {@code
 * client.execute} (that requires a real {@link NodeClient}/transport, already covered end to end
 * by {@code ServerlessStorageRetireShrinkSourceActionIT}); this only proves param-parsing
 * correctness in isolation.
 */
public class RestRetireShrinkSourceActionTests extends OpenSearchTestCase {

    private final RestRetireShrinkSourceAction action = new RestRetireShrinkSourceAction();

    private static RestRequest requestWithParams(Map<String, String> params) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.POST)
            .withPath("/_plugins/_serverless/storage/_shrink/my-source/_retire")
            .withParams(params)
            .build();
    }

    public void testPrepareRequestParsesAWellFormedRequest() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("source_index", "my-source");
        params.put("target_index_uuid", "target-idx");
        params.put("target_shard_id", "0");
        RestRequest request = requestWithParams(params);
        // A non-null consumer proves parsing succeeded without throwing -- dispatch itself needs a
        // real NodeClient, out of scope here (see class javadoc).
        assertNotNull(action.prepareRequest(request, null));
    }

    public void testPrepareRequestRejectsANonNumericTargetShardId() {
        Map<String, String> params = new HashMap<>();
        params.put("source_index", "my-source");
        params.put("target_index_uuid", "target-idx");
        params.put("target_shard_id", "zero");
        RestRequest request = requestWithParams(params);
        // NumberFormatException extends IllegalArgumentException.
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
    }

    public void testPrepareRequestRejectsAMissingTargetShardId() {
        // BUG: Integer.parseInt(null) (request.param("target_shard_id") returns null when absent)
        // throws NumberFormatException with message "null" -- it never names the missing
        // "target_shard_id" param the way the compaction/clone actions' hand-written validation
        // does. See RestRetireShrinkSourceAction#prepareRequest.
        Map<String, String> params = new HashMap<>();
        params.put("source_index", "my-source");
        params.put("target_index_uuid", "target-idx");
        RestRequest request = requestWithParams(params);
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
    }
}
