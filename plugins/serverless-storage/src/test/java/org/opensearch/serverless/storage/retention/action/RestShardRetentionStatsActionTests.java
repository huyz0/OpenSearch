/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.HashMap;
import java.util.Map;

/**
 * Proves {@link RestShardRetentionStatsAction#prepareRequest} correctly parses a well-formed
 * GET request's {@code index_uuid}/{@code shard_id} path parameters and rejects a malformed
 * {@code shard_id}, mirroring {@code RestShardCloneActionTests}'s own shape -- this endpoint is
 * query-param-only (no JSON body), so parameters stand in for the body used elsewhere.
 * Deliberately doesn't exercise the actual dispatch through {@code client.executeLocally} (that
 * requires a real {@link NodeClient}/transport, already covered end to end by {@code
 * ServerlessStorageShardRetentionStatsActionIT}); this only proves parameter-parsing correctness
 * in isolation.
 */
public class RestShardRetentionStatsActionTests extends OpenSearchTestCase {

    private final RestShardRetentionStatsAction action = new RestShardRetentionStatsAction();

    private static RestRequest requestWithParams(Map<String, String> params) {
        return new FakeRestRequest.Builder(org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.GET)
            .withPath("/_plugins/_serverless/storage/my-uuid/0/_retention_stats")
            .withParams(params)
            .build();
    }

    public void testPrepareRequestParsesWellFormedPathParams() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("index_uuid", "my-uuid");
        params.put("shard_id", "0");
        RestRequest request = requestWithParams(params);
        // A non-null consumer proves parsing succeeded without throwing -- dispatch itself needs a
        // real NodeClient, out of scope here (see class javadoc).
        assertNotNull(action.prepareRequest(request, null));
    }

    public void testPrepareRequestRejectsAMissingShardId() {
        Map<String, String> params = new HashMap<>();
        params.put("index_uuid", "my-uuid");
        RestRequest request = requestWithParams(params);
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
    }

    public void testPrepareRequestRejectsANonNumericShardId() {
        Map<String, String> params = new HashMap<>();
        params.put("index_uuid", "my-uuid");
        params.put("shard_id", "zero");
        RestRequest request = requestWithParams(params);
        // NumberFormatException is a subclass of IllegalArgumentException.
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
    }
}
