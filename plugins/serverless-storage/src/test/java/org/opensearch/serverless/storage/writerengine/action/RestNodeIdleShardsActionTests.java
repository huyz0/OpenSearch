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

/**
 * Proves {@link RestNodeIdleShardsAction#prepareRequest} builds cleanly on a bare GET request --
 * this endpoint takes no path or body parameters, so unlike {@code RestShardCloneActionTests}
 * there's no malformed-input shape to reject. Deliberately doesn't exercise the actual dispatch
 * through {@code client.executeLocally} (that requires a real {@link NodeClient}/transport,
 * already covered end to end by {@code ServerlessStorageNodeIdleShardsActionIT}); this only
 * proves request-building correctness in isolation.
 */
public class RestNodeIdleShardsActionTests extends OpenSearchTestCase {

    private final RestNodeIdleShardsAction action = new RestNodeIdleShardsAction();

    private static RestRequest bareRequest() {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.GET)
            .withPath("/_plugins/_serverless/storage/_idle_shards")
            .build();
    }

    public void testPrepareRequestBuildsOnABareGetRequest() throws Exception {
        RestRequest request = bareRequest();
        // A non-null consumer proves the request built successfully without throwing -- dispatch
        // itself needs a real NodeClient, out of scope here (see class javadoc).
        assertNotNull(action.prepareRequest(request, null));
    }
}
