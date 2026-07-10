/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;

public class RestHandlerTests extends OpenSearchTestCase {

    private static final RestHandler DEFAULT_HANDLER = (request, channel, client) -> {};

    public void testServerlessScopeDefaultsToUnavailable() {
        assertEquals(RestHandler.ServerlessScope.UNAVAILABLE, DEFAULT_HANDLER.serverlessScope());
    }

    public void testWrapperDelegatesServerlessScope() {
        RestHandler declaresAvailable = new RestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) {}

            @Override
            public RestHandler.ServerlessScope serverlessScope() {
                return RestHandler.ServerlessScope.AVAILABLE;
            }
        };

        RestHandler wrapped = RestHandler.wrapper(declaresAvailable);
        assertEquals(RestHandler.ServerlessScope.AVAILABLE, wrapped.serverlessScope());
    }

    public void testWrapperDelegatesDefaultUnavailableToo() {
        RestHandler wrapped = RestHandler.wrapper(DEFAULT_HANDLER);
        assertEquals(RestHandler.ServerlessScope.UNAVAILABLE, wrapped.serverlessScope());
    }
}
