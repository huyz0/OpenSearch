/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.rest.RestHandler.ServerlessScope;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

public class RestGetMappingActionTests extends OpenSearchTestCase {

    public void testServerlessScopeIsAvailable() {
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            assertEquals(ServerlessScope.AVAILABLE, new RestGetMappingAction(threadPool).serverlessScope());
        } finally {
            threadPool.shutdown();
        }
    }
}
