/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security;

import org.opensearch.test.OpenSearchTestCase;

public class ObjectStoreRequestCounterTests extends OpenSearchTestCase {

    public void testStartsAtZeroForEveryShape() {
        ObjectStoreRequestCounter counter = new ObjectStoreRequestCounter();
        assertEquals(0L, counter.getCount());
        assertEquals(0L, counter.putCount());
        assertEquals(0L, counter.deleteCount());
        assertEquals(0L, counter.listCount());
    }

    public void testEachRecordMethodOnlyIncrementsItsOwnShape() {
        ObjectStoreRequestCounter counter = new ObjectStoreRequestCounter();

        counter.recordGet();
        counter.recordGet();
        counter.recordPut();
        counter.recordDelete();
        counter.recordDelete();
        counter.recordDelete();
        counter.recordList();

        assertEquals(2L, counter.getCount());
        assertEquals(1L, counter.putCount());
        assertEquals(3L, counter.deleteCount());
        assertEquals(1L, counter.listCount());
    }
}
