/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.action;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Any non-negative timeout used to be accepted, so an hour-long read-after-write wait was a legal
 * request -- and every waiter costs the receiving engine a pending listener plus a share of that
 * shard's object-store poll cadence for the whole duration.
 */
public class WaitForGenerationRequestTimeoutBoundTests extends OpenSearchTestCase {

    private static WaitForGenerationRequest requestWithTimeout(TimeValue timeout) {
        return new WaitForGenerationRequest("idx-uuid", 0, 7L, timeout);
    }

    public void testAnOverlongTimeoutIsRejected() {
        var validation = requestWithTimeout(TimeValue.timeValueHours(1)).validate();
        assertNotNull("an hour-long read-after-write wait must not be accepted", validation);
        assertTrue(validation.getMessage(), validation.getMessage().contains("timeout must be <="));
    }

    public void testATimeoutAtTheBoundIsAccepted() {
        assertNull(requestWithTimeout(TimeValue.timeValueMillis(WaitForGenerationRequest.MAX_TIMEOUT_MILLIS)).validate());
        assertNull(requestWithTimeout(TimeValue.timeValueSeconds(5)).validate());
        assertNull(requestWithTimeout(TimeValue.ZERO).validate());
    }

    public void testANegativeTimeoutIsStillRejected() {
        var validation = requestWithTimeout(TimeValue.timeValueMillis(-1)).validate();
        assertNotNull(validation);
        assertTrue(validation.getMessage(), validation.getMessage().contains("timeout must be >= 0"));
    }
}
