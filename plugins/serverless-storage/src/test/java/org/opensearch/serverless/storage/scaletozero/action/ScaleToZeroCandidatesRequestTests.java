/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.test.OpenSearchTestCase;

public class ScaleToZeroCandidatesRequestTests extends OpenSearchTestCase {

    public void testDefaultConstructorValidates() {
        assertNull(new ScaleToZeroCandidatesRequest().validate());
    }

    public void testTheDefaultSentinelValidates() {
        assertNull(new ScaleToZeroCandidatesRequest(-1L, -1L).validate());
    }

    public void testOrdinaryPositiveOverridesValidate() {
        assertNull(new ScaleToZeroCandidatesRequest(60000L, 10L).validate());
    }

    /**
     * Regression test: an unvalidated negative idleThresholdMillis other than the -1 sentinel would
     * make ScaleToZeroCandidatesResponse's {@code millisSinceLastActivity() >= idleThresholdMillis}
     * check true for every shard, flagging actively-written shards as scale-to-zero candidates.
     */
    public void testNegativeIdleThresholdOtherThanTheSentinelIsRejected() {
        ActionRequestValidationException e = new ScaleToZeroCandidatesRequest(-100L, -1L).validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("idleThresholdMillis"));
    }

    public void testNegativeLagThresholdOtherThanTheSentinelIsRejected() {
        ActionRequestValidationException e = new ScaleToZeroCandidatesRequest(-1L, -5L).validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("lagThreshold"));
    }
}
