/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaleup.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.test.OpenSearchTestCase;

public class ScaleUpCandidatesRequestTests extends OpenSearchTestCase {

    public void testDefaultConstructorValidates() {
        assertNull(new ScaleUpCandidatesRequest().validate());
    }

    public void testTheDefaultSentinelValidates() {
        assertNull(new ScaleUpCandidatesRequest(-1L, -1).validate());
    }

    public void testOrdinaryPositiveOverridesValidate() {
        assertNull(new ScaleUpCandidatesRequest(100L, 5).validate());
    }

    /**
     * Regression test: an unvalidated negative qpmThreshold other than the -1 sentinel would make
     * ScaleUpCandidatesResponse's {@code queriesPerMinute() > qpmThreshold} check true for every
     * shard, since query rates are never negative -- flagging the entire cluster as a candidate.
     */
    public void testNegativeQpmThresholdOtherThanTheSentinelIsRejected() {
        ActionRequestValidationException e = new ScaleUpCandidatesRequest(-100L, -1).validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("qpmThreshold"));
    }

    public void testNegativeMaxSearchReplicasOtherThanTheSentinelIsRejected() {
        ActionRequestValidationException e = new ScaleUpCandidatesRequest(-1L, -5).validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("maxSearchReplicas"));
    }
}
