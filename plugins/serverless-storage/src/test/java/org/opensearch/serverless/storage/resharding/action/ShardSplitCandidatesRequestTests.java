/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

public class ShardSplitCandidatesRequestTests extends OpenSearchTestCase {

    public void testSerializationRoundTripWithDefaultThresholds() throws Exception {
        ShardSplitCandidatesRequest original = new ShardSplitCandidatesRequest();

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardSplitCandidatesRequest deserialized = new ShardSplitCandidatesRequest(out.bytes().streamInput());

        assertEquals(ShardSplitCandidatesRequest.USE_DEFAULT_WPM_THRESHOLD, deserialized.writesPerMinuteThreshold());
        assertEquals(ShardSplitCandidatesRequest.USE_DEFAULT_SIZE_THRESHOLD_BYTES, deserialized.sizeThresholdBytes());
    }

    public void testSerializationRoundTripWithExplicitThresholds() throws Exception {
        ShardSplitCandidatesRequest original = new ShardSplitCandidatesRequest(5000L, 1_000_000L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardSplitCandidatesRequest deserialized = new ShardSplitCandidatesRequest(out.bytes().streamInput());

        assertEquals(5000L, deserialized.writesPerMinuteThreshold());
        assertEquals(1_000_000L, deserialized.sizeThresholdBytes());
    }

    public void testSerializationRoundTripWithWpmOnlyConstructor() throws Exception {
        ShardSplitCandidatesRequest original = new ShardSplitCandidatesRequest(7500L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardSplitCandidatesRequest deserialized = new ShardSplitCandidatesRequest(out.bytes().streamInput());

        assertEquals(7500L, deserialized.writesPerMinuteThreshold());
        assertEquals(ShardSplitCandidatesRequest.USE_DEFAULT_SIZE_THRESHOLD_BYTES, deserialized.sizeThresholdBytes());
    }
}
