/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class CompactionTriggerResponseTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        CompactionTriggerResponse original = new CompactionTriggerResponse(true);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        CompactionTriggerResponse deserialized = new CompactionTriggerResponse(out.bytes().streamInput());

        assertEquals(original.attempted(), deserialized.attempted());
    }

    public void testSerializationRoundTripWhenNotAttempted() throws Exception {
        CompactionTriggerResponse original = new CompactionTriggerResponse(false);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        CompactionTriggerResponse deserialized = new CompactionTriggerResponse(out.bytes().streamInput());

        assertFalse(deserialized.attempted());
    }

    public void testToXContentReportsAttempted() throws Exception {
        CompactionTriggerResponse response = new CompactionTriggerResponse(true);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"attempted\":true}", builder.toString());
    }
}
