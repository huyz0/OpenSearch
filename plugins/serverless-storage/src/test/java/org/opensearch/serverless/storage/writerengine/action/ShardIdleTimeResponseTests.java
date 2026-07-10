/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class ShardIdleTimeResponseTests extends OpenSearchTestCase {

    public void testSerializationRoundTripWhenTracked() throws Exception {
        ShardIdleTimeResponse original = new ShardIdleTimeResponse(12345L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardIdleTimeResponse deserialized = new ShardIdleTimeResponse(out.bytes().streamInput());

        assertTrue(deserialized.tracked());
        assertEquals(12345L, deserialized.millisSinceLastActivity());
    }

    public void testSerializationRoundTripWhenNotTracked() throws Exception {
        ShardIdleTimeResponse original = ShardIdleTimeResponse.notTracked();

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardIdleTimeResponse deserialized = new ShardIdleTimeResponse(out.bytes().streamInput());

        assertFalse(deserialized.tracked());
    }

    public void testToXContentWhenTrackedIncludesTheIdleTime() throws Exception {
        ShardIdleTimeResponse response = new ShardIdleTimeResponse(999L);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"tracked\":true"));
        assertTrue(json.contains("\"millis_since_last_activity\":999"));
    }

    public void testToXContentWhenNotTrackedOmitsTheIdleTime() throws Exception {
        ShardIdleTimeResponse response = ShardIdleTimeResponse.notTracked();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"tracked\":false"));
        assertFalse(
            "a not-tracked response must not report a millis_since_last_activity value that could be mistaken for a real answer",
            json.contains("millis_since_last_activity")
        );
    }
}
