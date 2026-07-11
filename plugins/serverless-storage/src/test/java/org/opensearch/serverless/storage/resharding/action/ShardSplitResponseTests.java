/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class ShardSplitResponseTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        ShardSplitResponse original = new ShardSplitResponse(true);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardSplitResponse deserialized = new ShardSplitResponse(out.bytes().streamInput());

        assertEquals(original.acknowledged(), deserialized.acknowledged());
    }

    public void testSerializationRoundTripWhenNotAcknowledged() throws Exception {
        ShardSplitResponse original = new ShardSplitResponse(false);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardSplitResponse deserialized = new ShardSplitResponse(out.bytes().streamInput());

        assertFalse(deserialized.acknowledged());
    }

    public void testToXContentReportsAcknowledged() throws Exception {
        ShardSplitResponse response = new ShardSplitResponse(true);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"acknowledged\":true}", builder.toString());
    }
}
