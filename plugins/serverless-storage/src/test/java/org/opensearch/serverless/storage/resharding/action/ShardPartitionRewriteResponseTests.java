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

public class ShardPartitionRewriteResponseTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        ShardPartitionRewriteResponse original = new ShardPartitionRewriteResponse(true);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardPartitionRewriteResponse deserialized = new ShardPartitionRewriteResponse(out.bytes().streamInput());

        assertEquals(original.rewritten(), deserialized.rewritten());
    }

    public void testSerializationRoundTripWhenNotRewritten() throws Exception {
        ShardPartitionRewriteResponse original = new ShardPartitionRewriteResponse(false);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardPartitionRewriteResponse deserialized = new ShardPartitionRewriteResponse(out.bytes().streamInput());

        assertFalse(deserialized.rewritten());
    }

    public void testToXContentReportsRewritten() throws Exception {
        ShardPartitionRewriteResponse response = new ShardPartitionRewriteResponse(true);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"rewritten\":true}", builder.toString());
    }
}
