/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class NodeObjectStoreRequestStatsResponseTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        NodeObjectStoreRequestStatsResponse original = new NodeObjectStoreRequestStatsResponse(3L, 5L, 1L, 2L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        NodeObjectStoreRequestStatsResponse deserialized = new NodeObjectStoreRequestStatsResponse(out.bytes().streamInput());

        assertEquals(original.getCount(), deserialized.getCount());
        assertEquals(original.putCount(), deserialized.putCount());
        assertEquals(original.deleteCount(), deserialized.deleteCount());
        assertEquals(original.listCount(), deserialized.listCount());
    }

    public void testToXContent() throws Exception {
        NodeObjectStoreRequestStatsResponse response = new NodeObjectStoreRequestStatsResponse(3L, 5L, 1L, 2L);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"get_count\":3"));
        assertTrue(json.contains("\"put_count\":5"));
        assertTrue(json.contains("\"delete_count\":1"));
        assertTrue(json.contains("\"list_count\":2"));
    }
}
