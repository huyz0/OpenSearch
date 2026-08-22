/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class NodeWalBacklogResponseTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        NodeWalBacklogResponse original = new NodeWalBacklogResponse(42, 12345L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        NodeWalBacklogResponse deserialized = new NodeWalBacklogResponse(out.bytes().streamInput());

        assertEquals(original.bufferedRecordCount(), deserialized.bufferedRecordCount());
        assertEquals(original.totalBufferedBytes(), deserialized.totalBufferedBytes());
    }

    public void testToXContent() throws Exception {
        NodeWalBacklogResponse response = new NodeWalBacklogResponse(42, 12345L);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"buffered_record_count\":42"));
        assertTrue(json.contains("\"total_buffered_bytes\":12345"));
    }
}
