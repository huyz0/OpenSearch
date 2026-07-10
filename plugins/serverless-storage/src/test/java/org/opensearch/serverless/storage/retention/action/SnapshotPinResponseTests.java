/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class SnapshotPinResponseTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        SnapshotPinResponse original = new SnapshotPinResponse(3L, 7L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        SnapshotPinResponse deserialized = new SnapshotPinResponse(out.bytes().streamInput());

        assertEquals(3L, deserialized.primaryTerm());
        assertEquals(7L, deserialized.generation());
    }

    public void testToXContent() throws Exception {
        SnapshotPinResponse response = new SnapshotPinResponse(3L, 7L);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"primary_term\":3"));
        assertTrue(json.contains("\"generation\":7"));
    }
}
