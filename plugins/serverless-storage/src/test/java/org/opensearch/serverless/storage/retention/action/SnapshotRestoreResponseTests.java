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

public class SnapshotRestoreResponseTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        SnapshotRestoreResponse original = new SnapshotRestoreResponse(4L, 9L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        SnapshotRestoreResponse deserialized = new SnapshotRestoreResponse(out.bytes().streamInput());

        assertEquals(4L, deserialized.primaryTerm());
        assertEquals(9L, deserialized.generation());
    }

    public void testToXContent() throws Exception {
        SnapshotRestoreResponse response = new SnapshotRestoreResponse(4L, 9L);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"primary_term\":4"));
        assertTrue(json.contains("\"generation\":9"));
    }
}
