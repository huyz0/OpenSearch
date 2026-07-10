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

public class IndexSnapshotReleaseResponseTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        IndexSnapshotReleaseResponse original = new IndexSnapshotReleaseResponse(3);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        IndexSnapshotReleaseResponse deserialized = new IndexSnapshotReleaseResponse(out.bytes().streamInput());

        assertEquals(3, deserialized.shardCount());
    }

    public void testToXContent() throws Exception {
        IndexSnapshotReleaseResponse response = new IndexSnapshotReleaseResponse(3);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"acknowledged\":true"));
        assertTrue(json.contains("\"shard_count\":3"));
    }
}
