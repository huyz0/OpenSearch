/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class NodeManifestLagResponseTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        NodeManifestLagResponse original = new NodeManifestLagResponse(
            List.of(new ShardLagEntry("idx-a", 0, 1L), new ShardLagEntry("idx-b", 1, 2L))
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        NodeManifestLagResponse deserialized = new NodeManifestLagResponse(out.bytes().streamInput());

        assertEquals(original.entries(), deserialized.entries());
    }

    public void testSerializationRoundTripWithNoEntries() throws Exception {
        NodeManifestLagResponse original = new NodeManifestLagResponse(List.of());

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        NodeManifestLagResponse deserialized = new NodeManifestLagResponse(out.bytes().streamInput());

        assertTrue(deserialized.entries().isEmpty());
    }

    public void testToXContent() throws Exception {
        NodeManifestLagResponse response = new NodeManifestLagResponse(List.of(new ShardLagEntry("idx-a", 0, 1L)));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"lagging_shards\""));
        assertTrue(json.contains("\"index_uuid\":\"idx-a\""));
    }
}
