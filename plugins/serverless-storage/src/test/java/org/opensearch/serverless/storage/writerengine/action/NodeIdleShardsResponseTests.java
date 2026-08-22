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

import java.util.List;

public class NodeIdleShardsResponseTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        NodeIdleShardsResponse original = new NodeIdleShardsResponse(
            List.of(new IdleShardEntry("idx-a", 0, 100L), new IdleShardEntry("idx-b", 1, 200L))
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        NodeIdleShardsResponse deserialized = new NodeIdleShardsResponse(out.bytes().streamInput());

        assertEquals(original.entries(), deserialized.entries());
    }

    public void testSerializationRoundTripWithNoEntries() throws Exception {
        NodeIdleShardsResponse original = new NodeIdleShardsResponse(List.of());

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        NodeIdleShardsResponse deserialized = new NodeIdleShardsResponse(out.bytes().streamInput());

        assertTrue(deserialized.entries().isEmpty());
    }

    public void testToXContent() throws Exception {
        NodeIdleShardsResponse response = new NodeIdleShardsResponse(List.of(new IdleShardEntry("idx-a", 0, 100L)));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"idle_shards\""));
        assertTrue(json.contains("\"index_uuid\":\"idx-a\""));
    }
}
