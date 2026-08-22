/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class NodeCacheStatsResponseTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        NodeCacheStatsResponse original = new NodeCacheStatsResponse(
            5L,
            2L,
            List.of(new ShardCacheStatsEntry("idx-a", 0, 10L, 3L, 7L), new ShardCacheStatsEntry("idx-b", 1, 1L, 1L, 0L))
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        NodeCacheStatsResponse deserialized = new NodeCacheStatsResponse(out.bytes().streamInput());

        assertEquals(original.inMemoryCacheHitCount(), deserialized.inMemoryCacheHitCount());
        assertEquals(original.inMemoryCacheMissCount(), deserialized.inMemoryCacheMissCount());
        assertEquals(original.diskCacheEntries(), deserialized.diskCacheEntries());
    }

    public void testSerializationRoundTripWithNoDiskCacheEntries() throws Exception {
        NodeCacheStatsResponse original = new NodeCacheStatsResponse(0L, 0L, List.of());

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        NodeCacheStatsResponse deserialized = new NodeCacheStatsResponse(out.bytes().streamInput());

        assertTrue(deserialized.diskCacheEntries().isEmpty());
    }

    public void testToXContent() throws Exception {
        NodeCacheStatsResponse response = new NodeCacheStatsResponse(5L, 2L, List.of(new ShardCacheStatsEntry("idx-a", 0, 10L, 3L, 7L)));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"in_memory_cache\""));
        assertTrue(json.contains("\"disk_caches\""));
        assertTrue(json.contains("\"index_uuid\":\"idx-a\""));
        assertTrue(json.contains("\"average_cold_read_latency_millis\":7"));
    }
}
