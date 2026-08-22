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

public class ShardWriteRateEntryTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        ShardWriteRateEntry original = new ShardWriteRateEntry("idx-uuid", 3, 1200L, 4096L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardWriteRateEntry deserialized = new ShardWriteRateEntry(out.bytes().streamInput());

        assertEquals(original, deserialized);
        assertEquals("idx-uuid", deserialized.indexUuid());
        assertEquals(3, deserialized.shardId());
        assertEquals(1200L, deserialized.writesPerMinute());
        assertEquals(4096L, deserialized.shardSizeInBytes());
    }

    public void testToXContent() throws Exception {
        ShardWriteRateEntry entry = new ShardWriteRateEntry("idx-uuid", 3, 1200L, 4096L);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        entry.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"index_uuid\":\"idx-uuid\""));
        assertTrue(json.contains("\"shard_id\":3"));
        assertTrue(json.contains("\"writes_per_minute\":1200"));
        assertTrue(json.contains("\"shard_size_in_bytes\":4096"));
    }

    public void testEqualsAndHashCode() {
        ShardWriteRateEntry a = new ShardWriteRateEntry("idx-uuid", 3, 1200L, 4096L);
        ShardWriteRateEntry sameValues = new ShardWriteRateEntry("idx-uuid", 3, 1200L, 4096L);
        ShardWriteRateEntry differentShard = new ShardWriteRateEntry("idx-uuid", 4, 1200L, 4096L);

        assertEquals(a, sameValues);
        assertEquals(a.hashCode(), sameValues.hashCode());
        assertNotEquals(a, differentShard);
    }
}
