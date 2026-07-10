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

public class IdleShardEntryTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        IdleShardEntry original = new IdleShardEntry("idx-uuid", 2, 4200L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        IdleShardEntry deserialized = new IdleShardEntry(out.bytes().streamInput());

        assertEquals(original, deserialized);
        assertEquals("idx-uuid", deserialized.indexUuid());
        assertEquals(2, deserialized.shardId());
        assertEquals(4200L, deserialized.millisSinceLastActivity());
    }

    public void testToXContent() throws Exception {
        IdleShardEntry entry = new IdleShardEntry("idx-uuid", 2, 4200L);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        entry.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"index_uuid\":\"idx-uuid\""));
        assertTrue(json.contains("\"shard_id\":2"));
        assertTrue(json.contains("\"millis_since_last_activity\":4200"));
    }

    public void testEqualsAndHashCode() {
        IdleShardEntry a = new IdleShardEntry("idx-uuid", 2, 4200L);
        IdleShardEntry sameValues = new IdleShardEntry("idx-uuid", 2, 4200L);
        IdleShardEntry differentShard = new IdleShardEntry("idx-uuid", 3, 4200L);

        assertEquals(a, sameValues);
        assertEquals(a.hashCode(), sameValues.hashCode());
        assertNotEquals(a, differentShard);
    }
}
