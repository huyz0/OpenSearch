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

public class ShardLagEntryTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        ShardLagEntry original = new ShardLagEntry("idx-uuid", 2, 5L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardLagEntry deserialized = new ShardLagEntry(out.bytes().streamInput());

        assertEquals(original, deserialized);
        assertEquals("idx-uuid", deserialized.indexUuid());
        assertEquals(2, deserialized.shardId());
        assertEquals(5L, deserialized.manifestGenerationLag());
    }

    public void testToXContent() throws Exception {
        ShardLagEntry entry = new ShardLagEntry("idx-uuid", 2, 5L);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        entry.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"index_uuid\":\"idx-uuid\""));
        assertTrue(json.contains("\"shard_id\":2"));
        assertTrue(json.contains("\"manifest_generation_lag\":5"));
    }

    public void testEqualsAndHashCode() {
        ShardLagEntry a = new ShardLagEntry("idx-uuid", 2, 5L);
        ShardLagEntry sameValues = new ShardLagEntry("idx-uuid", 2, 5L);
        ShardLagEntry differentLag = new ShardLagEntry("idx-uuid", 2, 6L);

        assertEquals(a, sameValues);
        assertEquals(a.hashCode(), sameValues.hashCode());
        assertNotEquals(a, differentLag);
    }
}
