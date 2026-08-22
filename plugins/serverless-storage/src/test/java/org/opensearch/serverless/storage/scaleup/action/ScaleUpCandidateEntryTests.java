/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaleup.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class ScaleUpCandidateEntryTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        ScaleUpCandidateEntry original = new ScaleUpCandidateEntry("idx-uuid", 2, "my-index", 900L, 1, true);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ScaleUpCandidateEntry deserialized = new ScaleUpCandidateEntry(out.bytes().streamInput());

        assertEquals(original, deserialized);
        assertEquals("idx-uuid", deserialized.indexUuid());
        assertEquals(2, deserialized.shardId());
        assertEquals("my-index", deserialized.indexName());
        assertEquals(900L, deserialized.queriesPerMinute());
        assertEquals(1, deserialized.currentSearchReplicaCount());
        assertTrue(deserialized.candidate());
    }

    public void testToXContent() throws Exception {
        ScaleUpCandidateEntry entry = new ScaleUpCandidateEntry("idx-uuid", 2, "my-index", 900L, 1, true);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        entry.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"index_uuid\":\"idx-uuid\""));
        assertTrue(json.contains("\"shard_id\":2"));
        assertTrue(json.contains("\"index_name\":\"my-index\""));
        assertTrue(json.contains("\"queries_per_minute\":900"));
        assertTrue(json.contains("\"current_search_replica_count\":1"));
        assertTrue(json.contains("\"candidate\":true"));
    }

    public void testEqualsAndHashCode() {
        ScaleUpCandidateEntry a = new ScaleUpCandidateEntry("idx-uuid", 2, "my-index", 900L, 1, true);
        ScaleUpCandidateEntry sameValues = new ScaleUpCandidateEntry("idx-uuid", 2, "my-index", 900L, 1, true);
        ScaleUpCandidateEntry differentShard = new ScaleUpCandidateEntry("idx-uuid", 3, "my-index", 900L, 1, true);

        assertEquals(a, sameValues);
        assertEquals(a.hashCode(), sameValues.hashCode());
        assertNotEquals(a, differentShard);
    }
}
