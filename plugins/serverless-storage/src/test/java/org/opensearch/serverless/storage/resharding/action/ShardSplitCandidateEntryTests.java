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

public class ShardSplitCandidateEntryTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        ShardSplitCandidateEntry original = new ShardSplitCandidateEntry("idx-uuid", 2, "my-index", 15_000L, 500L, true, false);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardSplitCandidateEntry deserialized = new ShardSplitCandidateEntry(out.bytes().streamInput());

        assertEquals(original, deserialized);
        assertEquals("idx-uuid", deserialized.indexUuid());
        assertEquals(2, deserialized.shardId());
        assertEquals("my-index", deserialized.indexName());
        assertEquals(15_000L, deserialized.writesPerMinute());
        assertEquals(500L, deserialized.shardSizeInBytes());
        assertTrue(deserialized.writeRateCandidate());
        assertFalse(deserialized.sizeCandidate());
        assertTrue(deserialized.candidate());
    }

    public void testSerializationRoundTripWithUnknownSignal() throws Exception {
        ShardSplitCandidateEntry original = new ShardSplitCandidateEntry(
            "idx-uuid",
            0,
            "my-index",
            ShardSplitCandidateEntry.UNKNOWN,
            ShardSplitCandidateEntry.UNKNOWN,
            false,
            false
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardSplitCandidateEntry deserialized = new ShardSplitCandidateEntry(out.bytes().streamInput());

        assertEquals(original, deserialized);
        assertEquals(ShardSplitCandidateEntry.UNKNOWN, deserialized.writesPerMinute());
        assertEquals(ShardSplitCandidateEntry.UNKNOWN, deserialized.shardSizeInBytes());
        assertFalse(deserialized.candidate());
    }

    public void testCandidateIsTrueWhenEitherSignalTriggers() {
        ShardSplitCandidateEntry sizeOnly = new ShardSplitCandidateEntry("idx-uuid", 0, "my-index", 10L, 999_999_999L, false, true);
        assertTrue(sizeOnly.candidate());
        assertFalse(sizeOnly.writeRateCandidate());
        assertTrue(sizeOnly.sizeCandidate());
    }

    public void testToXContent() throws Exception {
        ShardSplitCandidateEntry entry = new ShardSplitCandidateEntry("idx-uuid", 2, "my-index", 15_000L, 500L, true, false);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        entry.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"index_uuid\":\"idx-uuid\""));
        assertTrue(json.contains("\"shard_id\":2"));
        assertTrue(json.contains("\"index_name\":\"my-index\""));
        assertTrue(json.contains("\"writes_per_minute\":15000"));
        assertTrue(json.contains("\"shard_size_in_bytes\":500"));
        assertTrue(json.contains("\"write_rate_candidate\":true"));
        assertTrue(json.contains("\"size_candidate\":false"));
        assertTrue(json.contains("\"candidate\":true"));
    }

    public void testEqualsAndHashCode() {
        ShardSplitCandidateEntry a = new ShardSplitCandidateEntry("idx-uuid", 2, "my-index", 15_000L, 500L, true, false);
        ShardSplitCandidateEntry sameValues = new ShardSplitCandidateEntry("idx-uuid", 2, "my-index", 15_000L, 500L, true, false);
        ShardSplitCandidateEntry differentShard = new ShardSplitCandidateEntry("idx-uuid", 3, "my-index", 15_000L, 500L, true, false);

        assertEquals(a, sameValues);
        assertEquals(a.hashCode(), sameValues.hashCode());
        assertNotEquals(a, differentShard);
    }
}
