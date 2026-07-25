/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * C3b, first slice. {@link UploadedManifestShard} is the reference from the top-level manifest to one
 * shard of its index list. Nothing writes or reads a shard blob yet, so these tests are the whole of
 * its contract: the wire and XContent forms round-trip, unknown fields are tolerated, and required
 * ones are not silently defaulted.
 */
public class UploadedManifestShardTests extends OpenSearchTestCase {

    public void testStreamRoundTrip() throws IOException {
        UploadedManifestShard shard = new UploadedManifestShard(17, "1__42__17__abcdef", 390);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            shard.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(shard, new UploadedManifestShard(in));
            }
        }
    }

    public void testXContentRoundTrip() throws IOException {
        UploadedManifestShard shard = new UploadedManifestShard(0, "1__1__0__deadbeef", 0);
        assertEquals(shard, parse(toJson(shard)));
    }

    /**
     * A shard reference written by a newer node may carry fields this one does not know. Skipping them
     * rather than failing is what lets a mixed-version cluster read each other's manifests at all --
     * the same choice {@link IndexDescriptor} makes, for the same reason.
     */
    public void testUnknownFieldsAreSkipped() throws IOException {
        String json = "{\"shard_id\":3,\"blob_name\":\"b\",\"entry_count\":9,\"future_field\":{\"nested\":[1,2]}}";
        UploadedManifestShard parsed = parse(json);

        assertEquals(3, parsed.getShardId());
        assertEquals("b", parsed.getBlobName());
        assertEquals(9, parsed.getEntryCount());
    }

    /**
     * The entry count exists so a truncated shard is detectable rather than silent, which only works
     * if a missing one is an error. Defaulting it to zero would make a partially written shard look
     * like an empty one, and the cleanup sweep deletes by subtraction -- an empty-looking shard
     * references nothing and everything it actually named becomes garbage.
     */
    public void testMissingRequiredFieldsAreRejected() {
        expectThrows(IOException.class, () -> parse("{\"shard_id\":1,\"blob_name\":\"b\"}"));
        expectThrows(IOException.class, () -> parse("{\"shard_id\":1,\"entry_count\":5}"));
        expectThrows(IOException.class, () -> parse("{\"blob_name\":\"b\",\"entry_count\":5}"));
    }

    public void testInvalidValuesAreRejectedAtConstruction() {
        expectThrows(IllegalArgumentException.class, () -> new UploadedManifestShard(-1, "b", 0));
        expectThrows(IllegalArgumentException.class, () -> new UploadedManifestShard(0, "b", -1));
        expectThrows(NullPointerException.class, () -> new UploadedManifestShard(0, null, 0));
    }

    public void testEqualsAndHashCodeCoverEveryField() {
        UploadedManifestShard base = new UploadedManifestShard(1, "b", 2);

        assertEquals(base, new UploadedManifestShard(1, "b", 2));
        assertEquals(base.hashCode(), new UploadedManifestShard(1, "b", 2).hashCode());
        assertNotEquals(base, new UploadedManifestShard(2, "b", 2));
        assertNotEquals(base, new UploadedManifestShard(1, "other", 2));
        assertNotEquals("a shard reference is not equal to its blob name", base, new UploadedManifestShard(1, "b", 3));
    }

    private static String toJson(UploadedManifestShard shard) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        shard.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return BytesReference.bytes(builder).utf8ToString();
    }

    private UploadedManifestShard parse(String json) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            return UploadedManifestShard.fromXContent(parser);
        }
    }
}
