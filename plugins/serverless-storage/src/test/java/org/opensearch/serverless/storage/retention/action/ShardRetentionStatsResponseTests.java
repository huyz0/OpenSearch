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

public class ShardRetentionStatsResponseTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        ShardRetentionStatsResponse original = new ShardRetentionStatsResponse(5, 2, 8, 3, 4, 1, 1_800_000L, 604_800_000L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardRetentionStatsResponse deserialized = new ShardRetentionStatsResponse(out.bytes().streamInput());

        assertEquals(original.manifestCount(), deserialized.manifestCount());
        assertEquals(original.deletableManifestCount(), deserialized.deletableManifestCount());
        assertEquals(original.bundleCount(), deserialized.bundleCount());
        assertEquals(original.deletableBundleCount(), deserialized.deletableBundleCount());
        assertEquals(original.durablePinCount(), deserialized.durablePinCount());
        assertEquals(original.pitrPinCount(), deserialized.pitrPinCount());
        assertEquals(original.gcRetentionWindowMillis(), deserialized.gcRetentionWindowMillis());
        assertEquals(original.pitrWindowMillis(), deserialized.pitrWindowMillis());
    }

    public void testSerializationRoundTripWithADisabledPitrWindow() throws Exception {
        // A non-positive pitrWindowMillis is how "PITR disabled on this node" is represented --
        // must round-trip correctly through writeZLong/readZLong, not just positive values.
        ShardRetentionStatsResponse original = new ShardRetentionStatsResponse(0, 0, 0, 0, 0, 0, 0L, -1L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardRetentionStatsResponse deserialized = new ShardRetentionStatsResponse(out.bytes().streamInput());

        assertEquals(-1L, deserialized.pitrWindowMillis());
    }

    public void testToXContent() throws Exception {
        ShardRetentionStatsResponse response = new ShardRetentionStatsResponse(5, 2, 8, 3, 4, 1, 1_800_000L, 604_800_000L);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"manifest_count\":5"));
        assertTrue(json.contains("\"deletable_manifest_count\":2"));
        assertTrue(json.contains("\"bundle_count\":8"));
        assertTrue(json.contains("\"deletable_bundle_count\":3"));
        assertTrue(json.contains("\"durable_pin_count\":4"));
        assertTrue(json.contains("\"pitr_pin_count\":1"));
        assertTrue(json.contains("\"gc_retention_window_millis\":1800000"));
        assertTrue(json.contains("\"pitr_window_millis\":604800000"));
    }
}
