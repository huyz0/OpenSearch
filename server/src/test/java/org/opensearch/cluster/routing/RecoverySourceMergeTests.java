/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.cluster.metadata.ShardRange;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

public class RecoverySourceMergeTests extends OpenSearchTestCase {

    public void testInPlaceMergeShardRecoverySourceType() {
        RecoverySource.InPlaceMergeShardRecoverySource source = RecoverySource.InPlaceMergeShardRecoverySource.INSTANCE;
        assertEquals(RecoverySource.Type.IN_PLACE_MERGE_SHARD, source.getType());
    }

    public void testInPlaceMergeShardRecoverySourceIsSingleton() {
        assertSame(RecoverySource.InPlaceMergeShardRecoverySource.INSTANCE, RecoverySource.InPlaceMergeShardRecoverySource.INSTANCE);
    }

    public void testInPlaceMergeShardRecoverySourceDoesNotExpectEmptyRetentionLeases() {
        assertFalse(RecoverySource.InPlaceMergeShardRecoverySource.INSTANCE.expectEmptyRetentionLeases());
    }

    public void testInPlaceMergeShardRecoverySourceToString() {
        assertEquals("in-place shard merge", RecoverySource.InPlaceMergeShardRecoverySource.INSTANCE.toString());
    }

    public void testSerializationRoundTripOfEmptyInstance() throws IOException {
        RecoverySource source = RecoverySource.InPlaceMergeShardRecoverySource.INSTANCE;

        BytesStreamOutput out = new BytesStreamOutput();
        source.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        RecoverySource deserialized = RecoverySource.readFrom(in);

        assertEquals(RecoverySource.InPlaceMergeShardRecoverySource.INSTANCE, deserialized);
        assertTrue(((RecoverySource.InPlaceMergeShardRecoverySource) deserialized).children().isEmpty());
    }

    public void testSerializationRoundTripPreservesChildren() throws IOException {
        List<ShardRange> children = List.of(new ShardRange(1, Integer.MIN_VALUE, -1), new ShardRange(2, 0, Integer.MAX_VALUE));
        RecoverySource source = new RecoverySource.InPlaceMergeShardRecoverySource(children);

        BytesStreamOutput out = new BytesStreamOutput();
        source.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        RecoverySource deserialized = RecoverySource.readFrom(in);

        assertEquals(source, deserialized);
        assertEquals(children, ((RecoverySource.InPlaceMergeShardRecoverySource) deserialized).children());
    }

    public void testTypeEnumOrdinalStability() {
        // IN_PLACE_MERGE_SHARD must be at the end to preserve ordinals of every existing type,
        // including IN_PLACE_SPLIT_SHARD immediately before it.
        RecoverySource.Type[] types = RecoverySource.Type.values();
        assertEquals(RecoverySource.Type.IN_PLACE_MERGE_SHARD, types[types.length - 1]);
    }
}
