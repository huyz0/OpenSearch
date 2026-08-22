/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

public class ShardPartitionDescriptorTests extends OpenSearchTestCase {

    public void testRejectsFewerThanTwoPartitions() {
        expectThrows(IllegalArgumentException.class, () -> new ShardPartitionDescriptor(0, 1));
        expectThrows(IllegalArgumentException.class, () -> new ShardPartitionDescriptor(0, 0));
    }

    public void testRejectsAnOutOfRangePartitionIndex() {
        expectThrows(IllegalArgumentException.class, () -> new ShardPartitionDescriptor(2, 2));
        expectThrows(IllegalArgumentException.class, () -> new ShardPartitionDescriptor(-1, 2));
    }

    public void testAcceptsAWellFormedDescriptor() {
        ShardPartitionDescriptor descriptor = new ShardPartitionDescriptor(1, 3);
        assertEquals(1, descriptor.partitionIndex());
        assertEquals(3, descriptor.numPartitions());
    }

    public void testSerializationRoundTrip() throws Exception {
        ShardPartitionDescriptor original = new ShardPartitionDescriptor(2, 5);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardPartitionDescriptor deserialized = new ShardPartitionDescriptor(out.bytes().streamInput());

        assertEquals(original, deserialized);
    }
}
