/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.opensearch.test.OpenSearchTestCase;

public class RoutingPartitionFilterTests extends OpenSearchTestCase {

    public void testEveryIdMatchesExactlyOnePartitionOfAFixedNumPartitions() {
        int numPartitions = 4;
        for (int i = 0; i < 1000; i++) {
            String id = "doc-" + i;
            int matches = 0;
            for (int partitionIndex = 0; partitionIndex < numPartitions; partitionIndex++) {
                if (RoutingPartitionFilter.matches(id, new ShardPartitionDescriptor(partitionIndex, numPartitions))) {
                    matches++;
                }
            }
            assertEquals("id [" + id + "] must match exactly one of " + numPartitions + " partitions", 1, matches);
        }
    }

    public void testIsDeterministicForTheSameIdAndDescriptor() {
        ShardPartitionDescriptor descriptor = new ShardPartitionDescriptor(1, 3);
        boolean first = RoutingPartitionFilter.matches("some-doc-id", descriptor);
        for (int i = 0; i < 50; i++) {
            assertEquals(first, RoutingPartitionFilter.matches("some-doc-id", descriptor));
        }
    }

    public void testDistributesRoughlyEvenlyAcrossPartitionsForManyIds() {
        int numPartitions = 5;
        int[] counts = new int[numPartitions];
        int total = 5000;
        for (int i = 0; i < total; i++) {
            String id = "distribution-test-doc-" + i;
            for (int partitionIndex = 0; partitionIndex < numPartitions; partitionIndex++) {
                if (RoutingPartitionFilter.matches(id, new ShardPartitionDescriptor(partitionIndex, numPartitions))) {
                    counts[partitionIndex]++;
                }
            }
        }
        int expectedPerPartition = total / numPartitions;
        for (int count : counts) {
            assertTrue(
                "partition counts should be roughly balanced for a well-distributed hash -- got "
                    + count
                    + ", expected around "
                    + expectedPerPartition,
                Math.abs(count - expectedPerPartition) < expectedPerPartition * 0.25
            );
        }
    }
}
