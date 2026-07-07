/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.directory;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PartitionedShardDirectoryTests extends OpenSearchTestCase {

    private static List<ShardDirectory> newPartitions(int count) {
        List<ShardDirectory> partitions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            partitions.add(new InMemoryShardDirectory());
        }
        return partitions;
    }

    public void testConstructorRejectsAnEmptyPartitionList() {
        expectThrows(IllegalArgumentException.class, () -> new PartitionedShardDirectory(List.of()));
    }

    public void testReportThenLookupReturnsTheEntryThroughTheCorrectPartition() {
        List<ShardDirectory> partitions = newPartitions(8);
        PartitionedShardDirectory directory = new PartitionedShardDirectory(partitions);

        ShardDirectoryEntry entry = new ShardDirectoryEntry("node-1", ShardRole.WRITER, 1, 0, Long.MAX_VALUE);
        directory.report("idx", 7, entry);

        assertEquals(Optional.of(entry), directory.lookup("idx", 7));

        int owningPartition = directory.partitionIndexFor("idx", 7);
        // The entry actually landed in the partition the ring says owns this key, not just
        // somewhere reachable through the facade -- proves routing is deterministic, not a
        // fallback scan across every partition.
        assertEquals(Optional.of(entry), partitions.get(owningPartition).lookup("idx", 7));
    }

    public void testTheSameShardAlwaysRoutesToTheSamePartition() {
        PartitionedShardDirectory directory = new PartitionedShardDirectory(newPartitions(16));
        int first = directory.partitionIndexFor("idx", 42);
        for (int i = 0; i < 100; i++) {
            assertEquals(first, directory.partitionIndexFor("idx", 42));
        }
    }

    public void testDifferentShardsSpreadAcrossMultiplePartitions() {
        PartitionedShardDirectory directory = new PartitionedShardDirectory(newPartitions(8));
        java.util.Set<Integer> touchedPartitions = new java.util.HashSet<>();
        for (int shardId = 0; shardId < 1000; shardId++) {
            touchedPartitions.add(directory.partitionIndexFor("idx", shardId));
        }
        // Not asserting a precise distribution (that's a property of SHA-256, not this class'
        // logic) -- just that 1000 distinct shards don't all collapse onto one or two partitions,
        // which would indicate the ring is broken rather than merely imperfectly balanced.
        assertTrue("expected shards to spread across most of the 8 partitions, got " + touchedPartitions, touchedPartitions.size() >= 6);
    }

    public void testDropOnlyRemovesFromTheOwningPartitionLeavingOthersUntouched() {
        List<ShardDirectory> partitions = newPartitions(4);
        PartitionedShardDirectory directory = new PartitionedShardDirectory(partitions);

        directory.report("idx", 1, new ShardDirectoryEntry("node-1", ShardRole.WRITER, 1, 0, Long.MAX_VALUE));
        directory.report("idx", 2, new ShardDirectoryEntry("node-2", ShardRole.WRITER, 1, 0, Long.MAX_VALUE));

        directory.drop("idx", 1);

        assertTrue(directory.lookup("idx", 1).isEmpty());
        assertTrue(directory.lookup("idx", 2).isPresent());
    }

    public void testIndependentPartitionedDirectoriesWithTheSamePartitionCountAgreeOnRouting() {
        // Simulates two clients that each build their own PartitionedShardDirectory against the
        // same partition count/order -- the whole point of consistent hashing over an interface
        // like round-robin is that routing is a pure function of the key, not shared runtime state.
        PartitionedShardDirectory directoryA = new PartitionedShardDirectory(newPartitions(12));
        PartitionedShardDirectory directoryB = new PartitionedShardDirectory(newPartitions(12));
        for (int shardId = 0; shardId < 200; shardId++) {
            assertEquals(directoryA.partitionIndexFor("idx", shardId), directoryB.partitionIndexFor("idx", shardId));
        }
    }
}
