/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.directory;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryShardDirectoryTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;

    public void testLookupOnAnUnreportedShardIsEmpty() {
        ShardDirectory directory = new InMemoryShardDirectory();
        assertTrue(directory.lookup(INDEX_UUID, SHARD_ID).isEmpty());
    }

    public void testReportThenLookupReturnsTheEntry() {
        ShardDirectory directory = new InMemoryShardDirectory();
        ShardDirectoryEntry entry = new ShardDirectoryEntry("node-1", ShardRole.WRITER, 1, 5, Long.MAX_VALUE);
        directory.report(INDEX_UUID, SHARD_ID, entry);

        Optional<ShardDirectoryEntry> looked = directory.lookup(INDEX_UUID, SHARD_ID);
        assertTrue(looked.isPresent());
        assertEquals(entry, looked.get());
    }

    public void testReportOverwritesAPreviousEntryForTheSameShard() {
        ShardDirectory directory = new InMemoryShardDirectory();
        directory.report(INDEX_UUID, SHARD_ID, new ShardDirectoryEntry("node-1", ShardRole.WRITER, 1, 5, Long.MAX_VALUE));
        directory.report(INDEX_UUID, SHARD_ID, new ShardDirectoryEntry("node-2", ShardRole.WRITER, 2, 0, Long.MAX_VALUE));

        ShardDirectoryEntry current = directory.lookup(INDEX_UUID, SHARD_ID).orElseThrow();
        assertEquals("node-2", current.nodeId());
        assertEquals(2, current.primaryTerm());
    }

    public void testDropRemovesTheEntry() {
        ShardDirectory directory = new InMemoryShardDirectory();
        directory.report(INDEX_UUID, SHARD_ID, new ShardDirectoryEntry("node-1", ShardRole.READER, 1, 0, Long.MAX_VALUE));
        directory.drop(INDEX_UUID, SHARD_ID);
        assertTrue(directory.lookup(INDEX_UUID, SHARD_ID).isEmpty());
    }

    public void testDroppingAnUnreportedShardIsANoOp() {
        ShardDirectory directory = new InMemoryShardDirectory();
        directory.drop(INDEX_UUID, SHARD_ID);
        assertTrue(directory.lookup(INDEX_UUID, SHARD_ID).isEmpty());
    }

    public void testDifferentShardsAreIndependent() {
        ShardDirectory directory = new InMemoryShardDirectory();
        directory.report(INDEX_UUID, 0, new ShardDirectoryEntry("node-1", ShardRole.WRITER, 1, 0, Long.MAX_VALUE));
        directory.report(INDEX_UUID, 1, new ShardDirectoryEntry("node-2", ShardRole.READER, 1, 0, Long.MAX_VALUE));

        assertEquals("node-1", directory.lookup(INDEX_UUID, 0).orElseThrow().nodeId());
        assertEquals("node-2", directory.lookup(INDEX_UUID, 1).orElseThrow().nodeId());
    }

    public void testEntryIsHiddenOnceItsTtlExpires() {
        AtomicLong now = new AtomicLong(1_000L);
        InMemoryShardDirectory directory = new InMemoryShardDirectory(now::get);
        directory.report(INDEX_UUID, SHARD_ID, new ShardDirectoryEntry("node-1", ShardRole.WRITER, 1, 0, 2_000L));

        assertTrue("not yet expired", directory.lookup(INDEX_UUID, SHARD_ID).isPresent());

        now.set(2_000L); // isExpired() is now-inclusive at the boundary
        assertTrue("expired at the boundary", directory.lookup(INDEX_UUID, SHARD_ID).isEmpty());
    }

    public void testExpiredEntryIsLazilyRemovedNotJustHidden() {
        AtomicLong now = new AtomicLong(1_000L);
        InMemoryShardDirectory directory = new InMemoryShardDirectory(now::get);
        directory.report(INDEX_UUID, SHARD_ID, new ShardDirectoryEntry("node-1", ShardRole.WRITER, 1, 0, 2_000L));
        assertEquals(1, directory.size());

        now.set(5_000L);
        assertTrue(directory.lookup(INDEX_UUID, SHARD_ID).isEmpty());
        assertEquals("the lookup that observed the expiry should also have cleaned it up", 0, directory.size());
    }

    public void testConcurrentReportsAndLookupsAcrossManyShardsDoNotCorruptState() throws Exception {
        ShardDirectory directory = new InMemoryShardDirectory();
        int shardCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(16);
        CountDownLatch startLatch = new CountDownLatch(1);
        try {
            List<Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < shardCount; i++) {
                final int shardId = i;
                futures.add(executor.submit(() -> {
                    startLatch.await();
                    directory.report(
                        INDEX_UUID,
                        shardId,
                        new ShardDirectoryEntry("node-" + shardId, ShardRole.WRITER, 1, 0, Long.MAX_VALUE)
                    );
                    return null;
                }));
            }
            startLatch.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
        }

        for (int i = 0; i < shardCount; i++) {
            assertEquals("node-" + i, directory.lookup(INDEX_UUID, i).orElseThrow().nodeId());
        }
    }
}
