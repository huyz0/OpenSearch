/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class CacheStatsRegistryTests extends OpenSearchTestCase {

    public void testSnapshotAllReflectsARegisteredShardsLiveStats() throws Exception {
        SegmentBundle bundle = BundleWriter.write(
            List.of(new BundleFileContent("a.bin", "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8)))
        );
        BundleFileEntry entry = bundle.entries().get("a.bin");
        BundleFileReader delegate = (bundleName, e) -> BundleReader.extractFile(bundle.bytes(), e);
        LocalDiskCachingBundleStore diskCache = new LocalDiskCachingBundleStore(delegate, createTempDir());
        CacheStatsRegistry registry = new CacheStatsRegistry();
        registry.register("idx-a", 2, diskCache);

        diskCache.readFile("bundle-1", entry); // miss
        diskCache.readFile("bundle-1", entry); // hit

        List<CacheStatsRegistry.ShardCacheStats> snapshot = registry.snapshotAll();
        assertEquals(1, snapshot.size());
        CacheStatsRegistry.ShardCacheStats stats = snapshot.get(0);
        assertEquals("idx-a", stats.indexUuid());
        assertEquals(2, stats.shardId());
        assertEquals(1L, stats.hitCount());
        assertEquals(1L, stats.missCount());
    }

    public void testSnapshotAllIsEmptyWithNothingRegistered() {
        assertTrue(new CacheStatsRegistry().snapshotAll().isEmpty());
    }

    public void testRegisteringUnderTheSameKeyReplacesThePreviousEntry() throws Exception {
        SegmentBundle bundle = BundleWriter.write(
            List.of(new BundleFileContent("a.bin", "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8)))
        );
        BundleFileReader delegate = (bundleName, e) -> BundleReader.extractFile(bundle.bytes(), e);
        LocalDiskCachingBundleStore first = new LocalDiskCachingBundleStore(delegate, createTempDir());
        LocalDiskCachingBundleStore second = new LocalDiskCachingBundleStore(delegate, createTempDir());
        CacheStatsRegistry registry = new CacheStatsRegistry();

        registry.register("idx-a", 0, first);
        first.readFile("bundle-1", bundle.entries().get("a.bin"));
        registry.register("idx-a", 0, second);

        List<CacheStatsRegistry.ShardCacheStats> snapshot = registry.snapshotAll();
        assertEquals(1, snapshot.size());
        assertEquals("the second registration must replace the first, not add a duplicate", 0L, snapshot.get(0).missCount());
    }

    /**
     * A shard whose local data -- cache directory included -- has been wiped from this node must
     * stop being reported. The WeakReference means a stale entry is not a leak, but it is a lie
     * until collection, and nothing here can force that collection.
     */
    public void testDeregisterRemovesExactlyOneShardAndLeavesTheRest() throws Exception {
        LocalDiskCachingBundleStore one = new LocalDiskCachingBundleStore(unusedReader(), createTempDir());
        LocalDiskCachingBundleStore two = new LocalDiskCachingBundleStore(unusedReader(), createTempDir());
        LocalDiskCachingBundleStore otherIndex = new LocalDiskCachingBundleStore(unusedReader(), createTempDir());
        CacheStatsRegistry registry = new CacheStatsRegistry();
        registry.register("idx-a", 0, one);
        registry.register("idx-a", 1, two);
        registry.register("idx-b", 0, otherIndex);

        registry.deregister("idx-a", 0);
        // Idempotent: the lifecycle callback can fire for a shard that never registered here.
        registry.deregister("idx-a", 9);

        List<CacheStatsRegistry.ShardCacheStats> snapshot = registry.snapshotAll();
        assertEquals(2, snapshot.size());
        assertTrue(snapshot.stream().noneMatch(s -> s.indexUuid().equals("idx-a") && s.shardId() == 0));
        assertTrue(snapshot.stream().anyMatch(s -> s.indexUuid().equals("idx-a") && s.shardId() == 1));
        assertTrue(snapshot.stream().anyMatch(s -> s.indexUuid().equals("idx-b") && s.shardId() == 0));
    }

    public void testDeregisterIndexRemovesEveryShardOfThatIndexAndNothingElse() throws Exception {
        LocalDiskCachingBundleStore one = new LocalDiskCachingBundleStore(unusedReader(), createTempDir());
        LocalDiskCachingBundleStore two = new LocalDiskCachingBundleStore(unusedReader(), createTempDir());
        LocalDiskCachingBundleStore otherIndex = new LocalDiskCachingBundleStore(unusedReader(), createTempDir());
        CacheStatsRegistry registry = new CacheStatsRegistry();
        registry.register("idx-a", 0, one);
        registry.register("idx-a", 11, two);
        registry.register("idx-b", 0, otherIndex);

        registry.deregisterIndex("idx-a");

        List<CacheStatsRegistry.ShardCacheStats> snapshot = registry.snapshotAll();
        assertEquals(1, snapshot.size());
        assertEquals("idx-b", snapshot.get(0).indexUuid());
    }

    /** A reader that must never be called -- these registry tests only care about keys, never about reads. */
    private static BundleFileReader unusedReader() {
        return (bundleName, entry) -> { throw new AssertionError("no read expected"); };
    }
}
