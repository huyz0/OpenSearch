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
}
