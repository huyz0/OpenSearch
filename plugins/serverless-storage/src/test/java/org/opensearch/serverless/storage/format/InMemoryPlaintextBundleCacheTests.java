/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryPlaintextBundleCacheTests extends OpenSearchTestCase {

    private static final class CountingBundleFileReader implements BundleFileReader {
        private final BundleFileReader delegate;
        final AtomicInteger callCount = new AtomicInteger();

        CountingBundleFileReader(BundleFileReader delegate) {
            this.delegate = delegate;
        }

        @Override
        public byte[] readFile(String bundleName, BundleFileEntry entry) throws IOException {
            callCount.incrementAndGet();
            return delegate.readFile(bundleName, entry);
        }
    }

    private static SegmentBundle writeSampleBundle() {
        return BundleWriter.write(List.of(new BundleFileContent("a.bin", "hello".getBytes(StandardCharsets.UTF_8))));
    }

    private static BundleFileReader inMemoryReader(SegmentBundle bundle) {
        return (bundleName, entry) -> BundleReader.extractFile(bundle.bytes(), entry);
    }

    public void testConstructorRejectsNegativeMaxBytes() {
        expectThrows(IllegalArgumentException.class, () -> new InMemoryPlaintextBundleCache(-1));
    }

    public void testSecondReadIsAHitAndDoesNotCallTheDelegateAgain() throws Exception {
        SegmentBundle bundle = writeSampleBundle();
        BundleFileEntry entry = bundle.entries().get("a.bin");
        CountingBundleFileReader counting = new CountingBundleFileReader(inMemoryReader(bundle));
        InMemoryPlaintextBundleCache cache = new InMemoryPlaintextBundleCache(1024);

        byte[] first = cache.readFile("bundle-1", entry, counting);
        byte[] second = cache.readFile("bundle-1", entry, counting);

        assertArrayEquals(first, second);
        assertEquals(1, counting.callCount.get());
        assertEquals(1, cache.missCount());
        assertEquals(1, cache.hitCount());
    }

    public void testEvictsLeastRecentlyUsedEntryOnceOverBudget() throws Exception {
        SegmentBundle bundle = BundleWriter.write(
            List.of(
                new BundleFileContent("a.bin", "aaaaa".getBytes(StandardCharsets.UTF_8)), // 5 bytes
                new BundleFileContent("b.bin", "bbbbb".getBytes(StandardCharsets.UTF_8)), // 5 bytes
                new BundleFileContent("c.bin", "ccccc".getBytes(StandardCharsets.UTF_8))  // 5 bytes
            )
        );
        CountingBundleFileReader counting = new CountingBundleFileReader(inMemoryReader(bundle));
        // Room for exactly 2 entries (10 bytes) -- the third eviction candidate.
        InMemoryPlaintextBundleCache cache = new InMemoryPlaintextBundleCache(10);

        cache.readFile("bundle-1", bundle.entries().get("a.bin"), counting);
        cache.readFile("bundle-1", bundle.entries().get("b.bin"), counting);
        // touch a.bin again so b.bin becomes the least-recently-used of the two
        cache.readFile("bundle-1", bundle.entries().get("a.bin"), counting);
        assertEquals("a.bin should still be a hit here", 2, counting.callCount.get());

        // c.bin's insertion must evict b.bin (least recently used), not a.bin.
        cache.readFile("bundle-1", bundle.entries().get("c.bin"), counting);
        assertEquals(3, counting.callCount.get());

        cache.readFile("bundle-1", bundle.entries().get("a.bin"), counting);
        assertEquals("a.bin should still be cached", 3, counting.callCount.get());

        cache.readFile("bundle-1", bundle.entries().get("b.bin"), counting);
        assertEquals("b.bin should have been evicted and re-fetched", 4, counting.callCount.get());
    }

    public void testAnEntryLargerThanTheWholeCapIsServedButNeverCached() throws Exception {
        SegmentBundle bundle = writeSampleBundle(); // "hello" == 5 bytes
        BundleFileEntry entry = bundle.entries().get("a.bin");
        CountingBundleFileReader counting = new CountingBundleFileReader(inMemoryReader(bundle));
        InMemoryPlaintextBundleCache cache = new InMemoryPlaintextBundleCache(2);

        byte[] first = cache.readFile("bundle-1", entry, counting);
        byte[] second = cache.readFile("bundle-1", entry, counting);

        assertArrayEquals(first, second);
        assertEquals("an entry over the cap must always be a miss", 2, counting.callCount.get());
        assertEquals(0, cache.currentTotalBytes());
    }

    public void testZeroCapNeverCachesAnything() throws Exception {
        SegmentBundle bundle = writeSampleBundle();
        BundleFileEntry entry = bundle.entries().get("a.bin");
        CountingBundleFileReader counting = new CountingBundleFileReader(inMemoryReader(bundle));
        InMemoryPlaintextBundleCache cache = new InMemoryPlaintextBundleCache(0);

        cache.readFile("bundle-1", entry, counting);
        cache.readFile("bundle-1", entry, counting);

        assertEquals(2, counting.callCount.get());
    }

    public void testDifferentEntriesAreCachedIndependently() throws Exception {
        SegmentBundle bundle = BundleWriter.write(
            List.of(
                new BundleFileContent("a.bin", "aaa".getBytes(StandardCharsets.UTF_8)),
                new BundleFileContent("b.bin", "bbbbb".getBytes(StandardCharsets.UTF_8))
            )
        );
        CountingBundleFileReader counting = new CountingBundleFileReader(inMemoryReader(bundle));
        InMemoryPlaintextBundleCache cache = new InMemoryPlaintextBundleCache(1024);

        byte[] a = cache.readFile("bundle-1", bundle.entries().get("a.bin"), counting);
        byte[] b = cache.readFile("bundle-1", bundle.entries().get("b.bin"), counting);

        assertEquals("aaa", new String(a, StandardCharsets.UTF_8));
        assertEquals("bbbbb", new String(b, StandardCharsets.UTF_8));
        assertEquals(2, counting.callCount.get());
    }

    public void testDifferentShardsSharingOneCacheDoNotCollideEvenWithIdenticalBundleFileEntries() throws Exception {
        // Bundle names already embed index/shard (ObjectStoreCommitPublisher), but this proves the
        // cache key genuinely depends on bundleName, not just the entry -- two shards' identically
        // shaped entries for a differently-named bundle must be tracked independently.
        SegmentBundle bundleA = writeSampleBundle();
        SegmentBundle bundleB = writeSampleBundle();
        BundleFileEntry entryA = bundleA.entries().get("a.bin");
        BundleFileEntry entryB = bundleB.entries().get("a.bin");
        CountingBundleFileReader countingA = new CountingBundleFileReader(inMemoryReader(bundleA));
        CountingBundleFileReader countingB = new CountingBundleFileReader(inMemoryReader(bundleB));
        InMemoryPlaintextBundleCache cache = new InMemoryPlaintextBundleCache(1024);

        cache.readFile("bundle-shard-A", entryA, countingA);
        cache.readFile("bundle-shard-B", entryB, countingB);
        cache.readFile("bundle-shard-A", entryA, countingA);
        cache.readFile("bundle-shard-B", entryB, countingB);

        assertEquals("shard A's entry must be a genuine hit on the second read", 1, countingA.callCount.get());
        assertEquals("shard B's entry must be a genuine hit on the second read", 1, countingB.callCount.get());
    }
}
