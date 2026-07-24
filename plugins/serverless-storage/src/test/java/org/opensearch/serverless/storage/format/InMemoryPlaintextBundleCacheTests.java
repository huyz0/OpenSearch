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
import java.util.ArrayList;
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

    // Regression test for a real bug: readFile releases its lock between the initial get() check
    // and the later put(), so two concurrent misses for the SAME key both call onMiss independently
    // and both reach put(). put() on an already-present key silently replaces the old value, but
    // the old code unconditionally added the new value's length without subtracting the length of
    // whatever it just overwrote -- currentTotalBytes drifted upward by one entry's worth on every
    // such race, permanently, even though only one value ever actually survives in the map.
    public void testConcurrentMissesForTheSameKeyDoNotDoubleCountBytesOnOverwrite() throws Exception {
        SegmentBundle bundle = writeSampleBundle();
        BundleFileEntry entry = bundle.entries().get("a.bin");
        InMemoryPlaintextBundleCache cache = new InMemoryPlaintextBundleCache(1024);

        java.util.concurrent.CountDownLatch bothEntered = new java.util.concurrent.CountDownLatch(2);
        java.util.concurrent.CountDownLatch proceed = new java.util.concurrent.CountDownLatch(1);
        BundleFileReader blockingReader = (bundleName, e) -> {
            bothEntered.countDown();
            try {
                proceed.await();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new java.io.IOException(ex);
            }
            return inMemoryReader(bundle).readFile(bundleName, e);
        };

        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            java.util.List<java.util.concurrent.Future<byte[]>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < 2; i++) {
                futures.add(executor.submit(() -> cache.readFile("bundle-1", entry, blockingReader)));
            }
            // Both threads must have missed and entered onMiss (proving the race is real -- neither
            // saw the other's result cached yet) before either is allowed to proceed to put().
            assertTrue(bothEntered.await(10, java.util.concurrent.TimeUnit.SECONDS));
            proceed.countDown();
            for (java.util.concurrent.Future<byte[]> future : futures) {
                future.get(10, java.util.concurrent.TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
        }

        assertEquals(
            "currentTotalBytes must reflect only the one surviving entry, not both racing writers' lengths",
            entry.length(),
            cache.currentTotalBytes()
        );
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

    public void testSmallBudgetsCollapseToOneStripe() {
        // Every test above relies on this: a budget below MIN_BYTES_PER_STRIPE (1024) must behave
        // exactly like the pre-striping single map, which is what makes the exact-LRU and exact-
        // byte-count assertions above valid regardless of the striped implementation underneath.
        assertEquals(1, new InMemoryPlaintextBundleCache(0).stripeCountForTesting());
        assertEquals(1, new InMemoryPlaintextBundleCache(2).stripeCountForTesting());
        assertEquals(1, new InMemoryPlaintextBundleCache(1024).stripeCountForTesting());
    }

    public void testLargeBudgetsSpreadAcrossMultipleStripes() {
        InMemoryPlaintextBundleCache cache = new InMemoryPlaintextBundleCache(64 * 1024);
        assertTrue("a 64KiB budget should be worth striping", cache.stripeCountForTesting() > 1);
    }

    public void testEachStripeEvictsIndependentlyByItsOwnByteBudget() throws Exception {
        // A budget large enough to guarantee multiple stripes (see MIN_BYTES_PER_STRIPE/MAX_STRIPES).
        InMemoryPlaintextBundleCache cache = new InMemoryPlaintextBundleCache(64 * 1024);
        SegmentBundle bundle = BundleWriter.write(
            List.of(new BundleFileContent("f.bin", "x".repeat(200).getBytes(StandardCharsets.UTF_8)))
        );
        BundleFileEntry entry = bundle.entries().get("f.bin"); // 200 bytes, reused under many different bundle names
        BundleFileReader reader = inMemoryReader(bundle);

        String anchorBundle = "anchor-bundle";
        cache.readFile(anchorBundle, entry, reader);
        int anchorStripe = cache.stripeIndexFor(anchorBundle, entry);

        // Find enough distinct bundle names landing in one OTHER stripe to overflow that stripe's
        // own share of the budget (perStripeBudget = 64KiB / stripeCount, each entry is 200 bytes).
        List<String> floodBundles = new ArrayList<>();
        int floodStripe = -1;
        for (int i = 0; floodBundles.size() < 40; i++) {
            String candidate = "flood-bundle-" + i;
            int stripe = cache.stripeIndexFor(candidate, entry);
            if (stripe == anchorStripe) {
                continue;
            }
            if (floodStripe == -1) {
                floodStripe = stripe;
            }
            if (stripe == floodStripe) {
                floodBundles.add(candidate);
            }
        }

        for (String name : floodBundles) {
            cache.readFile(name, entry, reader);
        }

        CountingBundleFileReader countingAnchor = new CountingBundleFileReader(reader);
        cache.readFile(anchorBundle, entry, countingAnchor);
        assertEquals(
            "an entry in an untouched stripe must survive eviction pressure in a different stripe",
            0,
            countingAnchor.callCount.get()
        );

        CountingBundleFileReader countingFirstFlooded = new CountingBundleFileReader(reader);
        cache.readFile(floodBundles.get(0), entry, countingFirstFlooded);
        assertEquals(
            "the earliest entry in the flooded stripe should have been evicted by its own stripe's budget",
            1,
            countingFirstFlooded.callCount.get()
        );
    }
}
