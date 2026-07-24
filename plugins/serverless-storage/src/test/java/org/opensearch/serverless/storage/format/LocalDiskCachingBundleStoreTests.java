/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import org.opensearch.serverless.storage.security.StaticEncryptionKeyProvider;
import org.opensearch.test.OpenSearchTestCase;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalDiskCachingBundleStoreTests extends OpenSearchTestCase {

    /** Counts every delegate call, so tests can assert exactly how many real fetches happened. */
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
        return BundleWriter.write(List.of(new BundleFileContent("a.bin", "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8))));
    }

    private static BundleFileReader inMemoryReader(SegmentBundle bundle) {
        return (bundleName, entry) -> BundleReader.extractFile(bundle.bytes(), entry);
    }

    public void testSecondReadIsAHitAndDoesNotCallTheDelegateAgain() throws Exception {
        SegmentBundle bundle = writeSampleBundle();
        BundleFileEntry entry = bundle.entries().get("a.bin");
        CountingBundleFileReader counting = new CountingBundleFileReader(inMemoryReader(bundle));
        LocalDiskCachingBundleStore cache = new LocalDiskCachingBundleStore(counting, createTempDir());

        byte[] first = cache.readFile("bundle-1", entry);
        byte[] second = cache.readFile("bundle-1", entry);

        assertArrayEquals(first, second);
        assertEquals(1, counting.callCount.get());
        assertEquals(1, cache.missCount());
        assertEquals(1, cache.hitCount());
    }

    public void testCachedBytesSurviveAFreshCacheInstanceOverTheSameDirectory() throws Exception {
        SegmentBundle bundle = writeSampleBundle();
        BundleFileEntry entry = bundle.entries().get("a.bin");
        java.nio.file.Path cacheDir = createTempDir();
        CountingBundleFileReader counting = new CountingBundleFileReader(inMemoryReader(bundle));

        LocalDiskCachingBundleStore firstInstance = new LocalDiskCachingBundleStore(counting, cacheDir);
        firstInstance.readFile("bundle-1", entry);
        assertEquals(1, counting.callCount.get());

        // A brand new cache instance over the same directory (e.g. after a process restart) must
        // still see the file on disk and not re-fetch it.
        LocalDiskCachingBundleStore secondInstance = new LocalDiskCachingBundleStore(counting, cacheDir);
        byte[] fromSecondInstance = secondInstance.readFile("bundle-1", entry);

        assertEquals("hello".length(), fromSecondInstance.length);
        assertEquals("still just the one real fetch, from the first instance", 1, counting.callCount.get());
    }

    public void testDifferentEntriesAreCachedIndependently() throws Exception {
        SegmentBundle bundle = BundleWriter.write(
            List.of(
                new BundleFileContent("a.bin", "aaa".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new BundleFileContent("b.bin", "bbbbb".getBytes(java.nio.charset.StandardCharsets.UTF_8))
            )
        );
        CountingBundleFileReader counting = new CountingBundleFileReader(inMemoryReader(bundle));
        LocalDiskCachingBundleStore cache = new LocalDiskCachingBundleStore(counting, createTempDir());

        byte[] a = cache.readFile("bundle-1", bundle.entries().get("a.bin"));
        byte[] b = cache.readFile("bundle-1", bundle.entries().get("b.bin"));

        assertEquals("aaa", new String(a, java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("bbbbb", new String(b, java.nio.charset.StandardCharsets.UTF_8));
        assertEquals(2, counting.callCount.get());
    }

    public void testConcurrentReadersOfTheSameEntryOnlyFetchOnce() throws Exception {
        SegmentBundle bundle = writeSampleBundle();
        BundleFileEntry entry = bundle.entries().get("a.bin");
        CountingBundleFileReader counting = new CountingBundleFileReader(inMemoryReader(bundle));
        LocalDiskCachingBundleStore cache = new LocalDiskCachingBundleStore(counting, createTempDir());

        int readerCount = 16;
        ExecutorService executor = Executors.newFixedThreadPool(readerCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        try {
            List<Future<byte[]>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < readerCount; i++) {
                futures.add(executor.submit(() -> {
                    startLatch.await();
                    return cache.readFile("bundle-1", entry);
                }));
            }
            startLatch.countDown();
            for (Future<byte[]> future : futures) {
                assertArrayEquals("hello".getBytes(java.nio.charset.StandardCharsets.UTF_8), future.get(30, TimeUnit.SECONDS));
            }
        } finally {
            executor.shutdown();
        }

        assertEquals("all concurrent readers of the same entry must share a single real fetch", 1, counting.callCount.get());
    }

    private static SecretKey newAesKey() throws Exception {
        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
        keyGenerator.init(256);
        return keyGenerator.generateKey();
    }

    public void testWithAnEncryptionKeyTheDiskFileDoesNotContainThePlaintext() throws Exception {
        SegmentBundle bundle = writeSampleBundle();
        BundleFileEntry entry = bundle.entries().get("a.bin");
        Path cacheDir = createTempDir();
        LocalDiskCachingBundleStore cache = new LocalDiskCachingBundleStore(
            inMemoryReader(bundle),
            cacheDir,
            new StaticEncryptionKeyProvider(newAesKey())
        );

        byte[] result = cache.readFile("bundle-1", entry);
        assertEquals("hello", new String(result, java.nio.charset.StandardCharsets.UTF_8));

        try (var files = Files.list(cacheDir)) {
            // Not endsWith(".tmp"): LocalDiskCachingBundleStore's actual temp-file naming is
            // "<key>.tmp-<threadId>" (see #writeAtomically), which never ends in bare ".tmp" -- that
            // check never excluded anything. Filter on isRegularFile too: Lucene's randomized
            // createTempDir() can inject extra junk subdirectories into the same temp dir, and
            // Files.readAllBytes on a directory throws "Is a directory" instead of a clear failure.
            Path cachedFile = files.filter(Files::isRegularFile)
                .filter(p -> p.getFileName().toString().contains(".tmp") == false)
                .findFirst()
                .orElseThrow();
            byte[] onDisk = Files.readAllBytes(cachedFile);
            assertFalse(
                "the on-disk cache file must not contain the plaintext when encryption is enabled",
                new String(onDisk, java.nio.charset.StandardCharsets.UTF_8).contains("hello")
            );
        }
    }

    public void testWithAnEncryptionKeyASecondReadDecryptsBackToTheOriginalBytes() throws Exception {
        SegmentBundle bundle = writeSampleBundle();
        BundleFileEntry entry = bundle.entries().get("a.bin");
        CountingBundleFileReader counting = new CountingBundleFileReader(inMemoryReader(bundle));
        LocalDiskCachingBundleStore cache = new LocalDiskCachingBundleStore(
            counting,
            createTempDir(),
            new StaticEncryptionKeyProvider(newAesKey())
        );

        byte[] first = cache.readFile("bundle-1", entry);
        byte[] second = cache.readFile("bundle-1", entry);

        assertArrayEquals(first, second);
        assertEquals("hello", new String(second, java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("second read must be a disk-cache hit, not a re-fetch", 1, counting.callCount.get());
        assertEquals(1, cache.hitCount());
    }

    public void testACacheDirectoryEncryptedWithOneKeyIsNotReadableWithAnother() throws Exception {
        SegmentBundle bundle = writeSampleBundle();
        BundleFileEntry entry = bundle.entries().get("a.bin");
        Path cacheDir = createTempDir();
        CountingBundleFileReader counting = new CountingBundleFileReader(inMemoryReader(bundle));

        LocalDiskCachingBundleStore writer = new LocalDiskCachingBundleStore(
            counting,
            cacheDir,
            new StaticEncryptionKeyProvider(newAesKey())
        );
        writer.readFile("bundle-1", entry);
        assertEquals(1, counting.callCount.get());

        // A fresh instance with a different key can't decrypt what's on disk -- it must fall back
        // to a real re-fetch rather than surface a decryption error or return garbage.
        LocalDiskCachingBundleStore readerWithDifferentKey = new LocalDiskCachingBundleStore(
            counting,
            cacheDir,
            new StaticEncryptionKeyProvider(newAesKey())
        );
        byte[] result = readerWithDifferentKey.readFile("bundle-1", entry);

        assertEquals("hello", new String(result, java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("undecryptable cache entry must fall back to a real fetch", 2, counting.callCount.get());
    }

    private interface ThrowingRunnable {
        void run() throws IOException;
    }

    private static Set<Path> listCacheFiles(Path cacheDir) throws IOException {
        try (var stream = Files.list(cacheDir)) {
            return stream.filter(Files::isRegularFile).collect(java.util.stream.Collectors.toSet());
        }
    }

    /** Runs {@code write}, returning whichever single cache file appeared in {@code cacheDir} as a result. */
    private static Path fileWrittenBy(Path cacheDir, ThrowingRunnable write) throws IOException {
        Set<Path> before = listCacheFiles(cacheDir);
        write.run();
        Set<Path> after = new HashSet<>(listCacheFiles(cacheDir));
        after.removeAll(before);
        assertEquals("expected exactly one new cache file", 1, after.size());
        return after.iterator().next();
    }

    public void testEvictsTheLeastRecentlyTouchedEntryOnceOverBudget() throws Exception {
        SegmentBundle bundle = BundleWriter.write(
            List.of(
                new BundleFileContent("a.bin", "aaaaaaaaaa".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new BundleFileContent("b.bin", "bbbbbbbbbb".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new BundleFileContent("c.bin", "cccccccccc".getBytes(java.nio.charset.StandardCharsets.UTF_8))
            )
        );
        CountingBundleFileReader counting = new CountingBundleFileReader(inMemoryReader(bundle));
        Path cacheDir = createTempDir();
        // Each entry caches to 10 bytes; a 25-byte budget fits two but not all three.
        LocalDiskCachingBundleStore cache = new LocalDiskCachingBundleStore(counting, cacheDir, null, 25L);

        Path fileA = fileWrittenBy(cacheDir, () -> cache.readFile("bundle-1", bundle.entries().get("a.bin")));
        Files.setLastModifiedTime(fileA, FileTime.from(Instant.now().minusSeconds(30)));
        Path fileB = fileWrittenBy(cacheDir, () -> cache.readFile("bundle-1", bundle.entries().get("b.bin")));
        Files.setLastModifiedTime(fileB, FileTime.from(Instant.now().minusSeconds(20)));
        // Writing C pushes total bytes to 30 > 25, triggering a sweep down to floor(25*0.9)=22:
        // A (oldest) alone is evicted (30 -> 20), B and C both survive.
        cache.readFile("bundle-1", bundle.entries().get("c.bin"));

        assertEquals(1, cache.evictedCount());
        assertFalse("the least-recently-touched entry must actually be gone from disk", Files.exists(fileA));
        assertTrue(Files.exists(fileB));

        // Check the surviving entry first, while it's still on disk -- re-fetching the evicted
        // entry below writes it back, which (at this same 2-of-3 budget) would otherwise trigger a
        // second sweep and evict B in turn, confusing what this assertion is meant to prove.
        int callsBeforeReread = counting.callCount.get();
        cache.readFile("bundle-1", bundle.entries().get("b.bin"));
        assertEquals("a surviving entry must still be a disk-cache hit", callsBeforeReread, counting.callCount.get());
        cache.readFile("bundle-1", bundle.entries().get("a.bin"));
        assertEquals(
            "the evicted entry must re-fetch from the delegate, not silently return nothing",
            callsBeforeReread + 1,
            counting.callCount.get()
        );
    }

    public void testATouchedHitOutlivesAnUntouchedEntryWrittenLater() throws Exception {
        SegmentBundle bundle = BundleWriter.write(
            List.of(
                new BundleFileContent("a.bin", "aaaaaaaaaa".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new BundleFileContent("b.bin", "bbbbbbbbbb".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new BundleFileContent("c.bin", "cccccccccc".getBytes(java.nio.charset.StandardCharsets.UTF_8))
            )
        );
        CountingBundleFileReader counting = new CountingBundleFileReader(inMemoryReader(bundle));
        Path cacheDir = createTempDir();
        LocalDiskCachingBundleStore cache = new LocalDiskCachingBundleStore(counting, cacheDir, null, 25L);

        // A is written first (would be the oldest by write order alone) but explicitly backdated,
        // then re-read (a hit) so its recency becomes "now" -- refreshed strictly newer than B.
        Path fileA = fileWrittenBy(cacheDir, () -> cache.readFile("bundle-1", bundle.entries().get("a.bin")));
        Files.setLastModifiedTime(fileA, FileTime.from(Instant.now().minusSeconds(30)));
        cache.readFile("bundle-1", bundle.entries().get("a.bin"));

        // B is written after A's original write, never touched again, and backdated older than
        // both A's refreshed recency and C's imminent creation time.
        Path fileB = fileWrittenBy(cacheDir, () -> cache.readFile("bundle-1", bundle.entries().get("b.bin")));
        Files.setLastModifiedTime(fileB, FileTime.from(Instant.now().minusSeconds(20)));

        // Writing C triggers the sweep: B, not A, must be the one evicted, proving the touch above
        // actually changed eviction order rather than raw write order deciding it.
        cache.readFile("bundle-1", bundle.entries().get("c.bin"));

        assertEquals(1, cache.evictedCount());
        assertTrue("the touched entry must survive over the untouched-but-more-recently-written one", Files.exists(fileA));
        assertFalse(Files.exists(fileB));

        int callsBeforeReread = counting.callCount.get();
        cache.readFile("bundle-1", bundle.entries().get("a.bin"));
        assertEquals("the touched, surviving entry must still be a disk-cache hit", callsBeforeReread, counting.callCount.get());
    }

    // Regression test for a real bug: eviction was only ever triggered from the miss path (right
    // after writeAtomically), never from a hit. A shard whose working set is already fully warm --
    // every subsequent read a hit, no new file ever written -- would never get another chance to
    // evict if its directory ended up over budget for a reason that didn't originate from that same
    // call (e.g. content that predates the cache, or a directory that grew over budget purely from
    // touches without any fresh write crossing the line). Simulated here by dropping an oversized
    // file into the cache directory directly (bypassing the cache's own write path entirely, so
    // maybeEvict() is never triggered by it), then proving a plain HIT on an unrelated, already-
    // cached entry is what finally triggers the sweep that reclaims it.
    public void testAHitAloneCanTriggerEvictionWhenTheDirectoryIsAlreadyOverBudgetWithNoNewWrite() throws Exception {
        SegmentBundle bundle = writeSampleBundle(); // "hello" == 5 bytes
        BundleFileEntry entry = bundle.entries().get("a.bin");
        CountingBundleFileReader counting = new CountingBundleFileReader(inMemoryReader(bundle));
        Path cacheDir = createTempDir();
        LocalDiskCachingBundleStore cache = new LocalDiskCachingBundleStore(counting, cacheDir, null, 25L);

        Path cachedEntryPath = fileWrittenBy(cacheDir, () -> cache.readFile("bundle-1", entry));
        Files.setLastModifiedTime(cachedEntryPath, FileTime.from(Instant.now()));

        // Dropped in directly, not through the cache's own write path -- represents content that
        // predates this instance (or a budget lowered after the fact), backdated older than the
        // real entry above so it's the sweep's obvious eviction candidate.
        Path oversizedLeftover = cacheDir.resolve("leftover-from-a-previous-session");
        Files.write(oversizedLeftover, new byte[30]); // alone already exceeds the 25-byte budget
        Files.setLastModifiedTime(oversizedLeftover, FileTime.from(Instant.now().minusSeconds(60)));

        assertEquals("no eviction must have run yet -- the oversized file bypassed the cache's own write path", 0, cache.evictedCount());

        cache.readFile("bundle-1", entry); // a plain hit -- the real entry is already on disk and valid

        assertTrue(
            "a hit against an already over-budget directory must still trigger a sweep",
            cache.evictedCount() >= 1
        );
        assertFalse("the oldest, oversized entry must be the one reclaimed", Files.exists(oversizedLeftover));
        assertTrue("the entry the hit itself just served must survive its own triggering read", Files.exists(cachedEntryPath));
        assertEquals("the hit must still have been served from disk, not the delegate", 1, counting.callCount.get());
    }

    public void testUnboundedByDefaultNeverEvictsRegardlessOfSize() throws Exception {
        SegmentBundle bundle = BundleWriter.write(
            List.of(
                new BundleFileContent("a.bin", "aaaaaaaaaa".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new BundleFileContent("b.bin", "bbbbbbbbbb".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new BundleFileContent("c.bin", "cccccccccc".getBytes(java.nio.charset.StandardCharsets.UTF_8))
            )
        );
        CountingBundleFileReader counting = new CountingBundleFileReader(inMemoryReader(bundle));
        // The 3-arg constructor (no maxBytesOnDisk) must remain exactly as unbounded as before.
        LocalDiskCachingBundleStore cache = new LocalDiskCachingBundleStore(counting, createTempDir(), null);

        cache.readFile("bundle-1", bundle.entries().get("a.bin"));
        cache.readFile("bundle-1", bundle.entries().get("b.bin"));
        cache.readFile("bundle-1", bundle.entries().get("c.bin"));

        assertEquals(0, cache.evictedCount());
    }

    public void testAverageColdReadLatencyIsZeroWithNoMisses() throws Exception {
        LocalDiskCachingBundleStore cache = new LocalDiskCachingBundleStore(inMemoryReader(writeSampleBundle()), createTempDir());
        assertEquals("no read has happened at all yet", 0L, cache.averageColdReadLatencyMillis());
    }

    public void testAverageColdReadLatencyReflectsRealTimeSpentInTheDelegate() throws Exception {
        SegmentBundle bundle = writeSampleBundle();
        BundleFileEntry entry = bundle.entries().get("a.bin");
        // A delegate that deliberately sleeps on every call, so a real elapsed-time measurement
        // must reflect (at least) that sleep -- distinguishing a genuine timer from one that always
        // reports 0 or a constant regardless of how long the delegate actually took.
        BundleFileReader slowDelegate = (bundleName, e) -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
            return inMemoryReader(bundle).readFile(bundleName, e);
        };
        LocalDiskCachingBundleStore cache = new LocalDiskCachingBundleStore(slowDelegate, createTempDir());

        cache.readFile("bundle-1", entry);
        // A second read is a disk-cache hit, not a further delegate call -- must not dilute the
        // average toward a fast/zero value.
        cache.readFile("bundle-1", entry);

        assertEquals(1, cache.missCount());
        assertTrue(
            "average cold-read latency must reflect the delegate's real ~50ms sleep, not read as 0",
            cache.averageColdReadLatencyMillis() >= 40L
        );
    }

    public void testUnencryptedCacheStillWorksWithNoKeyProvider() throws Exception {
        // The 2-arg constructor must remain equivalent to passing a null key provider.
        SegmentBundle bundle = writeSampleBundle();
        BundleFileEntry entry = bundle.entries().get("a.bin");
        LocalDiskCachingBundleStore cache = new LocalDiskCachingBundleStore(inMemoryReader(bundle), createTempDir(), null);

        byte[] result = cache.readFile("bundle-1", entry);
        assertEquals("hello", new String(result, java.nio.charset.StandardCharsets.UTF_8));
    }
}
