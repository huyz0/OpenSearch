/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import org.opensearch.serverless.storage.security.AesGcmCipher;
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

    // Regression test for the byte counter this fix's own cheap eviction bail-out depends on
    // (currentTotalBytes): a write that REPLACES an existing, differently-sized on-disk entry
    // (rather than creating a brand-new one) must net out the old size against the new one, not add
    // the new size on top of a counter that still includes the old. Realistic trigger: a cache
    // directory populated without encryption, then reopened by a new instance with encryption now
    // enabled (a legitimate config change across a restart) -- the old plaintext file can't decrypt,
    // so it's replaced by a longer ciphertext file at the very same path.
    //
    // Not a correctness bug if missed: evictIfOverBudget always re-verifies against a real, fresh
    // directory listing before deleting anything, so a wrongly-inflated counter can never cause an
    // INCORRECT eviction -- only a wasted one that finds nothing to actually reclaim. What it DOES
    // break, permanently, for the rest of this instance's lifetime, is the whole point of this
    // session's OWN earlier hit-path eviction fix: with no write ever correcting a wrongly-inflated
    // counter back down, every single future hit would forever re-trigger a full sweep it doesn't
    // need. sweepCountForTesting() (distinct from evictedCount(), which only counts actual deletes)
    // is what makes that permanent-drift regression directly observable rather than merely inferred.
    public void testReplacingACorruptedEntryDoesNotDoubleCountItsOldSizeInTheEvictionBudget() throws Exception {
        SegmentBundle bundle = BundleWriter.write(
            List.of(
                new BundleFileContent("a.bin", "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new BundleFileContent("b.bin", "world".getBytes(java.nio.charset.StandardCharsets.UTF_8))
            )
        );
        BundleFileEntry entryA = bundle.entries().get("a.bin");
        BundleFileEntry entryB = bundle.entries().get("b.bin");
        Path cacheDir = createTempDir();

        // First instance: no encryption -- writes only A, as 5 plaintext bytes. B is deliberately
        // left uncached here (see below for why).
        LocalDiskCachingBundleStore plainWriter = new LocalDiskCachingBundleStore(inMemoryReader(bundle), cacheDir);
        plainWriter.readFile("bundle-1", entryA);

        SecretKey key = newAesKey();
        long ciphertextLength = AesGcmCipher.encrypt("hello".getBytes(java.nio.charset.StandardCharsets.UTF_8), key).length;

        // Second instance: encryption now enabled. Budget has headroom over A's ciphertext + B's
        // (about-to-be-written) ciphertext -- both 33 bytes here, "hello"/"world" being the same
        // length -- but stays comfortably under what double-counting A's replaced plaintext would
        // add on top, so the two cases stay clearly distinguishable.
        LocalDiskCachingBundleStore secondInstance = new LocalDiskCachingBundleStore(
            inMemoryReader(bundle),
            cacheDir,
            new StaticEncryptionKeyProvider(key),
            2 * ciphertextLength + 2
        );

        secondInstance.readFile("bundle-1", entryA); // undecryptable plaintext -- falls through, re-fetches, re-writes as ciphertext
        // B was never cached under ANY key before this -- a genuine first-ever miss, not a replace,
        // so it can never exercise the double-counting this test targets. Writing it now (rather
        // than via plainWriter above) is what keeps every later read of B a plain, uncomplicated
        // hit -- if B had also started out as plaintext, its OWN first read here would need the same
        // corrupted-entry replace A's did, compounding the arithmetic this test is trying to isolate
        // to A alone.
        secondInstance.readFile("bundle-1", entryB);
        long sweepsAfterBothWrites = secondInstance.sweepCountForTesting();

        // Many subsequent hits against an unchanged, correctly-within-budget directory -- with the
        // counter accurate, none of these should need to re-verify anything.
        for (int i = 0; i < 50; i++) {
            secondInstance.readFile("bundle-1", entryA);
            secondInstance.readFile("bundle-1", entryB);
        }

        assertEquals(
            "a correctly-accounted-for replacement must not leave the counter permanently over budget, "
                + "which would otherwise re-trigger a full sweep on every single future hit forever",
            sweepsAfterBothWrites,
            secondInstance.sweepCountForTesting()
        );
        assertEquals("no entry should ever have actually needed eviction in this scenario", 0, secondInstance.evictedCount());
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
    // call. The realistic way that happens: a node restart or shard relocation constructs a NEW
    // instance over a directory a PRIOR instance already populated under a larger (or unbounded)
    // budget, and the new instance's own budget is smaller -- the new instance's constructor
    // correctly measures the real, already-over-budget total, but (before this fix) nothing but a
    // fresh write would ever act on that. Proven here with two separate instances over the same
    // directory, and a plain HIT (no write) on the second one triggering the sweep.
    public void testAHitAloneCanTriggerEvictionWhenTheDirectoryIsAlreadyOverBudgetWithNoNewWrite() throws Exception {
        SegmentBundle bundle = BundleWriter.write(
            List.of(
                new BundleFileContent("a.bin", "aaaaaaaaaa".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new BundleFileContent("b.bin", "bbbbbbbbbb".getBytes(java.nio.charset.StandardCharsets.UTF_8))
            )
        );
        BundleFileEntry entryA = bundle.entries().get("a.bin");
        BundleFileEntry entryB = bundle.entries().get("b.bin");
        Path cacheDir = createTempDir();

        // First instance: unbounded, so both 10-byte entries land with no eviction at all.
        LocalDiskCachingBundleStore firstInstance = new LocalDiskCachingBundleStore(inMemoryReader(bundle), cacheDir);
        Path fileA = fileWrittenBy(cacheDir, () -> firstInstance.readFile("bundle-1", entryA));
        Files.setLastModifiedTime(fileA, FileTime.from(Instant.now().minusSeconds(60)));
        Path fileB = fileWrittenBy(cacheDir, () -> firstInstance.readFile("bundle-1", entryB));
        assertEquals(20, Files.size(fileA) + Files.size(fileB));

        // Second instance: simulates a restart/relocation reopening the same directory under a
        // smaller, newly configured 15-byte budget -- its own constructor must measure the real
        // 20-byte total already on disk, not start from a blank slate.
        CountingBundleFileReader counting = new CountingBundleFileReader(inMemoryReader(bundle));
        LocalDiskCachingBundleStore secondInstance = new LocalDiskCachingBundleStore(counting, cacheDir, null, 15L);
        assertEquals("no eviction must have run yet -- construction only measures, it doesn't sweep", 0, secondInstance.evictedCount());

        secondInstance.readFile("bundle-1", entryB); // a plain hit -- B is already on disk and valid

        assertTrue("a hit against an already over-budget directory must still trigger a sweep", secondInstance.evictedCount() >= 1);
        assertFalse("the older, unrelated entry must be the one reclaimed", Files.exists(fileA));
        assertEquals("the hit must still have been served from disk, not the delegate", 0, counting.callCount.get());
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
