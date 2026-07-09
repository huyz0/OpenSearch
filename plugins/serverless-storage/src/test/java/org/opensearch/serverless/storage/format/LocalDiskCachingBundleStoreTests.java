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
import java.util.List;
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

    public void testUnencryptedCacheStillWorksWithNoKeyProvider() throws Exception {
        // The 2-arg constructor must remain equivalent to passing a null key provider.
        SegmentBundle bundle = writeSampleBundle();
        BundleFileEntry entry = bundle.entries().get("a.bin");
        LocalDiskCachingBundleStore cache = new LocalDiskCachingBundleStore(inMemoryReader(bundle), createTempDir(), null);

        byte[] result = cache.readFile("bundle-1", entry);
        assertEquals("hello", new String(result, java.nio.charset.StandardCharsets.UTF_8));
    }
}
