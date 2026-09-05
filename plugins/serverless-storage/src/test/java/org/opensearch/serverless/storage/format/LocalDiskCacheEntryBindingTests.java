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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An encrypted cache entry used to be <em>substitutable</em>: its AES-GCM tag proved the bytes came
 * from this node's key and were unaltered, but said nothing about which file they were, so one
 * cache file moved over another's name was accepted end to end -- the CRC32C the reader compares
 * against comes out of the very bytes that were substituted. Binding the file's identity in as
 * associated data is what makes the decryption itself fail on that.
 */
public class LocalDiskCacheEntryBindingTests extends OpenSearchTestCase {

    private static SecretKey newAesKey() throws Exception {
        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
        keyGenerator.init(256);
        return keyGenerator.generateKey();
    }

    /** Counts real delegate fetches, so a "was it re-fetched?" assertion is possible. */
    private static final class CountingReader implements BundleFileReader {
        private final SegmentBundle bundle;
        private final AtomicInteger calls = new AtomicInteger();

        CountingReader(SegmentBundle bundle) {
            this.bundle = bundle;
        }

        @Override
        public byte[] readFile(String bundleName, BundleFileEntry entry) throws IOException {
            calls.incrementAndGet();
            return BundleReader.extractFile(bundle.bytes(), bundle.entries().get(entry.name()));
        }
    }

    private static SegmentBundle twoFileBundle() {
        return BundleWriter.write(
            List.of(
                new BundleFileContent("alpha.bin", "AAAAAAAA".getBytes(StandardCharsets.UTF_8)),
                new BundleFileContent("beta.bin", "BBBBBBBB".getBytes(StandardCharsets.UTF_8))
            )
        );
    }

    private static Path cacheFileMatching(Path cacheDirectory, long offset) throws IOException {
        try (var files = Files.list(cacheDirectory)) {
            return files.filter(p -> p.getFileName().toString().contains("-" + offset + "-")).findFirst().orElseThrow();
        }
    }

    /**
     * The substitution itself. Two same-length files are cached, then one entry's ciphertext is
     * moved over the other's path. Before the binding, the moved bytes decrypted cleanly and their
     * self-consistent CRC32C matched, so the wrong file's contents were returned as a cache hit.
     * With the binding, the decrypt fails on the associated data and the store falls through to a
     * correct re-fetch.
     */
    public void testACacheFileCannotBeServedUnderAnotherFilesKey() throws Exception {
        SegmentBundle bundle = twoFileBundle();
        BundleFileEntry alpha = bundle.entries().get("alpha.bin");
        BundleFileEntry beta = bundle.entries().get("beta.bin");
        assertEquals("test setup: the two entries must be the same length", alpha.length(), beta.length());

        Path cacheDirectory = createTempDir();
        CountingReader counting = new CountingReader(bundle);
        LocalDiskCachingBundleStore cache = new LocalDiskCachingBundleStore(
            counting,
            cacheDirectory,
            new StaticEncryptionKeyProvider(newAesKey())
        );

        assertEquals("AAAAAAAA", new String(cache.readFile("bundle-1", alpha), StandardCharsets.UTF_8));
        assertEquals("BBBBBBBB", new String(cache.readFile("bundle-1", beta), StandardCharsets.UTF_8));
        assertEquals(2, counting.calls.get());

        // Move beta's ciphertext over alpha's cache path: a rename inside a directory this cache
        // already assumes local access to.
        Path alphaPath = cacheFileMatching(cacheDirectory, alpha.offset());
        Path betaPath = cacheFileMatching(cacheDirectory, beta.offset());
        Files.move(betaPath, alphaPath, StandardCopyOption.REPLACE_EXISTING);

        assertEquals(
            "reading alpha must still yield alpha, never the substituted file's contents",
            "AAAAAAAA",
            new String(cache.readFile("bundle-1", alpha), StandardCharsets.UTF_8)
        );
        assertEquals("the substituted entry must have been rejected and re-fetched", 3, counting.calls.get());
    }

    /** An honest hit must still be a hit -- the binding must not turn every read into a miss. */
    public void testABoundEntryIsStillServedFromCache() throws Exception {
        SegmentBundle bundle = twoFileBundle();
        BundleFileEntry alpha = bundle.entries().get("alpha.bin");
        CountingReader counting = new CountingReader(bundle);
        LocalDiskCachingBundleStore cache = new LocalDiskCachingBundleStore(
            counting,
            createTempDir(),
            new StaticEncryptionKeyProvider(newAesKey())
        );

        assertEquals("AAAAAAAA", new String(cache.readFile("bundle-1", alpha), StandardCharsets.UTF_8));
        assertEquals("AAAAAAAA", new String(cache.readFile("bundle-1", alpha), StandardCharsets.UTF_8));
        assertEquals("the second read must be a cache hit", 1, counting.calls.get());
        assertEquals(1, cache.hitCount());
    }

    /**
     * A cache directory written by a build from before the binding existed must stay usable across
     * an upgrade rather than forcing a node to re-fetch its whole working set on first start -- the
     * reason the read path keeps an unbound fallback.
     */
    public void testAnEntryWrittenWithoutTheBindingIsStillReadable() throws Exception {
        SegmentBundle bundle = twoFileBundle();
        BundleFileEntry alpha = bundle.entries().get("alpha.bin");
        SecretKey key = newAesKey();
        Path cacheDirectory = createTempDir();

        CountingReader counting = new CountingReader(bundle);
        LocalDiskCachingBundleStore cache = new LocalDiskCachingBundleStore(counting, cacheDirectory, new StaticEncryptionKeyProvider(key));
        assertEquals("AAAAAAAA", new String(cache.readFile("bundle-1", alpha), StandardCharsets.UTF_8));
        Path alphaPath = cacheFileMatching(cacheDirectory, alpha.offset());

        // Rewrite the entry exactly as the pre-binding build would have: encrypted, no associated data.
        byte[] plaintext = BundleReader.extractFile(bundle.bytes(), alpha);
        Files.write(alphaPath, AesGcmCipher.encrypt(plaintext, key));

        int callsBefore = counting.calls.get();
        assertEquals("AAAAAAAA", new String(cache.readFile("bundle-1", alpha), StandardCharsets.UTF_8));
        assertEquals("an unbound legacy entry must still be a hit, not a forced re-fetch", callsBefore, counting.calls.get());
    }
}
