/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import org.opensearch.serverless.storage.security.AesGcmCipher;
import org.opensearch.serverless.storage.security.EncryptionKeyProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32C;

/**
 * The first slice of the "directory tier" (rfc-serverless-opensearch.md &sect;9): a read-through
 * local-disk cache in front of a real {@link BundleFileReader}, so a reader shard that has
 * already fetched a file doesn't re-fetch it from the object store on every query. Segment bundle
 * files are immutable once written (a bundle is never modified after {@code writeBundle}, only
 * superseded by a new one after a merge/compaction), so this cache never needs invalidation --
 * once a {@code (bundleName, entry)} pair is on disk, it is correct forever.
 *
 * <p>Deliberately not an LRU or size-bounded cache yet: eviction policy is a genuinely separate
 * concern (rfc-serverless-opensearch.md &sect;9.2's admission control) that needs real workload
 * data to tune sensibly, not a default guessed at here. This class is the correctness-and-hit-path
 * foundation an eviction policy would sit on top of.
 *
 * <p>If an {@link EncryptionKeyProvider} is supplied, every file this cache writes to local disk
 * is encrypted first and decrypted on read back -- when {@code
 * ServerlessStoragePlugin#SERVERLESS_STORAGE_ENCRYPTION_KEY_SETTING} is set, the delegate this
 * class wraps has already decrypted the bytes on their way out of {@code EncryptingBlobContainer}
 * (that's a separate, independent encryption boundary at the object-store seam -- see its
 * javadoc), so without this, "encryption at rest" would be true of the object store but silently
 * false of every reader node's local disk cache and the OS page cache backing it. This is why
 * {@link org.opensearch.serverless.storage.format.InMemoryPlaintextBundleCache} exists as a
 * companion, not a replacement: wrap this class in that one, and the decrypt cost this constructor
 * adds is only paid on a disk-cache hit that missed the (bounded, in-memory, plaintext) layer in
 * front of it, not on every read.
 */
public final class LocalDiskCachingBundleStore implements BundleFileReader {

    private final BundleFileReader delegate;
    private final Path cacheDirectory;
    private final EncryptionKeyProvider encryptionKeyProvider;
    // Guards the read-check-write-then-rename sequence for one cache key so concurrent readers of
    // the same file don't race to write the same temp file; different keys never contend.
    private final ConcurrentMap<String, Object> locksByKey = new ConcurrentHashMap<>();
    private final AtomicLong hitCount = new AtomicLong();
    private final AtomicLong missCount = new AtomicLong();

    public LocalDiskCachingBundleStore(BundleFileReader delegate, Path cacheDirectory) throws IOException {
        this(delegate, cacheDirectory, null);
    }

    /** @param encryptionKeyProvider {@code null} to cache plaintext on disk, matching the no-arg constructor. */
    public LocalDiskCachingBundleStore(BundleFileReader delegate, Path cacheDirectory, EncryptionKeyProvider encryptionKeyProvider)
        throws IOException {
        this.delegate = delegate;
        this.cacheDirectory = cacheDirectory;
        this.encryptionKeyProvider = encryptionKeyProvider;
        Files.createDirectories(cacheDirectory);
    }

    @Override
    public byte[] readFile(String bundleName, BundleFileEntry entry) throws IOException {
        Path cachedPath = cachePathFor(bundleName, entry);
        synchronized (lockFor(cachedPath)) {
            if (Files.exists(cachedPath)) {
                byte[] cached = null;
                try {
                    cached = decryptIfNeeded(Files.readAllBytes(cachedPath));
                } catch (IOException e) {
                    // A cache entry that fails to decrypt (corrupted/truncated on disk, or a
                    // leftover plaintext file from before encryption was enabled) is exactly as
                    // untrustworthy as one that fails its checksum below -- fall through and
                    // re-fetch rather than propagate, same reasoning as the checksum-mismatch case.
                }
                if (cached != null && cached.length == entry.length() && checksum(cached) == entry.checksum()) {
                    hitCount.incrementAndGet();
                    return cached;
                }
                // A cached file that doesn't match its own name's recorded length/checksum can
                // only mean local disk corruption (bundles are immutable, so this can never be a
                // legitimately stale cache entry) -- fall through and re-fetch rather than trust it.
            }
            missCount.incrementAndGet();
            byte[] fresh = delegate.readFile(bundleName, entry);
            writeAtomically(cachedPath, encryptIfNeeded(fresh));
            return fresh;
        }
    }

    private byte[] encryptIfNeeded(byte[] plaintext) throws IOException {
        return encryptionKeyProvider == null ? plaintext : AesGcmCipher.encrypt(plaintext, encryptionKeyProvider.currentKey());
    }

    private byte[] decryptIfNeeded(byte[] bytes) throws IOException {
        return encryptionKeyProvider == null ? bytes : AesGcmCipher.decrypt(bytes, encryptionKeyProvider.currentKey());
    }

    public long hitCount() {
        return hitCount.get();
    }

    public long missCount() {
        return missCount.get();
    }

    private Path cachePathFor(String bundleName, BundleFileEntry entry) {
        String key = bundleName + "-" + entry.offset() + "-" + entry.length() + "-" + entry.checksum();
        return cacheDirectory.resolve(sanitize(key));
    }

    private static String sanitize(String key) {
        StringBuilder sanitized = new StringBuilder(key.length());
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            sanitized.append(Character.isLetterOrDigit(c) || c == '-' || c == '_' ? c : '_');
        }
        return sanitized.toString();
    }

    private Object lockFor(Path cachedPath) {
        return locksByKey.computeIfAbsent(cachedPath.toString(), p -> new Object());
    }

    private void writeAtomically(Path cachedPath, byte[] bytes) throws IOException {
        Path tempPath = cachedPath.resolveSibling(cachedPath.getFileName() + ".tmp-" + Thread.currentThread().threadId());
        Files.write(tempPath, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        Files.move(tempPath, cachedPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private static long checksum(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes);
        return crc.getValue();
    }
}
