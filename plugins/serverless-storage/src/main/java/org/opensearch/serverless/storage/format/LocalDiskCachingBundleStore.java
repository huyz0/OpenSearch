/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.serverless.storage.security.AesGcmCipher;
import org.opensearch.serverless.storage.security.EncryptionKeyProvider;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * <p><b>Optional, off-by-default size-bounded eviction</b> (rfc-serverless-opensearch.md
 * &sect;9.2's admission control): {@link #maxBytesOnDisk} &le; 0 (the default, via {@code
 * ServerlessStoragePlugin#SERVERLESS_STORAGE_LOCAL_CACHE_MAX_BYTES_PER_SHARD_SETTING} being unset)
 * leaves this exactly the unbounded cache it always was -- every existing deployment is untouched.
 * When a positive budget is configured, this evicts the least-recently-*touched* file (by disk
 * {@code lastModifiedTime}, refreshed on every hit, not just on write) once the shard's cache
 * directory exceeds it, sweeping down to {@link #EVICTION_TARGET_FRACTION} of the budget so a
 * write sitting right at the line doesn't immediately re-trigger another sweep. <b>This is a
 * first-pass policy, not one tuned against real workload data</b> -- plain LRU-by-mtime over a
 * directory listing, evaluated synchronously (CAS-guarded so only one thread sweeps at a time) on
 * whichever request's write happens to cross the budget, not a background schedule. Good enough to
 * cap disk growth; not represented as the final word on cache admission policy for this tier.
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

    private static final Logger logger = LogManager.getLogger(LocalDiskCachingBundleStore.class);

    /** Sweep down to this fraction of {@link #maxBytesOnDisk} once eviction triggers, so a write sitting right at the line doesn't immediately re-trigger another sweep. */
    private static final double EVICTION_TARGET_FRACTION = 0.9;

    private final BundleFileReader delegate;
    private final Path cacheDirectory;
    private final EncryptionKeyProvider encryptionKeyProvider;
    // Guards the read-check-write-then-rename sequence for one cache key so concurrent readers of
    // the same file don't race to write the same temp file; different keys never contend.
    private final ConcurrentMap<String, Object> locksByKey = new ConcurrentHashMap<>();
    private final AtomicLong hitCount = new AtomicLong();
    private final AtomicLong missCount = new AtomicLong();

    /** {@code <= 0} means unbounded -- see this class's own javadoc. */
    private final long maxBytesOnDisk;
    private final AtomicLong evictedCount = new AtomicLong();
    // CAS-guarded so only one thread runs a sweep at a time; a write that loses the race just
    // leaves its own overage for the next write to catch, same "best-effort, never blocks
    // correctness" shape as this plugin's other in-flight guards (e.g. ObjectStoreWriterEngine's
    // refreshPublicationInFlight).
    private final AtomicBoolean evictionInProgress = new AtomicBoolean(false);

    /**
     * Wraps a delegate reader with a plaintext-on-disk cache, unbounded (no eviction).
     *
     * @param delegate the underlying reader to consult on a cache miss.
     * @param cacheDirectory the local directory to cache files in.
     */
    public LocalDiskCachingBundleStore(BundleFileReader delegate, Path cacheDirectory) throws IOException {
        this(delegate, cacheDirectory, null);
    }

    /**
     * Wraps a delegate reader with an unbounded disk cache, optionally encrypting cached files at rest.
     *
     * @param delegate the underlying reader to consult on a cache miss.
     * @param cacheDirectory the local directory to cache files in.
     * @param encryptionKeyProvider {@code null} to cache plaintext on disk, matching the no-arg constructor.
     */
    public LocalDiskCachingBundleStore(BundleFileReader delegate, Path cacheDirectory, EncryptionKeyProvider encryptionKeyProvider)
        throws IOException {
        this(delegate, cacheDirectory, encryptionKeyProvider, 0L);
    }

    /**
     * Wraps a delegate reader with a disk cache, optionally encrypting cached files at rest and
     * optionally bounding its size with LRU-by-mtime eviction -- see this class's own javadoc for
     * the eviction mechanism and its "first-pass, not workload-tuned" caveat.
     *
     * @param delegate the underlying reader to consult on a cache miss.
     * @param cacheDirectory the local directory to cache files in.
     * @param encryptionKeyProvider {@code null} to cache plaintext on disk.
     * @param maxBytesOnDisk {@code <= 0} (the default) leaves this cache unbounded; a positive value
     *                       is the byte budget this cache directory evicts down to once crossed.
     */
    public LocalDiskCachingBundleStore(
        BundleFileReader delegate,
        Path cacheDirectory,
        EncryptionKeyProvider encryptionKeyProvider,
        long maxBytesOnDisk
    ) throws IOException {
        this.delegate = delegate;
        this.cacheDirectory = cacheDirectory;
        this.encryptionKeyProvider = encryptionKeyProvider;
        this.maxBytesOnDisk = maxBytesOnDisk;
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
                    touch(cachedPath);
                    return cached;
                }
                // A cached file that doesn't match its own name's recorded length/checksum can
                // only mean local disk corruption (bundles are immutable, so this can never be a
                // legitimately stale cache entry) -- fall through and re-fetch rather than trust it.
            }
            missCount.incrementAndGet();
            byte[] fresh = delegate.readFile(bundleName, entry);
            writeAtomically(cachedPath, encryptIfNeeded(fresh));
            maybeEvict();
            return fresh;
        }
    }

    /** Bumps {@code cachedPath}'s {@code lastModifiedTime} to now, so a hit keeps a hot file recently-touched for eviction purposes even if it was written long ago. */
    private void touch(Path cachedPath) {
        if (maxBytesOnDisk <= 0) {
            return; // eviction disabled -- no reason to pay a metadata write on every hit.
        }
        try {
            Files.setLastModifiedTime(cachedPath, FileTime.from(Instant.now()));
        } catch (IOException e) {
            // Best-effort recency tracking: at worst this file looks staler than it really is and
            // gets evicted a bit early, never a correctness issue (a miss just re-fetches it).
            logger.debug("failed to update cache entry recency for [" + cachedPath + "]", e);
        }
    }

    private void maybeEvict() {
        if (maxBytesOnDisk <= 0) {
            return;
        }
        if (evictionInProgress.compareAndSet(false, true) == false) {
            return;
        }
        try {
            evictIfOverBudget();
        } catch (IOException e) {
            // Eviction failing must never fail the write/read that triggered it -- the cache
            // directory just stays over budget until the next successful sweep.
            logger.warn("failed to evict entries from local disk cache [" + cacheDirectory + "]", e);
        } finally {
            evictionInProgress.set(false);
        }
    }

    private void evictIfOverBudget() throws IOException {
        List<CacheEntry> entries = listCacheEntries();
        long totalBytes = 0;
        for (CacheEntry entry : entries) {
            totalBytes += entry.sizeBytes;
        }
        if (totalBytes <= maxBytesOnDisk) {
            return;
        }
        long targetBytes = (long) (maxBytesOnDisk * EVICTION_TARGET_FRACTION);
        entries.sort(Comparator.comparing(e -> e.lastModifiedTime));
        for (CacheEntry entry : entries) {
            if (totalBytes <= targetBytes) {
                break;
            }
            try {
                Files.deleteIfExists(entry.path);
                totalBytes -= entry.sizeBytes;
                evictedCount.incrementAndGet();
            } catch (IOException e) {
                // Another thread's concurrent write/rename raced this file, or it's already gone --
                // move on to the next candidate rather than aborting the whole sweep.
                logger.debug("failed to evict cache entry [" + entry.path + "]", e);
            }
        }
    }

    private List<CacheEntry> listCacheEntries() throws IOException {
        List<CacheEntry> entries = new ArrayList<>();
        try (
            DirectoryStream<Path> stream = Files.newDirectoryStream(
                cacheDirectory,
                p -> p.getFileName().toString().contains(".tmp-") == false
            )
        ) {
            for (Path path : stream) {
                if (Files.isRegularFile(path) == false) {
                    continue;
                }
                try {
                    entries.add(new CacheEntry(path, Files.size(path), Files.getLastModifiedTime(path)));
                } catch (IOException e) {
                    // Deleted or replaced between the listing and this stat -- not a candidate either way.
                }
            }
        }
        return entries;
    }

    /** Number of cache entries evicted so far by the size-bounded sweep -- always 0 when eviction is disabled. */
    public long evictedCount() {
        return evictedCount.get();
    }

    private record CacheEntry(Path path, long sizeBytes, FileTime lastModifiedTime) {
    }

    private byte[] encryptIfNeeded(byte[] plaintext) throws IOException {
        return encryptionKeyProvider == null ? plaintext : AesGcmCipher.encrypt(plaintext, encryptionKeyProvider.currentKey());
    }

    private byte[] decryptIfNeeded(byte[] bytes) throws IOException {
        return encryptionKeyProvider == null ? bytes : AesGcmCipher.decrypt(bytes, encryptionKeyProvider.currentKey());
    }

    /** Number of reads served from the local disk cache. */
    public long hitCount() {
        return hitCount.get();
    }

    /** Number of reads that missed the local disk cache and fell through to the delegate. */
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
