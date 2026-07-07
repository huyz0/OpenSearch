/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

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
 */
public final class LocalDiskCachingBundleStore implements BundleFileReader {

    private final BundleFileReader delegate;
    private final Path cacheDirectory;
    // Guards the read-check-write-then-rename sequence for one cache key so concurrent readers of
    // the same file don't race to write the same temp file; different keys never contend.
    private final ConcurrentMap<String, Object> locksByKey = new ConcurrentHashMap<>();
    private final AtomicLong hitCount = new AtomicLong();
    private final AtomicLong missCount = new AtomicLong();

    public LocalDiskCachingBundleStore(BundleFileReader delegate, Path cacheDirectory) throws IOException {
        this.delegate = delegate;
        this.cacheDirectory = cacheDirectory;
        Files.createDirectories(cacheDirectory);
    }

    @Override
    public byte[] readFile(String bundleName, BundleFileEntry entry) throws IOException {
        Path cachedPath = cachePathFor(bundleName, entry);
        synchronized (lockFor(cachedPath)) {
            if (Files.exists(cachedPath)) {
                byte[] cached = Files.readAllBytes(cachedPath);
                if (cached.length == entry.length() && checksum(cached) == entry.checksum()) {
                    hitCount.incrementAndGet();
                    return cached;
                }
                // A cached file that doesn't match its own name's recorded length/checksum can
                // only mean local disk corruption (bundles are immutable, so this can never be a
                // legitimately stale cache entry) -- fall through and re-fetch rather than trust it.
            }
            missCount.incrementAndGet();
            byte[] fresh = delegate.readFile(bundleName, entry);
            writeAtomically(cachedPath, fresh);
            return fresh;
        }
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
