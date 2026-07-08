/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A bounded, size-limited, in-process LRU cache of decrypted file bytes, meant to sit in front of
 * {@link LocalDiskCachingBundleStore} when encryption is enabled: that class's disk cache holds
 * ciphertext (so a reader node's local disk and OS page cache never hold plaintext at rest), which
 * means every disk-cache hit still pays one AES/GCM decrypt. This class exists to keep the truly
 * hot working set decrypted in JVM heap, so repeated reads of the same file (the common case for a
 * reader shard serving many queries against the same recent segments) skip both the disk I/O and
 * the decrypt, and only the first read of a given file after this cache is cold pays either cost.
 *
 * <p>Unlike {@link LocalDiskCachingBundleStore} (correct to keep indefinitely -- bundle files are
 * immutable, and disk is cheap), this cache is memory, which is not: it is bounded by total bytes
 * and evicts least-recently-used entries once that bound is exceeded. An entry larger than the
 * entire cap is never cached at all (it would immediately evict everything else for a single-use
 * gain) -- it is still served correctly, just via a pass-through miss every time.
 *
 * <p>Safe to use even when nothing is encrypted (the delegate is a plain object-store fetch, or an
 * unencrypted {@link LocalDiskCachingBundleStore}): it has no opinion on what its delegate does,
 * only that whatever bytes it returns are safe to hold in heap and safe to hand back verbatim on a
 * later hit for the same {@code (bundleName, entry)}, which is true either way since bundle files
 * are immutable.
 */
public final class InMemoryPlaintextBundleCache implements BundleFileReader {

    private final BundleFileReader delegate;
    private final long maxTotalBytes;
    private final Object lock = new Object();
    private final LinkedHashMap<String, byte[]> entriesByKey;
    private long currentTotalBytes;
    private final AtomicLong hitCount = new AtomicLong();
    private final AtomicLong missCount = new AtomicLong();

    public InMemoryPlaintextBundleCache(BundleFileReader delegate, long maxTotalBytes) {
        if (maxTotalBytes < 0) {
            throw new IllegalArgumentException("maxTotalBytes must be >= 0, got " + maxTotalBytes);
        }
        this.delegate = delegate;
        this.maxTotalBytes = maxTotalBytes;
        // accessOrder=true turns iteration order into LRU order; removeEldestEntry below is never
        // used since eviction has to happen by total bytes, not entry count, so it's done manually.
        this.entriesByKey = new LinkedHashMap<>(16, 0.75f, true);
    }

    @Override
    public byte[] readFile(String bundleName, BundleFileEntry entry) throws IOException {
        String key = cacheKey(bundleName, entry);
        synchronized (lock) {
            byte[] cached = entriesByKey.get(key); // get(), not containsKey+get, so this also marks it most-recently-used
            if (cached != null) {
                hitCount.incrementAndGet();
                return cached;
            }
        }

        missCount.incrementAndGet();
        byte[] fresh = delegate.readFile(bundleName, entry);

        synchronized (lock) {
            if (fresh.length <= maxTotalBytes) {
                entriesByKey.put(key, fresh);
                currentTotalBytes += fresh.length;
                evictUntilWithinBudget();
            }
        }
        return fresh;
    }

    private void evictUntilWithinBudget() {
        var iterator = entriesByKey.entrySet().iterator();
        while (currentTotalBytes > maxTotalBytes && iterator.hasNext()) {
            Map.Entry<String, byte[]> eldest = iterator.next();
            currentTotalBytes -= eldest.getValue().length;
            iterator.remove();
        }
    }

    public long hitCount() {
        return hitCount.get();
    }

    public long missCount() {
        return missCount.get();
    }

    /** Current total bytes held -- exposed for tests/metrics, not part of the read path's contract. */
    long currentTotalBytes() {
        synchronized (lock) {
            return currentTotalBytes;
        }
    }

    private static String cacheKey(String bundleName, BundleFileEntry entry) {
        return bundleName + "-" + entry.offset() + "-" + entry.length() + "-" + entry.checksum();
    }
}
