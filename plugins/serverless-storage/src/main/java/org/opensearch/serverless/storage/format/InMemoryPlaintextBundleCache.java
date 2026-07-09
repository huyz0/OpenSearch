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
 * <p><b>One instance per node, not per shard.</b> A budget is only meaningful against the node's
 * actual memory, so this class deliberately holds no per-shard state and no fixed delegate: {@link
 * #readFile} takes the miss-path {@link BundleFileReader} as an argument instead of a constructor
 * field, so one shared instance -- sized against the whole node's budget, not guessed per shard --
 * serves every reader shard on the node. {@link CachingBundleFileReader} adapts this back into a
 * plain {@link BundleFileReader} for a specific shard's miss path, which is what a shard's engine
 * factory actually wires up. Cache keys are the bundle name plus the entry's offset/length/checksum;
 * bundle names already embed index UUID and shard id (see {@code ObjectStoreCommitPublisher}), so
 * two different shards can never collide on the same key.
 */
public final class InMemoryPlaintextBundleCache {

    private final long maxTotalBytes;
    private final Object lock = new Object();
    private final LinkedHashMap<String, byte[]> entriesByKey;
    private long currentTotalBytes;
    private final AtomicLong hitCount = new AtomicLong();
    private final AtomicLong missCount = new AtomicLong();

    /**
     * Creates a node-wide cache bounded by total bytes held.
     *
     * @param maxTotalBytes the maximum total bytes of cached entries before LRU eviction kicks in.
     */
    public InMemoryPlaintextBundleCache(long maxTotalBytes) {
        if (maxTotalBytes < 0) {
            throw new IllegalArgumentException("maxTotalBytes must be >= 0, got " + maxTotalBytes);
        }
        this.maxTotalBytes = maxTotalBytes;
        // accessOrder=true turns iteration order into LRU order; removeEldestEntry below is never
        // used since eviction has to happen by total bytes, not entry count, so it's done manually.
        this.entriesByKey = new LinkedHashMap<>(16, 0.75f, true);
    }

    /**
     * Returns the cached bytes for {@code (bundleName, entry)} if present, otherwise fetches them
     * from {@code onMiss}, caches the result (subject to the byte budget), and returns it.
     *
     * @param bundleName the name of the bundle containing the file.
     * @param entry the file's location and expected checksum within the bundle.
     * @param onMiss the reader to fetch from on a cache miss.
     * @return the file's raw bytes.
     */
    public byte[] readFile(String bundleName, BundleFileEntry entry, BundleFileReader onMiss) throws IOException {
        String key = cacheKey(bundleName, entry);
        synchronized (lock) {
            byte[] cached = entriesByKey.get(key); // get(), not containsKey+get, so this also marks it most-recently-used
            if (cached != null) {
                hitCount.incrementAndGet();
                return cached;
            }
        }

        missCount.incrementAndGet();
        byte[] fresh = onMiss.readFile(bundleName, entry);

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

    /** Number of reads served from the in-memory cache. */
    public long hitCount() {
        return hitCount.get();
    }

    /** Number of reads that missed the in-memory cache and fell through to the miss-path reader. */
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
