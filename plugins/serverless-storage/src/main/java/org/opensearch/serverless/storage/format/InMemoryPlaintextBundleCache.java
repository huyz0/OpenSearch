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
 *
 * <p><b>Striped, not one global lock.</b> Every {@link #readFile} hit re-links the touched entry
 * to the most-recently-used end of a {@link LinkedHashMap}, which is a write, not just a read --
 * a single shared lock around one such map serializes every concurrent cache hit across every
 * reader shard on the node, even though each individual critical section is pure in-memory work.
 * Measured under concurrent hits (16+ threads, all-cache-resident keys), a single global lock's
 * throughput actually falls below its own 2-thread throughput as thread count grows, rather than
 * merely plateauing -- real contention, not a theoretical one. The keyspace is instead partitioned
 * across {@link #stripes}, each an independently-locked {@link Stripe} holding its own slice of
 * {@code maxTotalBytes}: LRU eviction becomes per-stripe rather than a single global ordering, and
 * an entry evicts only other entries that hash to the same stripe. This is the same tradeoff
 * {@code ConcurrentHashMap}-backed caches (Guava, Caffeine) make -- approximate LRU under real
 * concurrency beats exact LRU serialized behind one lock. For small budgets (below {@link
 * #MIN_BYTES_PER_STRIPE} per stripe -- covers every test in this class and any tiny cache) the
 * stripe count collapses to 1, which is exactly the pre-striping single-map behavior: this only
 * changes behavior once the budget is large enough for multiple stripes to hold a meaningful share
 * each, which is also precisely the regime where contention was ever going to matter.
 */
public final class InMemoryPlaintextBundleCache {

    /** Below this many bytes per stripe, striping buys nothing and only fragments a tiny budget. */
    private static final long MIN_BYTES_PER_STRIPE = 1024L;
    /** Caps lock fragmentation (and per-stripe fixed overhead) once the budget is large. */
    private static final int MAX_STRIPES = 32;

    private final Stripe[] stripes;
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
        int stripeCount = (int) Math.max(1, Math.min(MAX_STRIPES, maxTotalBytes / MIN_BYTES_PER_STRIPE));
        long perStripeBudget = maxTotalBytes / stripeCount;
        this.stripes = new Stripe[stripeCount];
        for (int i = 0; i < stripeCount; i++) {
            stripes[i] = new Stripe(perStripeBudget);
        }
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
        Stripe stripe = stripes[stripeIndexFor(key)];

        byte[] cached = stripe.get(key); // marks it most-recently-used within its own stripe
        if (cached != null) {
            hitCount.incrementAndGet();
            // A defensive copy, not the cached instance itself. Handing out the live array made
            // this cache one Arrays.fill or one in-place decrypt away from poisoning every
            // subsequent hit for that key, for every shard on the node, with no way to detect it --
            // the entry would still have the right length and would never be re-verified, because
            // this tier (unlike LocalDiskCachingBundleStore) has no checksum re-check on a hit. No
            // caller mutates the array today, which is precisely why the bug would be latent until
            // the day one does. The cost is one memcpy per hit; the alternative is an unbounded,
            // silent, node-wide wrong-bytes class, and this tier exists in front of one that
            // already re-verifies, so the memcpy is the cheap side of the trade.
            return cached.clone();
        }

        missCount.incrementAndGet();
        byte[] fresh = onMiss.readFile(bundleName, entry);
        // The cached copy and the returned array are deliberately distinct for the same reason as
        // the hit path above: the caller owns what it is handed and may do anything with it.
        stripe.put(key, fresh.clone());
        return fresh;
    }

    private int stripeIndexFor(String key) {
        // Same spreading trick HashMap/ConcurrentHashMap use on hashCode() -- without it, keys
        // that only differ in low bits (identical bundle name, adjacent offsets) could alias onto
        // the same stripe far more often than chance would predict.
        int h = key.hashCode();
        h ^= (h >>> 16);
        return (h & 0x7fffffff) % stripes.length;
    }

    /** Number of reads served from the in-memory cache. */
    public long hitCount() {
        return hitCount.get();
    }

    /** Number of reads that missed the in-memory cache and fell through to the miss-path reader. */
    public long missCount() {
        return missCount.get();
    }

    /** Current total bytes held across every stripe -- exposed for tests/metrics, not part of the read path's contract. */
    long currentTotalBytes() {
        long total = 0;
        for (Stripe stripe : stripes) {
            total += stripe.currentTotalBytes();
        }
        return total;
    }

    /** Exposed for tests only, to find bundle-file keys that land in a chosen stripe deterministically. */
    int stripeIndexFor(String bundleName, BundleFileEntry entry) {
        return stripeIndexFor(cacheKey(bundleName, entry));
    }

    /** Exposed for tests only, to assert the sizing formula collapses to 1 stripe for small budgets. */
    int stripeCountForTesting() {
        return stripes.length;
    }

    /**
     * The content-addressed identity of one cached file: bundle name plus offset, length and
     * CRC32C. Package-private rather than private so {@link CachingBundleFileReader} can key its
     * own per-key single-flight map on exactly the same identity this cache keys entries on -- two
     * independent definitions of "the same file" would silently let a fetch dedupe against the
     * wrong entry, or fail to dedupe at all.
     *
     * @param bundleName the name of the bundle containing the file.
     * @param entry the file's location and expected checksum within the bundle.
     * @return the cache key for this file.
     */
    static String cacheKey(String bundleName, BundleFileEntry entry) {
        return bundleName + "-" + entry.offset() + "-" + entry.length() + "-" + entry.checksum();
    }

    /**
     * One independently-locked shard of the cache: its own LRU map, its own lock, and its own
     * fixed slice of the total byte budget. Eviction here never looks at, or affects, any other
     * stripe -- the tradeoff striping makes, see this class's own javadoc for why it's safe.
     */
    private static final class Stripe {

        private final long maxBytes;
        private final Object lock = new Object();
        private final LinkedHashMap<String, byte[]> entriesByKey;
        private long currentTotalBytes;

        Stripe(long maxBytes) {
            this.maxBytes = maxBytes;
            // accessOrder=true turns iteration order into LRU order; removeEldestEntry below is
            // never used since eviction has to happen by total bytes, not entry count, so it's
            // done manually.
            this.entriesByKey = new LinkedHashMap<>(16, 0.75f, true);
        }

        byte[] get(String key) {
            synchronized (lock) {
                return entriesByKey.get(key); // get(), not containsKey+get, so this also marks it most-recently-used
            }
        }

        void put(String key, byte[] fresh) {
            if (fresh.length > maxBytes) {
                return;
            }
            synchronized (lock) {
                // Two concurrent misses for the same key both reach here independently (the lock
                // is released between get() and this put()) -- byte[] previous holds whichever one
                // this put() is about to overwrite, so its length is subtracted rather than double-
                // counted. Without this, currentTotalBytes drifts permanently upward on every such
                // race, since only one value ever actually survives in the map but both writers'
                // lengths would otherwise be added.
                byte[] previous = entriesByKey.put(key, fresh);
                currentTotalBytes += fresh.length - (previous == null ? 0 : previous.length);
                evictUntilWithinBudget();
            }
        }

        private void evictUntilWithinBudget() {
            var iterator = entriesByKey.entrySet().iterator();
            while (currentTotalBytes > maxBytes && iterator.hasNext()) {
                Map.Entry<String, byte[]> eldest = iterator.next();
                currentTotalBytes -= eldest.getValue().length;
                iterator.remove();
            }
        }

        long currentTotalBytes() {
            synchronized (lock) {
                return currentTotalBytes;
            }
        }
    }
}
