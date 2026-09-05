/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A bounded cache of fixed-size byte ranges fetched from the object store.
 *
 * <p>The unit of transfer for a reader. Without it, serving a shard means downloading every segment
 * file whole — hundreds of megabytes to answer a query that touches tens of kilobytes — which is what
 * made cache placement carry more weight than it should. A query reads postings and stored fields at
 * scattered offsets; fetching the blocks it actually touches is the difference between a cold read
 * costing seconds and costing tens of milliseconds.
 *
 * <p>Counters are part of the interface, not instrumentation added later: a lazy reader that quietly
 * fetches everything is indistinguishable from an eager one unless someone is counting bytes.
 *
 * <p><b>Misses are fetched once, however many readers miss at the same moment.</b> Every shard on the
 * node shares this cache, and a hot block — the start of a postings list, the segment info of a segment
 * every query opens — is missed by every concurrent query at once when it is cold. Each of them used to
 * issue its own range request; {@link #fetch} lets the first one fetch and the rest wait for it, which is
 * one GET rather than one per query.
 *
 * <p><b>Striped rather than one monitor</b>, because every 64 KB read on the node used to take the same
 * lock. A small cache stays a single stripe so that it evicts exactly the least recently used block,
 * which is what a test sized to a handful of blocks is asserting; a large one is split by key hash into
 * stripes that are each their own least-recently-used list.
 */
public final class BlockCache {

    /** Default block size. Large enough to amortise a request, small enough that a miss is cheap. */
    public static final int DEFAULT_BLOCK_SIZE = 64 * 1024;

    /** Caches holding fewer blocks than this are one stripe, so their eviction order is exact. */
    static final int STRIPING_THRESHOLD_BLOCKS = 256;

    /** How many stripes a large cache is split into. */
    static final int STRIPES = 16;

    /** Fetches one block from wherever it lives when the cache does not have it. */
    @FunctionalInterface
    public interface Fetcher {
        /**
         * Fetches the block.
         *
         * @return the block's bytes
         * @throws IOException if the fetch fails
         */
        byte[] fetch() throws IOException;
    }

    /**
     * What the cache has done and what it holds, read without a lock.
     *
     * @param hits lookups served from memory
     * @param misses lookups that found nothing in memory
     * @param coalesced misses that waited on another thread's fetch of the same block instead of fetching
     * @param bytesFetched bytes fetched from the object store
     * @param blocksResident blocks currently held
     * @param bytesResident bytes currently held
     * @param maxBlocks the most blocks the cache will hold
     * @param blockSize bytes per block
     */
    public record Stats(long hits, long misses, long coalesced, long bytesFetched, long blocksResident, long bytesResident, int maxBlocks,
        int blockSize) {
    }

    private final int blockSize;
    private final int maxBlocks;
    private final Stripe[] stripes;
    private final ConcurrentHashMap<String, CompletableFuture<byte[]>> inFlight = new ConcurrentHashMap<>();
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong coalesced = new AtomicLong();
    private final AtomicLong bytesFetched = new AtomicLong();
    private final AtomicLong bytesResident = new AtomicLong();
    private final AtomicLong blocksResident = new AtomicLong();

    /**
     * Creates a cache.
     *
     * @param blockSize bytes per block
     * @param maxBlocks how many blocks to retain before evicting the least recently used
     */
    public BlockCache(int blockSize, int maxBlocks) {
        this.blockSize = blockSize;
        this.maxBlocks = maxBlocks;
        final int count = maxBlocks < STRIPING_THRESHOLD_BLOCKS ? 1 : STRIPES;
        this.stripes = new Stripe[count];
        // Rounded up, so the stripes together hold at least maxBlocks; a cache that held fewer than it
        // was configured for would be a quiet regression in every cost measurement.
        final int perStripe = (maxBlocks + count - 1) / count;
        for (int i = 0; i < count; i++) {
            stripes[i] = new Stripe(perStripe);
        }
    }

    /**
     * Returns the block size in bytes.
     *
     * @return the block size
     */
    public int blockSize() {
        return blockSize;
    }

    /**
     * Looks up a cached block.
     *
     * @param key the block's identity
     * @return the block, or null on a miss
     */
    public byte[] get(String key) {
        final byte[] block = stripeFor(key).get(key);
        if (block == null) {
            misses.incrementAndGet();
        } else {
            hits.incrementAndGet();
        }
        return block;
    }

    /**
     * Stores a freshly fetched block.
     *
     * @param key the block's identity
     * @param block the bytes
     */
    public void put(String key, byte[] block) {
        final Stripe stripe = stripeFor(key);
        final long evicted = stripe.put(key, block);
        bytesFetched.addAndGet(block.length);
        bytesResident.addAndGet(block.length - evicted);
        blocksResident.set(residentBlocks());
    }

    /**
     * Returns a block, fetching it at most once however many callers ask at the same moment.
     *
     * <p>The first caller to miss fetches; every caller that misses the same key while that fetch is in
     * flight waits for its result rather than issuing a request of its own. A fetch that fails is not
     * cached and its failure is reported to every waiter, each of which may retry.
     *
     * @param key the block's identity
     * @param fetcher how to fetch it on a miss
     * @return the block
     * @throws IOException if the fetch failed
     */
    public byte[] fetch(String key, Fetcher fetcher) throws IOException {
        final byte[] cached = get(key);
        if (cached != null) {
            return cached;
        }
        final CompletableFuture<byte[]> mine = new CompletableFuture<>();
        final CompletableFuture<byte[]> theirs = inFlight.putIfAbsent(key, mine);
        if (theirs != null) {
            coalesced.incrementAndGet();
            try {
                return theirs.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting for another reader's fetch of " + key, e);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof IOException io) {
                    throw new IOException("the fetch of " + key + " this reader waited on failed", io);
                }
                throw new IOException("the fetch of " + key + " this reader waited on failed", e.getCause());
            }
        }
        try {
            final byte[] block = fetcher.fetch();
            put(key, block);
            mine.complete(block);
            return block;
        } catch (IOException | RuntimeException | Error e) {
            mine.completeExceptionally(e);
            throw e;
        } finally {
            inFlight.remove(key, mine);
        }
    }

    /**
     * Returns how many block lookups were served from memory.
     *
     * @return the hit count
     */
    public long hits() {
        return hits.get();
    }

    /**
     * Returns how many block lookups had to go to the object store.
     *
     * @return the miss count
     */
    public long misses() {
        return misses.get();
    }

    /**
     * Returns how many misses waited on a fetch another thread was already making.
     *
     * @return the coalesced count
     */
    public long coalesced() {
        return coalesced.get();
    }

    /**
     * Returns how many bytes were actually fetched from the object store.
     *
     * <p>The number that says whether reads are lazy. Compared against the size of the segments, it is
     * the whole claim.
     *
     * @return bytes fetched
     */
    public long bytesFetched() {
        return bytesFetched.get();
    }

    /**
     * Returns how many bytes the cache currently holds.
     *
     * <p>Heap that no circuit breaker sees unless somebody reports it; this is the number to report.
     *
     * @return bytes resident
     */
    public long bytesResident() {
        return bytesResident.get();
    }

    /**
     * Returns the counters and the occupancy as one reading.
     *
     * @return the stats
     */
    public Stats stats() {
        return new Stats(
            hits.get(),
            misses.get(),
            coalesced.get(),
            bytesFetched.get(),
            residentBlocks(),
            bytesResident.get(),
            maxBlocks,
            blockSize
        );
    }

    /** Resets the counters, leaving cached blocks in place. */
    public void resetCounters() {
        hits.set(0);
        misses.set(0);
        coalesced.set(0);
        bytesFetched.set(0);
    }

    private long residentBlocks() {
        long total = 0;
        for (Stripe stripe : stripes) {
            total += stripe.size();
        }
        return total;
    }

    private Stripe stripeFor(String key) {
        return stripes.length == 1 ? stripes[0] : stripes[Math.floorMod(key.hashCode(), stripes.length)];
    }

    /** One least-recently-used list under its own monitor. */
    private static final class Stripe {

        private final Map<String, byte[]> blocks;
        private long evictedBytes;

        Stripe(int max) {
            this.blocks = new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
                    if (size() > max) {
                        evictedBytes += eldest.getValue().length;
                        return true;
                    }
                    return false;
                }
            };
        }

        synchronized byte[] get(String key) {
            return blocks.get(key);
        }

        /** Stores a block and returns how many bytes left the stripe to make room, replaced value included. */
        synchronized long put(String key, byte[] block) {
            evictedBytes = 0;
            final byte[] replaced = blocks.put(key, block);
            return evictedBytes + (replaced == null ? 0 : replaced.length);
        }

        synchronized int size() {
            return blocks.size();
        }
    }
}
