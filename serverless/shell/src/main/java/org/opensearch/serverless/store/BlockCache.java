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
    private final AtomicLong bytesCopied = new AtomicLong();
    private volatile java.util.concurrent.Executor prefetchExecutor;
    private volatile java.util.function.LongSupplier clock = System::currentTimeMillis;

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
     * Returns the most blocks this cache holds.
     *
     * @return the capacity in blocks
     */
    public int maxBlocks() {
        return maxBlocks;
    }

    /**
     * Lets a directory opened over this cache fetch the blocks its open will read concurrently, before
     * Lucene asks for them one after another. Without an executor nothing is fetched ahead.
     *
     * @param executor where the helper reads run, or null to fetch nothing ahead
     * @return this, for chaining
     */
    public BlockCache prefetchingWith(java.util.concurrent.Executor executor) {
        this.prefetchExecutor = executor;
        return this;
    }

    /** Where opens fetch ahead, or null when they do not. */
    java.util.concurrent.Executor prefetchExecutor() {
        return prefetchExecutor;
    }

    /** Looks up a block without counting a hit or a miss: a prefetch asking what it can skip is not a reader. */
    byte[] peek(String key) {
        return stripeFor(key).get(key, false, 0L);
    }

    /**
     * Whether a block is held, without counting a hit or a miss.
     *
     * @param key the block's identity
     * @return true if it is held
     */
    public boolean holds(String key) {
        return peek(key) != null;
    }

    /**
     * Looks up a cached block.
     *
     * @param key the block's identity
     * @return the block, or null on a miss
     */
    public byte[] get(String key) {
        final byte[] block = stripeFor(key).get(key, true, clock.getAsLong());
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
        // A fetched block was fetched to be read, by a search or an open, so it counts as read once now.
        final long evicted = stripe.put(key, block, clock.getAsLong(), 1);
        bytesFetched.addAndGet(block.length);
        bytesResident.addAndGet(block.length - evicted);
    }

    /**
     * Stores a block copied from a peer's cache, if this cache does not already hold it.
     *
     * <p>Stamped as never read here: it is hot on the peer, not yet on this node, and a node that is asked for its own
     * hot set before searches reach it names only what it has read itself.
     *
     * <p>Not counted as fetched: {@link #bytesFetched} is what the object store was asked for, and a copy from a peer
     * cost it nothing. Counted in {@link #bytesCopied} instead, so a warm join can show what it was spared.
     *
     * @param key the block's identity, the same on every node
     * @param block the bytes
     * @return true if it was stored, false if a block was already held under the key
     */
    public boolean putCopied(String key, byte[] block) {
        final Stripe stripe = stripeFor(key);
        final long evicted = stripe.putIfAbsent(key, block);
        if (evicted < 0) {
            return false;
        }
        bytesCopied.addAndGet(block.length);
        bytesResident.addAndGet(block.length - evicted);
        return true;
    }

    /**
     * Returns the keys held for each of some cache scopes, most recently used first; see {@link #residentEntriesByScope}.
     *
     * @param scopes the scopes, the part of a key before its first {@code /}
     * @return each scope's keys, most recent first; a scope holding nothing is absent
     */
    public Map<String, java.util.List<String>> residentByScope(java.util.Set<String> scopes) {
        final Map<String, java.util.List<String>> keys = new java.util.HashMap<>();
        for (Map.Entry<String, java.util.List<Resident>> scope : residentEntriesByScope(scopes).entrySet()) {
            keys.put(scope.getKey(), scope.getValue().stream().map(Resident::key).toList());
        }
        return keys;
    }

    /**
     * Returns the blocks held for each of some cache scopes, most recently used first, with how often and how lately
     * each was read here.
     *
     * <p>What a node hands a joining peer to choose from. One pass over the cache for all the scopes asked about, since a
     * node holding hundreds of shards would otherwise walk every block once per shard. Each stripe is its own
     * least-recently-used list and the stripes are taken in turn, so the order is by recency within a stripe and close
     * to it across them.
     *
     * @param scopes the scopes, the part of a key before its first {@code /}
     * @return each scope's blocks, most recent first; a scope holding nothing is absent
     */
    public Map<String, java.util.List<Resident>> residentEntriesByScope(java.util.Set<String> scopes) {
        final java.util.List<java.util.List<Resident>> perStripe = new java.util.ArrayList<>(stripes.length);
        for (Stripe stripe : stripes) {
            perStripe.add(stripe.residentMostRecentFirst(scopes));
        }
        final Map<String, java.util.List<Resident>> byScope = new java.util.HashMap<>();
        for (int i = 0, remaining = stripes.length; remaining > 0; i++) {
            remaining = 0;
            for (java.util.List<Resident> held : perStripe) {
                if (i < held.size()) {
                    final Resident block = held.get(i);
                    byScope.computeIfAbsent(scopeOf(block.key()), s -> new java.util.ArrayList<>()).add(block);
                }
                if (i + 1 < held.size()) {
                    remaining++;
                }
            }
        }
        return byScope;
    }

    /**
     * Replaces the clock that stamps when a block was last read, for a test.
     *
     * @param millis a millisecond clock
     * @return this
     */
    public BlockCache withClock(java.util.function.LongSupplier millis) {
        this.clock = millis;
        return this;
    }

    /**
     * Returns the time by the clock that stamps reads.
     *
     * @return milliseconds
     */
    public long nowMillis() {
        return clock.getAsLong();
    }

    /**
     * Returns the blocks held under some keys, leaving out any no longer held, without counting hits or misses: a
     * peer copying the cache is not a reader of it. Like {@link #peek}, it does refresh their recency, which is
     * harmless here since the keys a peer asks for are the ones {@link #residentByScope} named as the hottest.
     *
     * @param keys the blocks wanted
     * @return the ones held, in the order asked
     */
    public Map<String, byte[]> copyOut(java.util.List<String> keys) {
        final Map<String, byte[]> held = new LinkedHashMap<>();
        for (String key : keys) {
            final byte[] block = stripeFor(key).get(key, false, 0L);
            if (block != null) {
                held.put(key, block);
            }
        }
        return held;
    }

    /**
     * The scope a key belongs to: everything before its first {@code /}.
     *
     * @param key a block key
     * @return its scope
     */
    public static String scopeOf(String key) {
        final int slash = key.indexOf('/');
        return slash < 0 ? key : key.substring(0, slash);
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
     * Returns how many bytes were copied in from peers' caches rather than fetched from the object store.
     *
     * @return bytes copied
     */
    public long bytesCopied() {
        return bytesCopied.get();
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
        bytesCopied.set(0);
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

    /**
     * A block held, with when it was last read here and how often: what tells a hot block from one that is merely
     * resident. Mutable, and only touched under its stripe's monitor.
     */
    private static final class Entry {
        final byte[] block;
        long lastReadMillis;
        int reads;

        Entry(byte[] block, long lastReadMillis, int reads) {
            this.block = block;
            this.lastReadMillis = lastReadMillis;
            this.reads = reads;
        }
    }

    /**
     * A held block as a peer is told of it.
     *
     * @param key the block's key
     * @param bytes its size
     * @param reads how many times it was read here
     * @param lastReadMillis when it was last read here, or 0 if never: a block copied in and not yet read
     */
    public record Resident(String key, int bytes, int reads, long lastReadMillis) {
    }

    /** One least-recently-used list under its own monitor. */
    private static final class Stripe {

        private final Map<String, Entry> blocks;
        private long evictedBytes;

        Stripe(int max) {
            this.blocks = new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Entry> eldest) {
                    if (size() > max) {
                        evictedBytes += eldest.getValue().block.length;
                        return true;
                    }
                    return false;
                }
            };
        }

        /** Looks a block up; a read, not a peek, counts as one. */
        synchronized byte[] get(String key, boolean read, long now) {
            final Entry entry = blocks.get(key);
            if (entry == null) {
                return null;
            }
            if (read) {
                entry.reads++;
                entry.lastReadMillis = now;
            }
            return entry.block;
        }

        /** Stores a block and returns how many bytes left the stripe to make room, replaced value included. */
        synchronized long put(String key, byte[] block, long lastReadMillis, int reads) {
            evictedBytes = 0;
            final Entry replaced = blocks.put(key, new Entry(block, lastReadMillis, reads));
            return evictedBytes + (replaced == null ? 0 : replaced.block.length);
        }

        /** Stores a block unless one is held; returns the bytes evicted to make room, or -1 if one was held. */
        synchronized long putIfAbsent(String key, byte[] block) {
            if (blocks.containsKey(key)) {
                return -1;
            }
            return put(key, block, 0L, 0);
        }

        /** The blocks in some scopes, most recently used first. */
        synchronized java.util.List<Resident> residentMostRecentFirst(java.util.Set<String> scopes) {
            final java.util.ArrayList<Resident> held = new java.util.ArrayList<>();
            for (Map.Entry<String, Entry> entry : blocks.entrySet()) {
                if (scopes.contains(scopeOf(entry.getKey()))) {
                    final Entry e = entry.getValue();
                    held.add(new Resident(entry.getKey(), e.block.length, e.reads, e.lastReadMillis));
                }
            }
            java.util.Collections.reverse(held);
            return held;
        }

        synchronized int size() {
            return blocks.size();
        }
    }
}
