/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import java.util.LinkedHashMap;
import java.util.Map;
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
 */
public final class BlockCache {

    /** Default block size. Large enough to amortise a request, small enough that a miss is cheap. */
    public static final int DEFAULT_BLOCK_SIZE = 64 * 1024;

    private final int blockSize;
    private final int maxBlocks;
    private final Map<String, byte[]> blocks;
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong bytesFetched = new AtomicLong();

    /**
     * Creates a cache.
     *
     * @param blockSize bytes per block
     * @param maxBlocks how many blocks to retain before evicting the least recently used
     */
    public BlockCache(int blockSize, int maxBlocks) {
        this.blockSize = blockSize;
        this.maxBlocks = maxBlocks;
        this.blocks = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
                return size() > BlockCache.this.maxBlocks;
            }
        };
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
    public synchronized byte[] get(String key) {
        final byte[] block = blocks.get(key);
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
    public synchronized void put(String key, byte[] block) {
        blocks.put(key, block);
        bytesFetched.addAndGet(block.length);
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

    /** Resets the counters, leaving cached blocks in place. */
    public void resetCounters() {
        hits.set(0);
        misses.set(0);
        bytesFetched.set(0);
    }
}
