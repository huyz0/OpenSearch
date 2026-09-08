/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.opensearch.common.blobstore.BlobContainer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Reads one segment file out of the object store, a block at a time.
 *
 * <p>Extends {@link BufferedIndexInput} so Lucene's own buffering and slicing come for free and only
 * the fetch is ours. A read that crosses a block boundary fetches each block it touches and no more —
 * the point being that a query reading a few kilobytes of postings does not pay for the whole segment.
 */
final class ObjectStoreIndexInput extends BufferedIndexInput {

    private final BlobContainer container;
    private final BlockCache cache;
    /**
     * The most blocks one read-ahead may cover: 8 MB at the default block size.
     *
     * <p>A bound on two different things. The span is held whole in memory while it is split, and it is
     * also what a single mistaken guess costs -- a scan that stops right after the ramp reaches its cap has
     * fetched this much it will not read, and evicted this much that somebody else might have.
     */
    private static final int MAX_READAHEAD_BLOCKS = 128;

    private final String blobName;
    private final String cacheKeyPrefix;
    private final long length;
    private long position;

    /**
     * The highest block index the last fetch covered, or {@link Long#MIN_VALUE} before the first.
     *
     * <p>How a sequential scan is told from a seek. A miss for exactly the block after the last fetch ended
     * is a reader walking forward; anything else is a jump, and the two want opposite things.
     */
    private long fetchedThrough = Long.MIN_VALUE;

    /** How many blocks the next miss fetches. Doubles while the reads stay sequential, resets on a jump. */
    private int readahead = 1;

    ObjectStoreIndexInput(
        String resourceDescription,
        IOContext context,
        BlobContainer container,
        BlockCache cache,
        String blobName,
        String cacheKeyPrefix,
        long length
    ) {
        super(resourceDescription, context);
        this.container = container;
        this.cache = cache;
        this.blobName = blobName;
        this.cacheKeyPrefix = cacheKeyPrefix;
        this.length = length;
    }

    @Override
    protected void readInternal(ByteBuffer b) throws IOException {
        final int wanted = b.remaining();
        if (position + wanted > length) {
            throw new IOException("read past end of " + blobName + ": " + (position + wanted) + " > " + length);
        }
        long remainingOffset = position;
        int remaining = wanted;
        while (remaining > 0) {
            final int blockSize = cache.blockSize();
            final long blockIndex = remainingOffset / blockSize;
            final int withinBlock = (int) (remainingOffset - blockIndex * blockSize);
            final byte[] block = blockAt(blockIndex);
            final int available = block.length - withinBlock;
            final int copy = Math.min(available, remaining);
            b.put(block, withinBlock, copy);
            remainingOffset += copy;
            remaining -= copy;
        }
        position += wanted;
    }

    private byte[] blockAt(long blockIndex) throws IOException {
        final String key = cacheKeyPrefix + "#" + blockIndex;
        final byte[] resident = cache.get(key);
        if (resident != null) {
            // A hit says nothing about the access pattern -- under read-ahead most of a sequential scan is
            // hits, by construction -- so the detector below is left alone.
            return resident;
        }
        // Single-flighted: a cold block every concurrent query of this segment needs at once is fetched
        // by the first of them and handed to the rest, rather than fetched once per query.
        return cache.fetch(key, () -> {
            final int blockSize = cache.blockSize();
            // Sequential if this miss is for the block right after the last fetch ended. A scan therefore
            // ramps -- 1 block, 2, 4, 8 -- and a seek drops straight back to one, so random access costs
            // exactly what it did before read-ahead existed.
            readahead = blockIndex == fetchedThrough + 1 ? Math.min(readahead * 2, MAX_READAHEAD_BLOCKS) : 1;
            final long start = blockIndex * blockSize;
            final int size = (int) Math.min((long) readahead * blockSize, length - start);
            final byte[] span = new byte[size];
            // The one range request, and the whole point of the ramp: a cold scan of a large segment used
            // to pay one of these per 64 KB, which on an object store is one billed round trip per 64 KB.
            try (InputStream in = container.readBlob(blobName, start, size)) {
                int read = 0;
                while (read < size) {
                    final int n = in.read(span, read, size - read);
                    if (n < 0) {
                        throw new IOException("short read of " + blobName + " at " + start + ": wanted " + size + ", got " + read);
                    }
                    read += n;
                }
            }
            fetchedThrough = blockIndex + (size + blockSize - 1) / blockSize - 1;
            if (size <= blockSize) {
                // One block: the span is the block, and there is nothing to split or to put.
                return span;
            }
            // Everything the span carried beyond the block that was asked for goes into the cache, so the
            // reads that follow are hits rather than requests. Put, not fetch: these are not being waited
            // on, and a concurrent writer of the same key is storing identical bytes.
            for (int offset = blockSize; offset < size; offset += blockSize) {
                final int blockLength = Math.min(blockSize, size - offset);
                final byte[] ahead = new byte[blockLength];
                System.arraycopy(span, offset, ahead, 0, blockLength);
                cache.put(cacheKeyPrefix + "#" + (blockIndex + offset / blockSize), ahead);
            }
            final byte[] first = new byte[blockSize];
            System.arraycopy(span, 0, first, 0, blockSize);
            return first;
        });
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
        if (pos > length) {
            throw new IOException("seek past end of " + blobName + ": " + pos + " > " + length);
        }
        this.position = pos;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public void close() {
        // Nothing is held open: a block is either in the cache or it is not, and the container is shared.
    }
}
