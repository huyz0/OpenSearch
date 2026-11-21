/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.blobstore.BlobContainer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * Reads one segment file out of the object store, a block at a time.
 *
 * <p>Extends {@link BufferedIndexInput} so Lucene's own buffering and slicing come for free and only
 * the fetch is ours. A read that crosses a block boundary fetches each block it touches and no more —
 * the point being that a query reading a few kilobytes of postings does not pay for the whole segment.
 *
 * <p>A slice is a view of the same file, not a copy: its blocks are the file's blocks, keyed on the file's own
 * grid, so a compound file's sub-file read through a slice and read through the whole file share one cached copy,
 * and a hint given to a slice reaches the store rather than being dropped by a generic wrapper.
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

    /**
     * The most blocks one hint fetches. Lucene announces a term block, a postings start or a stored-fields chunk, each
     * a few kilobytes, so a hint is one block in practice; the bound keeps a mistaken long one from evicting the cache.
     */
    static final int MAX_HINT_BLOCKS = 16;

    private static final Logger logger = LogManager.getLogger(ObjectStoreIndexInput.class);

    private final String blobName;
    private final String cacheKeyPrefix;
    /** The length of this view: the whole file's, or a slice's. */
    private final long length;
    /** Where this view starts in the file: zero unless it is a slice. */
    private final long sliceOffset;
    /** The whole file's length, which bounds a fetch whatever view made it. */
    private final long fileLength;
    /** Where the file starts in its blob: zero for a file of its own, its offset in a pack. */
    private final long blobOffset;
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
        long length,
        long blobOffset
    ) {
        super(resourceDescription, context);
        this.container = container;
        this.cache = cache;
        this.blobName = blobName;
        this.cacheKeyPrefix = cacheKeyPrefix;
        this.length = length;
        this.sliceOffset = 0;
        this.fileLength = length;
        this.blobOffset = blobOffset;
    }

    private ObjectStoreIndexInput(String resourceDescription, ObjectStoreIndexInput file, long sliceOffset, long length) {
        super(resourceDescription, file.getBufferSize());
        this.container = file.container;
        this.cache = file.cache;
        this.blobName = file.blobName;
        this.cacheKeyPrefix = file.cacheKeyPrefix;
        this.length = length;
        this.sliceOffset = sliceOffset;
        this.fileLength = file.fileLength;
        this.blobOffset = file.blobOffset;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) {
        if (offset < 0 || length < 0 || offset + length > this.length) {
            throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: " + this);
        }
        return new ObjectStoreIndexInput(getFullSliceDescription(sliceDescription), this, sliceOffset + offset, length);
    }

    /**
     * Fetches the announced range ahead, on the cache's prefetch executor, so the read that follows finds it held or
     * in flight. Does nothing when hints are off, for blocks already held, and for a range past the end: a hint never
     * fails and never blocks the reader that gave it.
     */
    @Override
    public void prefetch(long offset, long length) {
        final Executor executor = cache.readPrefetchExecutor();
        if (executor == null || length <= 0 || offset < 0 || offset >= this.length) {
            return;
        }
        final int blockSize = cache.blockSize();
        long first = (sliceOffset + offset) / blockSize;
        long last = Math.min((sliceOffset + Math.min(this.length, offset + length) - 1) / blockSize, first + MAX_HINT_BLOCKS - 1);
        while (first <= last && cache.holds(key(first))) {
            first++;
        }
        while (last > first && cache.holds(key(last))) {
            last--;
        }
        if (first > last) {
            return;
        }
        final long from = first;
        final int blocks = (int) (last - first + 1);
        try {
            executor.execute(() -> {
                try {
                    cache.fetchAhead(key(from), () -> fetchSpan(from, (long) blocks * blockSize));
                } catch (IOException | RuntimeException e) {
                    // The read the hint was for fetches for itself, and reports the failure if it is real.
                    logger.debug("Object-store read-ahead failed (block={}, error_type={})", from, e.getClass().getSimpleName());
                }
            });
        } catch (RejectedExecutionException e) {
            // A busy pool drops the hint; the read pays its own round trip, as it did before hints.
        }
    }

    private String key(long blockIndex) {
        return cacheKeyPrefix + "#" + blockIndex;
    }

    @Override
    protected void readInternal(ByteBuffer b) throws IOException {
        final int wanted = b.remaining();
        if (position + wanted > length) {
            throw new IOException("read past end of " + blobName + ": " + (position + wanted) + " > " + length);
        }
        long remainingOffset = sliceOffset + position;
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
        final String key = key(blockIndex);
        final byte[] resident = cache.get(key);
        if (resident != null) {
            // A hit says nothing about the access pattern -- under read-ahead most of a sequential scan is
            // hits, by construction -- so the detector below is left alone.
            return resident;
        }
        // Single-flighted: a cold block every concurrent query of this segment needs at once is fetched
        // by the first of them and handed to the rest, rather than fetched once per query.
        return cache.fetch(key, () -> {
            // Sequential if this miss is for the block right after the last fetch ended. A scan therefore
            // ramps -- 1 block, 2, 4, 8 -- and a seek drops straight back to one, so random access costs
            // exactly what it did before read-ahead existed.
            readahead = blockIndex == fetchedThrough + 1 ? Math.min(readahead * 2, MAX_READAHEAD_BLOCKS) : 1;
            final int blockSize = cache.blockSize();
            final long wanted = Math.min((long) readahead * blockSize, fileLength - blockIndex * blockSize);
            final byte[] block = fetchSpan(blockIndex, wanted);
            fetchedThrough = blockIndex + (wanted + blockSize - 1) / blockSize - 1;
            return block;
        });
    }

    /**
     * Fetches up to {@code wanted} bytes of the file from the start of a block in one range request, puts every block
     * after the first into the cache, and returns the first.
     */
    private byte[] fetchSpan(long blockIndex, long wanted) throws IOException {
        final int blockSize = cache.blockSize();
        final long start = blockIndex * blockSize;
        final int size = (int) Math.min(wanted, fileLength - start);
        final byte[] span = new byte[size];
        // The one range request, and the whole point of the ramp: a cold scan of a large segment used
        // to pay one of these per 64 KB, which on an object store is one billed round trip per 64 KB.
        try (InputStream in = container.readBlob(blobName, blobOffset + start, size)) {
            int read = 0;
            while (read < size) {
                final int n = in.read(span, read, size - read);
                if (n < 0) {
                    throw new IOException("short read of " + blobName + " at " + start + ": wanted " + size + ", got " + read);
                }
                read += n;
            }
        }
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
            cache.put(key(blockIndex + offset / blockSize), ahead);
        }
        final byte[] first = new byte[blockSize];
        System.arraycopy(span, 0, first, 0, blockSize);
        return first;
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
