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
    private final String blobName;
    private final String cacheKeyPrefix;
    private final long length;
    private long position;

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
        // Single-flighted: a cold block every concurrent query of this segment needs at once is fetched
        // by the first of them and handed to the rest, rather than fetched once per query.
        return cache.fetch(key, () -> {
            final int blockSize = cache.blockSize();
            final long start = blockIndex * blockSize;
            final int size = (int) Math.min(blockSize, length - start);
            final byte[] block = new byte[size];
            // The one range request. Everything else in this class exists so that this asks for as little
            // as the query actually needs.
            try (InputStream in = container.readBlob(blobName, start, size)) {
                int read = 0;
                while (read < size) {
                    final int n = in.read(block, read, size - read);
                    if (n < 0) {
                        throw new IOException("short read of " + blobName + " at " + start + ": wanted " + size + ", got " + read);
                    }
                    read += n;
                }
            }
            return block;
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
