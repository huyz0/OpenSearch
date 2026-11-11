/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * A node pack's bytes as one stream: the encoded prefix and header, then each block's bytes in turn.
 *
 * <p>Blocks are opened lazily, one at a time, so a pack of many shards' segments holds at most one open source. The
 * stream supports {@link #mark} and {@link #reset} anywhere, by reopening the block the mark falls in and skipping to
 * it, because an object store client retrying an upload rewinds its input.
 *
 * <p>A block that yields fewer or more bytes than its stated length fails the upload rather than shifting every later
 * block's offset, which the header has already recorded.
 */
public final class NodePackInputStream extends InputStream {

    /** Opens one block's bytes from the first. */
    @FunctionalInterface
    public interface Source {
        /**
         * Opens the bytes.
         *
         * @return a fresh stream from the block's first byte
         * @throws IOException if the bytes cannot be opened
         */
        InputStream open() throws IOException;
    }

    private final byte[] header;
    private final List<Source> sources;
    private final long[] starts;
    private final long length;

    private long position;
    private long mark = 0L;
    private int current = -1;
    private InputStream open;
    private long openPosition;

    /**
     * Creates the stream.
     *
     * @param header the prefix and header
     * @param sources each block's bytes
     * @param lengths each block's length, in the same order
     */
    public NodePackInputStream(byte[] header, List<Source> sources, List<Long> lengths) {
        if (sources.size() != lengths.size()) {
            throw new IllegalArgumentException("one length per block");
        }
        this.header = header;
        this.sources = List.copyOf(sources);
        this.starts = new long[sources.size() + 1];
        long at = header.length;
        for (int i = 0; i < lengths.size(); i++) {
            starts[i] = at;
            at += lengths.get(i);
        }
        starts[sources.size()] = at;
        this.length = at;
    }

    /**
     * The pack's whole length.
     *
     * @return its length in bytes
     */
    public long length() {
        return length;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readLimit) {
        mark = position;
    }

    @Override
    public synchronized void reset() throws IOException {
        closeOpen();
        position = mark;
    }

    @Override
    public int available() {
        return (int) Math.min(Integer.MAX_VALUE, length - position);
    }

    @Override
    public int read() throws IOException {
        final byte[] one = new byte[1];
        return read(one, 0, 1) < 0 ? -1 : one[0] & 0xFF;
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) throws IOException {
        if (position >= length) {
            return -1;
        }
        if (len == 0) {
            return 0;
        }
        if (position < header.length) {
            final int n = (int) Math.min(len, header.length - position);
            System.arraycopy(header, (int) position, b, off, n);
            position += n;
            return n;
        }
        final int block = blockAt(position);
        final InputStream in = streamAt(block, position);
        final long blockEnd = starts[block + 1];
        final int want = (int) Math.min(len, blockEnd - position);
        final int n = in.read(b, off, want);
        if (n < 0) {
            throw new EOFException("pack block " + block + " ended " + (blockEnd - position) + " bytes short of its stated length");
        }
        position += n;
        openPosition += n;
        if (position == blockEnd) {
            if (in.read() >= 0) {
                throw new IOException("pack block " + block + " is longer than its stated length");
            }
            closeOpen();
        }
        return n;
    }

    @Override
    public synchronized long skip(long n) throws IOException {
        final long skipped = Math.max(0L, Math.min(n, length - position));
        if (skipped > 0) {
            closeOpen();
            position += skipped;
        }
        return skipped;
    }

    @Override
    public synchronized void close() throws IOException {
        closeOpen();
    }

    private int blockAt(long at) {
        for (int i = 0; i < sources.size(); i++) {
            if (at < starts[i + 1]) {
                return i;
            }
        }
        throw new IllegalStateException("position " + at + " is past the pack");
    }

    private InputStream streamAt(int block, long at) throws IOException {
        if (current == block && open != null && openPosition == at) {
            return open;
        }
        closeOpen();
        open = sources.get(block).open();
        current = block;
        openPosition = starts[block];
        long toSkip = at - starts[block];
        while (toSkip > 0) {
            final long skipped = open.skip(toSkip);
            if (skipped <= 0) {
                if (open.read() < 0) {
                    throw new EOFException("pack block " + block + " ended while seeking into it");
                }
                toSkip--;
                openPosition++;
            } else {
                toSkip -= skipped;
                openPosition += skipped;
            }
        }
        return open;
    }

    private void closeOpen() throws IOException {
        final InputStream closing = open;
        open = null;
        current = -1;
        if (closing != null) {
            closing.close();
        }
    }
}
