/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * The bytes of a blob holding several files as one stream: a header, then each file's bytes back to back.
 *
 * <p>Mark and reset are supported against the files' own seeks, for the reason {@code SegmentPublisher}'s stream
 * gives: an object store client retries an upload by rewinding its stream, and a mark that expired part-way through
 * a large pack would turn a retryable failure into a corrupt upload.
 */
final class PackInputStream extends InputStream {

    /** One file of the pack: its open input and where its bytes start in the pack. */
    record Part(IndexInput input, long start, long length) {
    }

    private final byte[] header;
    private final List<Part> parts;
    private final long length;
    private long position;
    private long mark = -1L;

    /**
     * Creates the stream.
     *
     * @param header the bytes before the first file
     * @param parts the files, in blob order, the first starting where the header ends and each where the last ended
     */
    PackInputStream(byte[] header, List<Part> parts) {
        this.header = header;
        this.parts = parts;
        this.length = parts.isEmpty() ? header.length : parts.get(parts.size() - 1).start() + parts.get(parts.size() - 1).length();
    }

    /** The blob's length in bytes. */
    long length() {
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
        if (mark < 0) {
            throw new IOException("reset without a mark");
        }
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
        final Part part = partAt(position);
        final long within = position - part.start();
        final int n = (int) Math.min(len, part.length() - within);
        // Seek every time: cheap on a local file, and what makes a reset land wherever the mark was.
        part.input().seek(within);
        part.input().readBytes(b, off, n);
        position += n;
        return n;
    }

    private Part partAt(long at) {
        int low = 0;
        int high = parts.size() - 1;
        while (low < high) {
            final int middle = (low + high + 1) >>> 1;
            if (parts.get(middle).start() <= at) {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        return parts.get(low);
    }
}
