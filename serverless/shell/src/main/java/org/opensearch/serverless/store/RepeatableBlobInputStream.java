/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.blobstore.BlobContainer;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * Reads an immutable blob with repeatable marks. Upload retries reopen a range at the saved
 * offset instead of retaining the copied file in memory. The caller owns and closes this stream.
 */
public final class RepeatableBlobInputStream extends InputStream {
    private final BlobContainer container;
    private final String name;
    private final long length;
    private InputStream input;
    private long position;
    private long mark;
    private boolean closed;

    /**
     * Opens one immutable blob whose length is known from its captured manifest or inventory.
     *
     * @param container its source container
     * @param name its blob name
     * @param length its complete length
     * @throws IOException if the initial read cannot be opened
     */
    public RepeatableBlobInputStream(BlobContainer container, String name, long length) throws IOException {
        if (length < 0) {
            throw new IllegalArgumentException("negative blob length");
        }
        this.container = Objects.requireNonNull(container);
        this.name = Objects.requireNonNull(name);
        this.length = length;
        this.input = container.readBlob(name);
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("blob stream is closed");
        }
    }

    private InputStream readable() throws IOException {
        ensureOpen();
        if (input == null) {
            throw new IOException("blob stream must be reset after its range could not be reopened");
        }
        return input;
    }

    @Override
    public int read() throws IOException {
        final InputStream current = readable();
        if (position == length) {
            return -1;
        }
        final int value = current.read();
        if (value < 0) {
            throw new EOFException("captured blob ended before its recorded length");
        }
        position++;
        return value;
    }

    @Override
    public int read(byte[] bytes, int offset, int count) throws IOException {
        Objects.checkFromIndexSize(offset, count, bytes.length);
        final InputStream current = readable();
        if (count == 0) {
            return 0;
        }
        if (position == length) {
            return -1;
        }
        final int read = current.read(bytes, offset, (int) Math.min((long) count, length - position));
        if (read < 0) {
            throw new EOFException("captured blob ended before its recorded length");
        }
        position += read;
        return read;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readLimit) {
        // No retained buffer, so the mark remains valid beyond the caller's read limit.
        mark = position;
    }

    @Override
    public void reset() throws IOException {
        ensureOpen();
        seek(mark);
    }

    private void seek(long offset) throws IOException {
        if (offset == position && input != null) {
            return;
        }
        final InputStream previous = input;
        input = null;
        if (previous != null) {
            previous.close();
        }
        input = offset == length ? InputStream.nullInputStream() : container.readBlob(name, offset, length - offset);
        position = offset;
    }

    @Override
    public long skip(long count) throws IOException {
        ensureOpen();
        if (count <= 0) {
            return 0;
        }
        final long skipped = Math.min(count, length - position);
        seek(position + skipped);
        return skipped;
    }

    @Override
    public int available() throws IOException {
        return (int) Math.min((long) readable().available(), length - position);
    }

    @Override
    public void close() throws IOException {
        if (closed == false) {
            closed = true;
            if (input != null) {
                input.close();
                input = null;
            }
        }
    }
}
