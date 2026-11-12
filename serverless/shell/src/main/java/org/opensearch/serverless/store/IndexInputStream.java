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

/**
 * Adapts a Lucene {@link IndexInput} to an {@link InputStream} for blob upload.
 *
 * <p><b>Mark and reset are supported, and have to be.</b> The S3 client refuses a stream it cannot
 * rewind — "No mark support on inputStream breaks the S3 SDK's ability to retry requests" — because a
 * request it cannot replay is a request it cannot retry, and an upload that fails once would then
 * fail permanently. A filesystem never asks, which is why this was invisible until the shell was
 * pointed at an object store: publishing a segment was impossible on S3 and worked perfectly on disk.
 *
 * <p>Implemented against the underlying seek rather than by wrapping in a {@code BufferedInputStream},
 * whose mark is bounded by a read-ahead limit. Segment files are routinely larger than any limit worth
 * buffering, and a mark that silently expires part-way through a large upload would turn a retryable
 * failure into a corrupt one.
 */
final class IndexInputStream extends InputStream {

    private final IndexInput input;
    private final long length;
    private final long origin;
    private long position;
    private long mark = -1L;

    IndexInputStream(IndexInput input, long length) {
        this.input = input;
        this.length = length;
        // Where this stream started, which is not necessarily where the IndexInput did.
        this.origin = input.getFilePointer();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readLimit) {
        // readLimit ignored on purpose: the source is seekable, so there is no read-ahead buffer to
        // outgrow and no honest reason to invalidate a mark.
        mark = position;
    }

    @Override
    public synchronized void reset() throws IOException {
        if (mark < 0) {
            throw new IOException("reset without a mark");
        }
        input.seek(origin + mark);
        position = mark;
    }

    @Override
    public int available() {
        return (int) Math.min(Integer.MAX_VALUE, length - position);
    }

    @Override
    public int read() throws IOException {
        if (position >= length) {
            return -1;
        }
        position++;
        return input.readByte() & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (position >= length) {
            return -1;
        }
        final int toRead = (int) Math.min(len, length - position);
        input.readBytes(b, off, toRead);
        position += toRead;
        return toRead;
    }
}
