/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.store.RepeatableBlobInputStream;
import org.opensearch.test.OpenSearchTestCase;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** Upload retries must reread the same bytes without keeping the copied blob in memory. */
public class RepeatableBlobInputStreamTests extends OpenSearchTestCase {
    public void testDefaultResetReopensTheBeginning() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var source = new TrackingContainer(store.blobContainer(BlobPath.cleanPath()), 64L);
            try (var input = new RepeatableBlobInputStream(source, "file", 64L)) {
                assertTrue(input.markSupported());
                final byte[] first = input.readAllBytes();
                assertEquals(64, first.length);
                input.reset();
                assertArrayEquals(first, input.readAllBytes());
                assertEquals(0L, source.ranges.get(0)[0]);
                assertEquals(64L, source.ranges.get(0)[1]);
            }
            assertEquals(source.opened, source.closed);
        }
    }

    public void testMarkSurvivesItsReadLimitAndResetsAtTheSavedOffset() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var source = new TrackingContainer(store.blobContainer(BlobPath.cleanPath()), 64L);
            try (var input = new RepeatableBlobInputStream(source, "file", 64L)) {
                input.readNBytes(5);
                input.mark(1);
                final byte[] first = input.readNBytes(40);
                input.reset();
                assertEquals(5L, source.ranges.get(0)[0]);
                assertEquals(59L, source.ranges.get(0)[1]);
                assertArrayEquals(first, input.readNBytes(40));
            }
            assertEquals(source.opened, source.closed);
        }
    }

    public void testUnusedResetCostsNoAdditionalObjectRead() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var source = new TrackingContainer(store.blobContainer(BlobPath.cleanPath()), 64L);
            try (var input = new RepeatableBlobInputStream(source, "file", 64L)) {
                input.mark(1);
                input.reset();
                input.reset();
                assertTrue(source.ranges.isEmpty());
                assertEquals(0, input.read());
            }
            assertEquals(1, source.opened);
            assertEquals(1, source.closed);
        }
    }

    public void testRetryCanRecoverFromAFailedRangeReopen() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var source = new TrackingContainer(store.blobContainer(BlobPath.cleanPath()), 64L);
            try (var input = new RepeatableBlobInputStream(source, "file", 64L)) {
                input.readNBytes(10);
                source.failRange.set(true);
                expectThrows(IOException.class, input::reset);
                input.reset();
                assertEquals(0, input.read());
            }
            assertEquals(source.opened, source.closed);
        }
    }

    public void testEarlyEndOfCapturedBlobIsAnError() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var source = new TrackingContainer(store.blobContainer(BlobPath.cleanPath()), 8L);
            try (var input = new RepeatableBlobInputStream(source, "file", 64L)) {
                expectThrows(EOFException.class, input::readAllBytes);
            }
            assertEquals(source.opened, source.closed);
        }
    }

    public void testLargeOffsetsAndSkipDoNotOverflowOrFetchSkippedBytes() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final long length = 1L << 35;
            final long offset = (1L << 34) + 17L;
            final var source = new TrackingContainer(store.blobContainer(BlobPath.cleanPath()), length);
            try (var input = new RepeatableBlobInputStream(source, "file", length)) {
                assertEquals(offset, input.skip(offset));
                assertEquals(offset, source.ranges.get(0)[0]);
                assertEquals(length - offset, source.ranges.get(0)[1]);
                input.mark(1);
                assertEquals(17, input.read());
                input.reset();
                assertEquals(17, input.read());
                assertEquals(length - offset - 1, input.skip(Long.MAX_VALUE));
                assertEquals(-1, input.read());
            }
            assertEquals(source.opened, source.closed);
        }
    }

    public void testEmptyBlobAndClosedStream() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var source = new TrackingContainer(store.blobContainer(BlobPath.cleanPath()), 0L);
            final var input = new RepeatableBlobInputStream(source, "empty", 0L);
            assertEquals(-1, input.read());
            assertEquals(0, input.read(new byte[0]));
            input.reset();
            assertTrue(source.ranges.isEmpty());
            input.close();
            input.close();
            expectThrows(IOException.class, input::reset);
            expectThrows(IOException.class, input::read);
            assertEquals(1, source.closed);
        }
    }

    private static final class TrackingContainer extends DelegatingBlobContainer {
        private final long length;
        private final List<long[]> ranges = new ArrayList<>();
        private final AtomicBoolean failRange = new AtomicBoolean();
        private int opened;
        private int closed;

        TrackingContainer(BlobContainer delegate, long length) {
            super(delegate);
            this.length = length;
        }

        private InputStream open(long offset, long count) {
            opened++;
            final var once = new AtomicBoolean();
            return new FilterInputStream(InputStream.nullInputStream()) {
                private long position = offset;
                private long remaining = Math.min(count, Math.max(0L, length - offset));

                @Override
                public int read() {
                    if (remaining == 0) {
                        return -1;
                    }
                    remaining--;
                    return (int) (position++ & 255);
                }

                @Override
                public int read(byte[] bytes, int offset, int count) {
                    if (count == 0) {
                        return 0;
                    }
                    if (remaining == 0) {
                        return -1;
                    }
                    final int size = (int) Math.min((long) count, remaining);
                    for (int i = 0; i < size; i++) {
                        bytes[offset + i] = (byte) position++;
                    }
                    remaining -= size;
                    return size;
                }

                @Override
                public boolean markSupported() {
                    return false;
                }

                @Override
                public void close() {
                    if (once.compareAndSet(false, true)) {
                        closed++;
                    }
                }
            };
        }

        @Override
        public InputStream readBlob(String name) {
            return open(0L, length);
        }

        @Override
        public InputStream readBlob(String name, long position, long length) throws IOException {
            if (failRange.compareAndSet(true, false)) {
                throw new IOException("injected range read failure");
            }
            ranges.add(new long[] { position, length });
            return open(position, length);
        }
    }
}
