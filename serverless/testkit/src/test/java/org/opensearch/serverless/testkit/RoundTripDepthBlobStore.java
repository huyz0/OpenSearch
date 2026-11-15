/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.ConditionalBlobContainer;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Measures how many object-store round trips a caller waits through in sequence: the depth of the chain, not the number
 * of requests in it.
 *
 * <p>A request is one round deeper than the deepest request that had already finished when it started. Requests sent
 * together, before any of them answers, share a round however many there are; a request sent after reading another's
 * answer is the next round. Each request is held for a fixed delay so that requests sent together really overlap even
 * on a filesystem that answers in microseconds. A call that S3 serves as two requests in sequence -- a compare-and-swap
 * or a conditional delete, each a GET and then a conditional write, as {@code S3BlobContainer} does them -- is two rounds. The depth is then {@link #depth()}: the deepest round any request
 * reached since the last {@link #reset(long)}.
 *
 * <p>The measure is an upper bound: an unrelated request that starts after the measured path's last answer is counted
 * as one round deeper. So a measurement runs with nothing else talking to the store.
 */
public final class RoundTripDepthBlobStore implements BlobStore {

    private final BlobStore delegate;
    private volatile long delayNanos;
    private final AtomicInteger finished = new AtomicInteger();
    private final AtomicInteger deepest = new AtomicInteger();

    /**
     * Wraps a store, holding no request until a measurement starts.
     *
     * @param delegate the real store
     */
    public RoundTripDepthBlobStore(BlobStore delegate) {
        this.delegate = delegate;
    }

    /**
     * Starts a new measurement: the next request is round one.
     *
     * @param delayMillis how long each request is held from now on, long enough for requests sent together to overlap;
     *                    zero to measure nothing and run at full speed
     */
    public void reset(long delayMillis) {
        this.delayNanos = TimeUnit.MILLISECONDS.toNanos(delayMillis);
        finished.set(0);
        deepest.set(0);
    }

    /**
     * Returns the deepest round a request reached since the last reset.
     *
     * @return sequential round trips, zero if nothing was requested
     */
    public int depth() {
        return deepest.get();
    }

    private <T> T round(CheckedSupplier<T, IOException> request) throws IOException {
        return rounds(1, request);
    }

    /** A call that is several requests in sequence on S3, such as a compare-and-swap: a GET, then a conditional PUT. */
    private <T> T rounds(int sequential, CheckedSupplier<T, IOException> request) throws IOException {
        final int round = finished.get() + sequential;
        deepest.accumulateAndGet(round, Math::max);
        if (delayNanos > 0) {
            try {
                TimeUnit.NANOSECONDS.sleep(delayNanos * sequential);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting out the round trip", e);
            }
        }
        try {
            return request.get();
        } finally {
            finished.accumulateAndGet(round, Math::max);
        }
    }

    @Override
    public void validateKey(BlobPath path, String blobName) {
        delegate.validateKey(path, blobName);
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new Rounds(delegate.blobContainer(path));
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    private final class Rounds implements ConditionalBlobContainer {

        private final BlobContainer inner;

        Rounds(BlobContainer inner) {
            this.inner = inner;
        }

        @Override
        public BlobPath path() {
            return inner.path();
        }

        @Override
        public boolean blobExists(String blobName) throws IOException {
            return round(() -> inner.blobExists(blobName));
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            return round(() -> inner.readBlob(blobName));
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            return round(() -> inner.readBlob(blobName, position, length));
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            round(() -> {
                inner.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
                return null;
            });
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            round(() -> {
                inner.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
                return null;
            });
        }

        @Override
        public DeleteResult delete() throws IOException {
            return round(inner::delete);
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
            round(() -> {
                inner.deleteBlobsIgnoringIfNotExists(blobNames);
                return null;
            });
        }

        @Override
        public Map<String, BlobMetadata> listBlobs() throws IOException {
            return round(inner::listBlobs);
        }

        @Override
        public Map<String, BlobContainer> children() throws IOException {
            return wrap(round(inner::children));
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
            return round(() -> inner.listBlobsByPrefix(blobNamePrefix));
        }

        @Override
        public List<BlobMetadata> listBlobsByPrefix(String blobNamePrefix, String startAfter, int limit) throws IOException {
            return round(() -> inner.listBlobsByPrefix(blobNamePrefix, startAfter, limit));
        }

        @Override
        public Map<String, BlobContainer> children(String startAfter, int limit) throws IOException {
            return wrap(round(() -> inner.children(startAfter, limit)));
        }

        @Override
        public List<BlobMetadata> listBlobsByPrefixInSortedOrder(String blobNamePrefix, int limit, BlobNameSortOrder order)
            throws IOException {
            return round(() -> inner.listBlobsByPrefixInSortedOrder(blobNamePrefix, limit, order));
        }

        @Override
        public Optional<BlobRegister> readRegister(String blobName) throws IOException {
            return round(() -> ConditionalBlobContainer.of(inner).readRegister(blobName));
        }

        @Override
        public boolean deleteRegisterIfUnchanged(String blobName, long expectedGeneration) throws IOException {
            return rounds(2, () -> ConditionalBlobContainer.of(inner).deleteRegisterIfUnchanged(blobName, expectedGeneration));
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
            throws IOException {
            return rounds(2, () -> ConditionalBlobContainer.of(inner).compareAndSwapRegister(blobName, expectedGeneration, newValue));
        }

        @Override
        public BlobRegisterCasResult createRegisterIfAbsent(String blobName, BytesReference value) throws IOException {
            return round(() -> ConditionalBlobContainer.of(inner).createRegisterIfAbsent(blobName, value));
        }

        private Map<String, BlobContainer> wrap(Map<String, BlobContainer> children) {
            final Map<String, BlobContainer> wrapped = new LinkedHashMap<>();
            for (Map.Entry<String, BlobContainer> child : children.entrySet()) {
                wrapped.put(child.getKey(), new Rounds(child.getValue()));
            }
            return wrapped;
        }
    }
}
