/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Counts object-store operations, so the cost of polling can be measured rather than argued.
 *
 * <p>{@code rfc-serverless-shell.md} §10.3 gates gossip on measurement: it is built only if polling
 * plus a Kubernetes informer turns out to be insufficient. That gate needs a number — how many requests
 * one reconciliation pass costs — and this is where it comes from.
 *
 * <p>Reads and writes are counted separately because they price differently on every real provider, and
 * a listing is counted as a read even though it usually costs more.
 */
public final class CountingBlobStore implements BlobStore {

    private final BlobStore delegate;
    private final AtomicLong reads = new AtomicLong();
    private final AtomicLong writes = new AtomicLong();

    /**
     * Wraps a store.
     *
     * @param delegate the real store
     */
    public CountingBlobStore(BlobStore delegate) {
        this.delegate = delegate;
    }

    /** Resets both counters. */
    public void reset() {
        reads.set(0);
        writes.set(0);
    }

    /**
     * Returns the read operations counted.
     *
     * @return reads and listings
     */
    public long reads() {
        return reads.get();
    }

    /**
     * Returns the write operations counted.
     *
     * @return writes and deletes
     */
    public long writes() {
        return writes.get();
    }

    /**
     * Returns the total operations counted.
     *
     * @return reads plus writes
     */
    public long total() {
        return reads.get() + writes.get();
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new Counting(delegate.blobContainer(path));
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    private final class Counting implements BlobContainer {

        private final BlobContainer inner;

        Counting(BlobContainer inner) {
            this.inner = inner;
        }

        @Override
        public BlobPath path() {
            return inner.path();
        }

        @Override
        public boolean blobExists(String blobName) throws IOException {
            reads.incrementAndGet();
            return inner.blobExists(blobName);
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            reads.incrementAndGet();
            return inner.readBlob(blobName);
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            reads.incrementAndGet();
            return inner.readBlob(blobName, position, length);
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            writes.incrementAndGet();
            inner.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            writes.incrementAndGet();
            inner.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public DeleteResult delete() throws IOException {
            writes.incrementAndGet();
            return inner.delete();
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
            writes.incrementAndGet();
            inner.deleteBlobsIgnoringIfNotExists(blobNames);
        }

        @Override
        public Map<String, BlobMetadata> listBlobs() throws IOException {
            reads.incrementAndGet();
            return inner.listBlobs();
        }

        @Override
        public Map<String, BlobContainer> children() throws IOException {
            reads.incrementAndGet();
            return inner.children().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new Counting(e.getValue())));
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
            reads.incrementAndGet();
            return inner.listBlobsByPrefix(blobNamePrefix);
        }

        @Override
        public Optional<BlobRegister> readRegister(String blobName) throws IOException {
            reads.incrementAndGet();
            return inner.readRegister(blobName);
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
            throws IOException {
            writes.incrementAndGet();
            return inner.compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }

        @Override
        public BlobRegisterCasResult createRegisterIfAbsent(String blobName, BytesReference value) throws IOException {
            writes.incrementAndGet();
            return inner.createRegisterIfAbsent(blobName, value);
        }
    }
}
