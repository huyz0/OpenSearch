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
 *
 * <p><b>Logical operations are not billable requests, and the gap is not small.</b> A
 * {@code compareAndSwapRegister} is one call here and <em>two</em> HTTP requests on S3 — a GET for the
 * current value and ETag, then a conditional PUT — as {@code S3BlobContainer} shows. Counting it once
 * would understate the cost of exactly the operation this design leans on hardest. So the per-kind
 * counters below exist alongside the totals, and {@link #impliedS3Requests()} applies what each kind
 * actually costs. That number is derived from reading the S3 container, not from watching the wire: it
 * is a better estimate than the logical count and still an estimate.
 */
public final class CountingBlobStore implements BlobStore {

    private final BlobStore delegate;
    private final AtomicLong reads = new AtomicLong();
    private final AtomicLong writes = new AtomicLong();
    private final AtomicLong registerReads = new AtomicLong();
    private final AtomicLong registerWrites = new AtomicLong();
    private final AtomicLong blobReads = new AtomicLong();
    private final AtomicLong blobWrites = new AtomicLong();
    private final AtomicLong listings = new AtomicLong();
    private final AtomicLong deletes = new AtomicLong();

    /**
     * Wraps a store.
     *
     * @param delegate the real store
     */
    public CountingBlobStore(BlobStore delegate) {
        this.delegate = delegate;
    }

    /** Resets every counter. */
    public void reset() {
        reads.set(0);
        writes.set(0);
        registerReads.set(0);
        registerWrites.set(0);
        blobReads.set(0);
        blobWrites.set(0);
        listings.set(0);
        deletes.set(0);
    }

    /**
     * Returns register reads.
     *
     * @return the count
     */
    public long registerReads() {
        return registerReads.get();
    }

    /**
     * Returns register compare-and-swaps, including put-if-absent.
     *
     * @return the count
     */
    public long registerWrites() {
        return registerWrites.get();
    }

    /**
     * Returns ordinary blob reads, ranged or whole.
     *
     * @return the count
     */
    public long blobReads() {
        return blobReads.get();
    }

    /**
     * Returns ordinary blob writes.
     *
     * @return the count
     */
    public long blobWrites() {
        return blobWrites.get();
    }

    /**
     * Returns listings, which price higher than a GET on every provider worth naming.
     *
     * @return the count
     */
    public long listings() {
        return listings.get();
    }

    /**
     * Returns delete calls. One call may remove many blobs.
     *
     * @return the count
     */
    public long deletes() {
        return deletes.get();
    }

    /**
     * Returns what these operations would actually cost in S3 requests.
     *
     * <p>Every kind is one request except a compare-and-swap, which is a GET followed by a conditional
     * PUT. Derived by reading {@code S3BlobContainer} rather than by observing the wire.
     *
     * @return the implied request count
     */
    public long impliedS3Requests() {
        return registerReads.get() + 2 * registerWrites.get() + blobReads.get() + blobWrites.get() + listings.get() + deletes.get();
    }

    /**
     * Returns a one-line breakdown, for logging a measurement rather than a bare total.
     *
     * @return the breakdown
     */
    public String breakdown() {
        return "registerReads="
            + registerReads.get()
            + " registerCas="
            + registerWrites.get()
            + " blobReads="
            + blobReads.get()
            + " blobWrites="
            + blobWrites.get()
            + " listings="
            + listings.get()
            + " deletes="
            + deletes.get()
            + " (logical="
            + total()
            + ", impliedS3Requests="
            + impliedS3Requests()
            + ")";
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
            blobReads.incrementAndGet();
            return inner.readBlob(blobName);
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            reads.incrementAndGet();
            blobReads.incrementAndGet();
            return inner.readBlob(blobName, position, length);
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            writes.incrementAndGet();
            blobWrites.incrementAndGet();
            inner.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            writes.incrementAndGet();
            blobWrites.incrementAndGet();
            inner.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public DeleteResult delete() throws IOException {
            writes.incrementAndGet();
            deletes.incrementAndGet();
            return inner.delete();
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
            writes.incrementAndGet();
            deletes.incrementAndGet();
            inner.deleteBlobsIgnoringIfNotExists(blobNames);
        }

        @Override
        public Map<String, BlobMetadata> listBlobs() throws IOException {
            reads.incrementAndGet();
            listings.incrementAndGet();
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
            listings.incrementAndGet();
            return inner.listBlobsByPrefix(blobNamePrefix);
        }

        @Override
        public Optional<BlobRegister> readRegister(String blobName) throws IOException {
            reads.incrementAndGet();
            registerReads.incrementAndGet();
            return inner.readRegister(blobName);
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
            throws IOException {
            writes.incrementAndGet();
            registerWrites.incrementAndGet();
            return inner.compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }

        @Override
        public BlobRegisterCasResult createRegisterIfAbsent(String blobName, BytesReference value) throws IOException {
            writes.incrementAndGet();
            registerWrites.incrementAndGet();
            return inner.createRegisterIfAbsent(blobName, value);
        }
    }
}
