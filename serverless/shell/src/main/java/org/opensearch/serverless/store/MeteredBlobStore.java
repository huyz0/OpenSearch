/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.ConditionalBlobContainer;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.core.common.bytes.BytesReference;

public final class MeteredBlobStore implements BlobStore {

    private final BlobStore delegate;
    private final java.util.concurrent.atomic.AtomicLong registerReads = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong registerWrites = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong blobReads = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong blobWrites = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong listings = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deletes = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong bytesRead = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong bytesWritten = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong errors = new java.util.concurrent.atomic.AtomicLong();
    private final StoreUsage usage;

    /**
     * Wraps a store.
     *
     * @param delegate the real store
     */
    public MeteredBlobStore(BlobStore delegate) {
        this(delegate, new StoreUsage(StoreUsage.DEFAULT_MAX_INDICES));
    }

    /**
     * Wraps a store, keeping per-index usage in the given counters.
     *
     * @param delegate the real store
     * @param usage where each request is charged to the index it was made for; null counts only node totals
     */
    public MeteredBlobStore(BlobStore delegate, StoreUsage usage) {
        this.delegate = delegate;
        this.usage = usage;
    }

    /**
     * Returns what each index's requests cost, if per-index metering is on.
     *
     * @return the usage, or null when metering is off
     */
    public StoreUsage usage() {
        return usage;
    }

    /**
     * Returns a container whose requests are charged to the shard a path names, whatever the container's own path
     * says: for a shard reading a file that lives in a node pack, outside its own path. A container from any other
     * store is returned as it is.
     *
     * @param container a container from a metered store
     * @param shardBase the shard's base path, {@code <base>/segments/<shard>}
     * @return the charged container
     */
    public static BlobContainer chargedTo(BlobContainer container, BlobPath shardBase) {
        final String index = StoreUsage.indexOfContainer(shardBase);
        if (container instanceof Counting counting && index != null && index.equals(counting.owner) == false) {
            return counting.store().new Counting(counting.inner, index);
        }
        return container;
    }

    /**
     * Returns the store this counts for.
     *
     * @return the real store
     */
    public BlobStore delegate() {
        return delegate;
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
     * Returns ordinary blob reads, ranged or whole, and existence checks.
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
     * Returns listings, by prefix or of children.
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
     * Returns the bytes actually read from blobs, as the streams were consumed.
     *
     * @return the byte count
     */
    public long bytesRead() {
        return bytesRead.get();
    }

    /**
     * Returns the bytes handed to blob writes.
     *
     * @return the byte count
     */
    public long bytesWritten() {
        return bytesWritten.get();
    }

    /**
     * Returns how many calls ended in an exception, of any kind.
     *
     * @return the count
     */
    public long errors() {
        return errors.get();
    }

    /**
     * Returns what these operations would cost in S3 requests: every kind is one, a compare-and-swap
     * is two.
     *
     * @return the implied request count
     */
    public long impliedS3Requests() {
        return registerReads.get() + 2 * registerWrites.get() + blobReads.get() + blobWrites.get() + listings.get() + deletes.get();
    }

    @Override
    public void validateKey(BlobPath path, String blobName) {
        delegate.validateKey(path, blobName);
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new Counting(delegate.blobContainer(path));
    }

    @Override
    public void close() throws java.io.IOException {
        delegate.close();
    }

    private final class Counting implements ConditionalBlobContainer {

        private final BlobContainer inner;
        // The index the whole container belongs to, if any; else the naming that tells a blob's index from its name.
        private final String owner;
        private final StoreUsage.BlobNaming naming;

        Counting(BlobContainer inner) {
            this.inner = inner;
            this.owner = usage == null ? null : StoreUsage.indexOfContainer(inner.path());
            this.naming = usage == null || owner != null ? StoreUsage.BlobNaming.NONE : StoreUsage.BlobNaming.of(inner.path());
        }

        Counting(BlobContainer inner, String owner) {
            this.inner = inner;
            this.owner = owner;
            this.naming = StoreUsage.BlobNaming.NONE;
        }

        MeteredBlobStore store() {
            return MeteredBlobStore.this;
        }

        private String ownerOf(String blobName) {
            return owner != null ? owner : StoreUsage.indexOfBlob(naming, blobName);
        }

        private void written(String blobName, long blobSize) {
            final long size = Math.max(0L, blobSize);
            bytesWritten.addAndGet(size);
            if (usage != null) {
                usage.charge(ownerOf(blobName), IndexUsage.Kind.BYTES_WRITTEN, size);
            }
        }

        private CountedStream counted(String blobName, java.io.InputStream in) {
            return new CountedStream(in, usage == null ? null : usage.payer(ownerOf(blobName)));
        }

        private <T> T count(
            java.util.concurrent.atomic.AtomicLong counter,
            IndexUsage.Kind kind,
            String blobName,
            org.opensearch.common.CheckedSupplier<T, java.io.IOException> call
        ) throws java.io.IOException {
            counter.incrementAndGet();
            if (usage != null) {
                usage.charge(ownerOf(blobName), kind, IndexUsage.ONE_REQUEST);
            }
            final java.util.concurrent.atomic.AtomicLong attributed = ObjectStores.ATTRIBUTION.get();
            if (attributed != null) {
                attributed.incrementAndGet();
            }
            try {
                return call.get();
            } catch (java.io.IOException | RuntimeException e) {
                errors.incrementAndGet();
                throw e;
            }
        }

        @Override
        public BlobPath path() {
            return inner.path();
        }

        @Override
        public boolean blobExists(String blobName) throws java.io.IOException {
            return count(blobReads, IndexUsage.Kind.BLOB_READ, blobName, () -> inner.blobExists(blobName));
        }

        @Override
        public java.io.InputStream readBlob(String blobName) throws java.io.IOException {
            return counted(blobName, count(blobReads, IndexUsage.Kind.BLOB_READ, blobName, () -> inner.readBlob(blobName)));
        }

        @Override
        public java.io.InputStream readBlob(String blobName, long position, long length) throws java.io.IOException {
            return counted(
                blobName,
                count(blobReads, IndexUsage.Kind.BLOB_READ, blobName, () -> inner.readBlob(blobName, position, length))
            );
        }

        @Override
        public void writeBlob(String blobName, java.io.InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws java.io.IOException {
            written(blobName, blobSize);
            count(blobWrites, IndexUsage.Kind.BLOB_WRITE, blobName, () -> {
                inner.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
                return null;
            });
        }

        @Override
        public void writeBlobAtomic(String blobName, java.io.InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws java.io.IOException {
            written(blobName, blobSize);
            count(blobWrites, IndexUsage.Kind.BLOB_WRITE, blobName, () -> {
                inner.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
                return null;
            });
        }

        @Override
        public DeleteResult delete() throws java.io.IOException {
            return count(deletes, IndexUsage.Kind.DELETE, null, inner::delete);
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(java.util.List<String> blobNames) throws java.io.IOException {
            count(deletes, IndexUsage.Kind.DELETE, blobNames.size() == 1 ? blobNames.get(0) : null, () -> {
                inner.deleteBlobsIgnoringIfNotExists(blobNames);
                return null;
            });
        }

        @Override
        public java.util.Map<String, BlobMetadata> listBlobs() throws java.io.IOException {
            return count(listings, IndexUsage.Kind.LISTING, null, inner::listBlobs);
        }

        @Override
        public java.util.Map<String, BlobContainer> children() throws java.io.IOException {
            // A listing with a delimiter on every provider worth naming, and priced as one.
            final java.util.Map<String, BlobContainer> children = count(listings, IndexUsage.Kind.LISTING, null, inner::children);
            final java.util.Map<String, BlobContainer> counted = new java.util.LinkedHashMap<>();
            for (java.util.Map.Entry<String, BlobContainer> child : children.entrySet()) {
                counted.put(child.getKey(), new Counting(child.getValue()));
            }
            return counted;
        }

        @Override
        public java.util.Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws java.io.IOException {
            return count(listings, IndexUsage.Kind.LISTING, null, () -> inner.listBlobsByPrefix(blobNamePrefix));
        }

        // Everything below is delegated rather than left to the interface default: a default here would
        // silently replace the backend's bounded or conditional form with the unbounded or unsupported one.

        @Override
        public java.util.List<BlobMetadata> listBlobsByPrefixInSortedOrder(
            String blobNamePrefix,
            int limit,
            BlobNameSortOrder blobNameSortOrder
        ) throws java.io.IOException {
            return count(
                listings,
                IndexUsage.Kind.LISTING,
                null,
                () -> inner.listBlobsByPrefixInSortedOrder(blobNamePrefix, limit, blobNameSortOrder)
            );
        }

        @Override
        public java.util.List<BlobMetadata> listBlobsByPrefix(String blobNamePrefix, String startAfter, int limit)
            throws java.io.IOException {
            return count(listings, IndexUsage.Kind.LISTING, null, () -> inner.listBlobsByPrefix(blobNamePrefix, startAfter, limit));
        }

        @Override
        public java.util.Map<String, BlobContainer> children(String startAfter, int limit) throws java.io.IOException {
            final java.util.Map<String, BlobContainer> children = count(
                listings,
                IndexUsage.Kind.LISTING,
                null,
                () -> inner.children(startAfter, limit)
            );
            final java.util.Map<String, BlobContainer> counted = new java.util.LinkedHashMap<>();
            for (java.util.Map.Entry<String, BlobContainer> child : children.entrySet()) {
                counted.put(child.getKey(), new Counting(child.getValue()));
            }
            return counted;
        }

        @Override
        public long readBlobPreferredLength() {
            return inner.readBlobPreferredLength();
        }

        @Override
        public boolean deleteRegisterIfUnchanged(String blobName, long expectedGeneration) throws java.io.IOException {
            return count(
                deletes,
                IndexUsage.Kind.DELETE,
                blobName,
                () -> ConditionalBlobContainer.of(inner).deleteRegisterIfUnchanged(blobName, expectedGeneration)
            );
        }

        @Override
        public org.opensearch.common.blobstore.InputStreamWithMetadata readBlobWithMetadata(String blobName) throws java.io.IOException {
            final var result = count(blobReads, IndexUsage.Kind.BLOB_READ, blobName, () -> inner.readBlobWithMetadata(blobName));
            return new org.opensearch.common.blobstore.InputStreamWithMetadata(
                result.getInputStream() == null ? null : counted(blobName, result.getInputStream()),
                result.getMetadata()
            );
        }

        @Override
        public void writeBlobWithMetadata(
            String blobName,
            java.io.InputStream inputStream,
            long blobSize,
            boolean failIfAlreadyExists,
            java.util.Map<String, String> metadata
        ) throws java.io.IOException {
            written(blobName, blobSize);
            count(blobWrites, IndexUsage.Kind.BLOB_WRITE, blobName, () -> {
                inner.writeBlobWithMetadata(blobName, inputStream, blobSize, failIfAlreadyExists, metadata);
                return null;
            });
        }

        @Override
        public void writeBlobWithMetadata(
            String blobName,
            java.io.InputStream inputStream,
            long blobSize,
            boolean failIfAlreadyExists,
            java.util.Map<String, String> metadata,
            org.opensearch.cluster.metadata.CryptoMetadata cryptoMetadata
        ) throws java.io.IOException {
            written(blobName, blobSize);
            count(blobWrites, IndexUsage.Kind.BLOB_WRITE, blobName, () -> {
                inner.writeBlobWithMetadata(blobName, inputStream, blobSize, failIfAlreadyExists, metadata, cryptoMetadata);
                return null;
            });
        }

        @Override
        public void writeBlobAtomicWithMetadata(
            String blobName,
            java.io.InputStream inputStream,
            java.util.Map<String, String> metadata,
            long blobSize,
            boolean failIfAlreadyExists
        ) throws java.io.IOException {
            written(blobName, blobSize);
            count(blobWrites, IndexUsage.Kind.BLOB_WRITE, blobName, () -> {
                inner.writeBlobAtomicWithMetadata(blobName, inputStream, metadata, blobSize, failIfAlreadyExists);
                return null;
            });
        }

        @Override
        public boolean isConditionalWriteSupported() {
            return inner.isConditionalWriteSupported();
        }

        @Override
        public org.opensearch.common.blobstore.VersionedBlob readBlobWithVersion(String blobName) throws java.io.IOException {
            final var result = count(blobReads, IndexUsage.Kind.BLOB_READ, blobName, () -> inner.readBlobWithVersion(blobName));
            bytesRead.addAndGet(result.content().length);
            if (usage != null) {
                usage.charge(ownerOf(blobName), IndexUsage.Kind.BYTES_READ, result.content().length);
            }
            return result;
        }

        @Override
        public String writeBlobConditionally(String blobName, java.io.InputStream inputStream, long blobSize, String expectedVersionToken)
            throws java.io.IOException {
            written(blobName, blobSize);
            return count(
                blobWrites,
                IndexUsage.Kind.BLOB_WRITE,
                blobName,
                () -> inner.writeBlobConditionally(blobName, inputStream, blobSize, expectedVersionToken)
            );
        }

        @Override
        public void listBlobsByPrefixInSortedOrder(
            String blobNamePrefix,
            int limit,
            BlobNameSortOrder blobNameSortOrder,
            org.opensearch.core.action.ActionListener<java.util.List<BlobMetadata>> listener
        ) {
            listings.incrementAndGet();
            if (usage != null) {
                usage.charge(owner, IndexUsage.Kind.LISTING, IndexUsage.ONE_REQUEST);
            }
            final java.util.concurrent.atomic.AtomicLong attributed = ObjectStores.ATTRIBUTION.get();
            if (attributed != null) {
                attributed.incrementAndGet();
            }
            final var completed = new java.util.concurrent.atomic.AtomicBoolean();
            final org.opensearch.core.action.ActionListener<java.util.List<BlobMetadata>> counted =
                new org.opensearch.core.action.ActionListener<>() {
                    @Override
                    public void onResponse(java.util.List<BlobMetadata> result) {
                        completed.set(true);
                        listener.onResponse(result);
                    }

                    @Override
                    public void onFailure(Exception failure) {
                        if (completed.compareAndSet(false, true)) {
                            errors.incrementAndGet();
                        }
                        listener.onFailure(failure);
                    }
                };
            try {
                inner.listBlobsByPrefixInSortedOrder(blobNamePrefix, limit, blobNameSortOrder, counted);
            } catch (RuntimeException failure) {
                if (completed.compareAndSet(false, true)) {
                    errors.incrementAndGet();
                }
                throw failure;
            }
        }

        @Override
        public java.util.Optional<BlobRegister> readRegister(String blobName) throws java.io.IOException {
            return count(
                registerReads,
                IndexUsage.Kind.REGISTER_READ,
                blobName,
                () -> ConditionalBlobContainer.of(inner).readRegister(blobName)
            );
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
            throws java.io.IOException {
            return count(
                registerWrites,
                IndexUsage.Kind.REGISTER_WRITE,
                blobName,
                () -> ConditionalBlobContainer.of(inner).compareAndSwapRegister(blobName, expectedGeneration, newValue)
            );
        }

        @Override
        public BlobRegisterCasResult createRegisterIfAbsent(String blobName, BytesReference value) throws java.io.IOException {
            return count(
                registerWrites,
                IndexUsage.Kind.REGISTER_WRITE,
                blobName,
                () -> ConditionalBlobContainer.of(inner).createRegisterIfAbsent(blobName, value)
            );
        }
    }

    /** Counts the bytes a caller actually pulls through a read, which for a ranged read is the range. */
    private final class CountedStream extends java.io.FilterInputStream {

        // Fixed when the stream opened: bytes are pulled later, maybe on another thread or outside the caller's scope.
        private final StoreUsage.Scope payer;

        CountedStream(java.io.InputStream in, StoreUsage.Scope payer) {
            super(in);
            this.payer = payer;
        }

        private void pulled(long n) {
            bytesRead.addAndGet(n);
            if (payer != null) {
                payer.charge(usage, IndexUsage.Kind.BYTES_READ, n);
            }
        }

        @Override
        public int read() throws java.io.IOException {
            try {
                final int b = super.read();
                if (b >= 0) {
                    pulled(1L);
                }
                return b;
            } catch (java.io.IOException | RuntimeException failure) {
                errors.incrementAndGet();
                throw failure;
            }
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws java.io.IOException {
            try {
                final int n = super.read(buffer, offset, length);
                if (n > 0) {
                    pulled(n);
                }
                return n;
            } catch (java.io.IOException | RuntimeException failure) {
                errors.incrementAndGet();
                throw failure;
            }
        }

        @Override
        public long skip(long n) throws java.io.IOException {
            try {
                final long skipped = super.skip(n);
                if (skipped > 0) {
                    pulled(skipped);
                }
                return skipped;
            } catch (java.io.IOException | RuntimeException failure) {
                errors.incrementAndGet();
                throw failure;
            }
        }

        @Override
        public void close() throws java.io.IOException {
            try {
                super.close();
            } catch (java.io.IOException | RuntimeException failure) {
                errors.incrementAndGet();
                throw failure;
            }
        }
    }
}
