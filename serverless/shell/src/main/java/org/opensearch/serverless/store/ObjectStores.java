/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.s3.S3RepositoryPlugin;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.nio.file.Path;

/**
 * Where a node's data actually lives.
 *
 * <p>Everything above this treats the object store as one interface with compare-and-swap registers,
 * ranged reads and listing on it. This is the one place that decides which implementation provides them,
 * and it exists because until now there was no such place: {@code ServerlessBootstrap} constructed an
 * {@code FsBlobStore} inline, so a node could only ever run on a local directory. R11 had established
 * what an object store must provide and that MinIO provides it, and the shell still could not be pointed
 * at one.
 *
 * <p><b>S3 is reached through {@link S3RepositoryPlugin#getRepositories}, the supported plugin API.</b>
 * The R11 harness builds an {@code S3BlobStore} directly, from inside the plugin's own package, because a
 * conformance suite has to exercise exactly the container production uses and nothing else. Production
 * code copying that would be depending on package-private constructors — so it does not. The cost is a
 * repository's worth of scaffolding; the benefit is that this keeps working when those internals change.
 */
public final class ObjectStores {

    /** Which implementation backs the store: {@code fs} or {@code s3}. */
    public static final String TYPE = "serverless.store.type";

    /** For {@code s3}: the bucket. */
    public static final String BUCKET = "serverless.store.bucket";

    /** For {@code s3}: an endpoint override, for S3-compatible services. Optional. */
    public static final String ENDPOINT = "serverless.store.endpoint";

    /** For {@code s3}: the region. */
    public static final String REGION = "serverless.store.region";

    /**
     * For {@code s3}: address the bucket by path rather than by hostname. Required by most S3-compatible
     * services, which do not have a wildcard DNS entry per bucket.
     */
    public static final String PATH_STYLE = "serverless.store.path_style_access";

    private ObjectStores() {}

    /**
     * Builds the blob store a node should use.
     *
     * @param settings the node's settings
     * @param clusterService the node's cluster service, which the repository needs
     * @param configPath a directory standing in for the node's config path
     * @return the store, which the caller must close
     * @throws Exception if the store cannot be opened
     */
    public static Handle create(Settings settings, ClusterService clusterService, Path configPath) throws Exception {
        final String type = settings.get(TYPE, "fs");
        if ("fs".equals(type)) {
            final String path = settings.get("serverless.store.path");
            if (path == null || path.isBlank()) {
                throw new IllegalArgumentException("serverless.store.path is required for a filesystem store");
            }
            return new Handle(new Metered(new FsBlobStore(8192, Path.of(path), false)), null);
        }
        if ("s3".equals(type) == false) {
            throw new IllegalArgumentException("unknown " + TYPE + ": " + type + "; expected fs or s3");
        }

        final String bucket = settings.get(BUCKET);
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalArgumentException(BUCKET + " is required for an s3 store");
        }

        // Credentials come from the node's keystore under s3.client.default.*, which is where the s3
        // plugin already looks. Deliberately not a serverless.* setting: a second way to spell an access
        // key is a second thing to leak.
        final Settings.Builder repository = Settings.builder().put("bucket", bucket).put("region", settings.get(REGION, "us-east-1"));
        if (settings.get(ENDPOINT) != null) {
            repository.put("endpoint", settings.get(ENDPOINT));
        }
        repository.put("path_style_access", settings.getAsBoolean(PATH_STYLE, false));

        final S3RepositoryPlugin plugin = new S3RepositoryPlugin(settings, configPath);

        // The plugin's asynchronous machinery is built in createComponents, out of executors it
        // contributes to the node's thread pool before that pool exists. The shell builds its own pool
        // without asking any plugin, so those executors are simply absent -- and the symptom is not a
        // missing-executor error but a null AsyncExecutorContainer, hit on the first delete.
        //
        // So the object-store client gets a pool of its own, built from the plugin's own executor
        // builders. Wasteful by a few threads, and worth it: object-store IO does not then share a pool
        // with search or indexing, and starving one cannot starve the other.
        final ThreadPool threadPool = new ThreadPool(
            Settings.builder().put(settings).put("node.name", settings.get("node.name", "serverless") + "-object-store").build(),
            plugin.getExecutorBuilders(settings).toArray(new ExecutorBuilder<?>[0])
        );
        final Environment environment = new Environment(settings, configPath);
        plugin.createComponents(
            null,
            clusterService,
            threadPool,
            null,
            null,
            NamedXContentRegistry.EMPTY,
            environment,
            null,
            null,
            null,
            null
        );

        final Repository.Factory factory = plugin.getRepositories(
            environment,
            NamedXContentRegistry.EMPTY,
            clusterService,
            new RecoverySettings(settings, clusterService.getClusterSettings())
        ).get("s3");

        final Repository created = factory.create(new RepositoryMetadata("serverless", "s3", repository.build()));
        if (created instanceof BlobStoreRepository == false) {
            throw new IllegalStateException("the s3 repository factory returned a " + created.getClass() + ", which has no blob store");
        }
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) created;
        // Started here rather than left to the caller: blobStore() is lazy and throws on a repository
        // that is not started, and the failure reads as a store problem rather than a lifecycle one.
        blobStoreRepository.start();
        return new Handle(new Metered(blobStoreRepository.blobStore()), threadPool);
    }

    /**
     * The store every node runs on: the real one, with every request counted on the way through.
     *
     * <p>The object store is priced per request and the RFC treats request counts as service levels
     * from day one, yet the only counter lived in the test kit and production returned the raw store.
     * A deployment could measure its cost in a test and not in the field. This is that counter, in the
     * one place every store is built, so {@code /_serverless/stats} can report what the node has spent.
     *
     * <p>Reads and writes are counted by kind because they price differently on every real provider.
     * {@code impliedS3Requests} applies what each kind actually costs -- a compare-and-swap is one call
     * here and two requests on S3, a GET for the ETag then a conditional PUT -- and is an estimate derived
     * from reading the S3 container, not from watching the wire. A listing by prefix and a
     * {@code children()} call are both listings, which price above a GET.
     *
     * <p>Bytes are counted at the stream: a ranged read of a segment counts what was actually pulled,
     * which is the number that says whether reads are lazy.
     */
    public static final class Metered implements BlobStore {

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

        /**
         * Wraps a store.
         *
         * @param delegate the real store
         */
        public Metered(BlobStore delegate) {
            this.delegate = delegate;
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
        public BlobContainer blobContainer(BlobPath path) {
            return new Counting(delegate.blobContainer(path));
        }

        @Override
        public void close() throws java.io.IOException {
            delegate.close();
        }

        private final class Counting implements BlobContainer {

            private final BlobContainer inner;

            Counting(BlobContainer inner) {
                this.inner = inner;
            }

            private <T> T count(
                java.util.concurrent.atomic.AtomicLong counter,
                org.opensearch.common.CheckedSupplier<T, java.io.IOException> call
            ) throws java.io.IOException {
                counter.incrementAndGet();
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
                return count(blobReads, () -> inner.blobExists(blobName));
            }

            @Override
            public java.io.InputStream readBlob(String blobName) throws java.io.IOException {
                return new CountedStream(count(blobReads, () -> inner.readBlob(blobName)));
            }

            @Override
            public java.io.InputStream readBlob(String blobName, long position, long length) throws java.io.IOException {
                return new CountedStream(count(blobReads, () -> inner.readBlob(blobName, position, length)));
            }

            @Override
            public void writeBlob(String blobName, java.io.InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
                throws java.io.IOException {
                bytesWritten.addAndGet(Math.max(0L, blobSize));
                count(blobWrites, () -> {
                    inner.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
                    return null;
                });
            }

            @Override
            public void writeBlobAtomic(String blobName, java.io.InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
                throws java.io.IOException {
                bytesWritten.addAndGet(Math.max(0L, blobSize));
                count(blobWrites, () -> {
                    inner.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
                    return null;
                });
            }

            @Override
            public DeleteResult delete() throws java.io.IOException {
                return count(deletes, inner::delete);
            }

            @Override
            public void deleteBlobsIgnoringIfNotExists(java.util.List<String> blobNames) throws java.io.IOException {
                count(deletes, () -> {
                    inner.deleteBlobsIgnoringIfNotExists(blobNames);
                    return null;
                });
            }

            @Override
            public java.util.Map<String, BlobMetadata> listBlobs() throws java.io.IOException {
                return count(listings, inner::listBlobs);
            }

            @Override
            public java.util.Map<String, BlobContainer> children() throws java.io.IOException {
                // A listing with a delimiter on every provider worth naming, and priced as one.
                final java.util.Map<String, BlobContainer> children = count(listings, inner::children);
                final java.util.Map<String, BlobContainer> counted = new java.util.LinkedHashMap<>();
                for (java.util.Map.Entry<String, BlobContainer> child : children.entrySet()) {
                    counted.put(child.getKey(), new Counting(child.getValue()));
                }
                return counted;
            }

            @Override
            public java.util.Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws java.io.IOException {
                return count(listings, () -> inner.listBlobsByPrefix(blobNamePrefix));
            }

            @Override
            public java.util.Optional<BlobRegister> readRegister(String blobName) throws java.io.IOException {
                return count(registerReads, () -> inner.readRegister(blobName));
            }

            @Override
            public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
                throws java.io.IOException {
                return count(registerWrites, () -> inner.compareAndSwapRegister(blobName, expectedGeneration, newValue));
            }

            @Override
            public BlobRegisterCasResult createRegisterIfAbsent(String blobName, BytesReference value) throws java.io.IOException {
                return count(registerWrites, () -> inner.createRegisterIfAbsent(blobName, value));
            }
        }

        /** Counts the bytes a caller actually pulls through a read, which for a ranged read is the range. */
        private final class CountedStream extends java.io.FilterInputStream {

            CountedStream(java.io.InputStream in) {
                super(in);
            }

            @Override
            public int read() throws java.io.IOException {
                final int b = super.read();
                if (b >= 0) {
                    bytesRead.incrementAndGet();
                }
                return b;
            }

            @Override
            public int read(byte[] buffer, int offset, int length) throws java.io.IOException {
                final int n = super.read(buffer, offset, length);
                if (n > 0) {
                    bytesRead.addAndGet(n);
                }
                return n;
            }

            @Override
            public long skip(long n) throws java.io.IOException {
                final long skipped = super.skip(n);
                if (skipped > 0) {
                    bytesRead.addAndGet(skipped);
                }
                return skipped;
            }
        }
    }

    /** A blob store and whatever had to be built to reach it. */
    public static final class Handle implements java.io.Closeable {

        private final BlobStore blobStore;
        private final ThreadPool threadPool;

        Handle(BlobStore blobStore, ThreadPool threadPool) {
            this.blobStore = blobStore;
            this.threadPool = threadPool;
        }

        /**
         * Returns the store.
         *
         * @return the blob store
         */
        public BlobStore blobStore() {
            return blobStore;
        }

        @Override
        public void close() throws java.io.IOException {
            blobStore.close();
            if (threadPool != null) {
                ThreadPool.terminate(threadPool, 10, java.util.concurrent.TimeUnit.SECONDS);
            }
        }
    }
}
