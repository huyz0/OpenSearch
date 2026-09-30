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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * A store whose every request takes at least a fixed extra time, and which records what each request was.
 *
 * <p>A local S3 answers in a millisecond; a real one in tens. Latency, not request count, is what a caller
 * waiting on a cold shard feels, and serial requests multiply it where concurrent ones do not -- so the delay
 * is what makes a sequence's shape measurable on a laptop. The record is what shows which requests the
 * sequence repeats.
 */
final class LatencyInjectingBlobStore implements BlobStore {

    /** One request: what, where, and on which thread. */
    record Request(String op, String key, String thread) {
        @Override
        public String toString() {
            return op + " " + key;
        }
    }

    private final BlobStore inner;
    private volatile long delayNanos;
    private final ConcurrentLinkedQueue<Request> log = new ConcurrentLinkedQueue<>();

    LatencyInjectingBlobStore(BlobStore inner, long delayMillis) {
        this.inner = inner;
        this.delayNanos = TimeUnit.MILLISECONDS.toNanos(delayMillis);
    }

    void setDelayMillis(long delayMillis) {
        this.delayNanos = TimeUnit.MILLISECONDS.toNanos(delayMillis);
    }

    /** Takes and clears what has been recorded since the last call. */
    List<Request> drain() {
        final List<Request> drained = new ArrayList<>();
        for (Request r = log.poll(); r != null; r = log.poll()) {
            drained.add(r);
        }
        return drained;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new Delayed(inner.blobContainer(path));
    }

    @Override
    public void close() throws IOException {
        inner.close();
    }

    @Override
    public Map<String, Long> stats() {
        return inner.stats();
    }

    private void pay(String op, BlobPath path, String name) {
        log.add(new Request(op, path.buildAsString() + (name == null ? "" : name), Thread.currentThread().getName()));
        final long until = System.nanoTime() + delayNanos;
        for (long left = delayNanos; left > 0; left = until - System.nanoTime()) {
            LockSupport.parkNanos(left);
        }
    }

    private final class Delayed extends DelegatingBlobContainer {

        private final BlobContainer delegate;

        Delayed(BlobContainer delegate) {
            super(delegate);
            this.delegate = delegate;
        }

        private Map<String, BlobContainer> wrap(Map<String, BlobContainer> children) {
            final Map<String, BlobContainer> wrapped = new LinkedHashMap<>();
            for (Map.Entry<String, BlobContainer> child : children.entrySet()) {
                wrapped.put(child.getKey(), new Delayed(child.getValue()));
            }
            return wrapped;
        }

        @Override
        public boolean blobExists(String blobName) throws IOException {
            pay("HEAD", path(), blobName);
            return delegate.blobExists(blobName);
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            pay("GET", path(), blobName);
            return delegate.readBlob(blobName);
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            pay("GET-range", path(), blobName);
            return delegate.readBlob(blobName, position, length);
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            pay("PUT", path(), blobName);
            delegate.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            pay("PUT", path(), blobName);
            delegate.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public DeleteResult delete() throws IOException {
            pay("DELETE-tree", path(), null);
            return delegate.delete();
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
            pay("DELETE", path(), String.valueOf(blobNames));
            delegate.deleteBlobsIgnoringIfNotExists(blobNames);
        }

        @Override
        public Map<String, BlobMetadata> listBlobs() throws IOException {
            pay("LIST", path(), null);
            return delegate.listBlobs();
        }

        @Override
        public Map<String, BlobContainer> children() throws IOException {
            pay("LIST-children", path(), null);
            return wrap(delegate.children());
        }

        @Override
        public Map<String, BlobContainer> children(String startAfter, int limit) throws IOException {
            pay("LIST-children", path(), null);
            return wrap(delegate.children(startAfter, limit));
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
            pay("LIST", path(), blobNamePrefix);
            return delegate.listBlobsByPrefix(blobNamePrefix);
        }

        @Override
        public List<BlobMetadata> listBlobsByPrefix(String blobNamePrefix, String startAfter, int limit) throws IOException {
            pay("LIST", path(), blobNamePrefix);
            return delegate.listBlobsByPrefix(blobNamePrefix, startAfter, limit);
        }

        @Override
        public List<BlobMetadata> listBlobsByPrefixInSortedOrder(String blobNamePrefix, int limit, BlobNameSortOrder order)
            throws IOException {
            pay("LIST", path(), blobNamePrefix);
            return delegate.listBlobsByPrefixInSortedOrder(blobNamePrefix, limit, order);
        }

        @Override
        public Optional<BlobRegister> readRegister(String blobName) throws IOException {
            pay("GET-reg", path(), blobName);
            return delegate.readRegister(blobName);
        }

        @Override
        public boolean deleteRegisterIfUnchanged(String blobName, long expectedGeneration) throws IOException {
            pay("DELETE-reg", path(), blobName);
            return delegate.deleteRegisterIfUnchanged(blobName, expectedGeneration);
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
            throws IOException {
            pay("CAS-reg", path(), blobName);
            return delegate.compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }

        @Override
        public BlobRegisterCasResult createRegisterIfAbsent(String blobName, BytesReference value) throws IOException {
            pay("CREATE-reg", path(), blobName);
            return delegate.createRegisterIfAbsent(blobName, value);
        }
    }
}
