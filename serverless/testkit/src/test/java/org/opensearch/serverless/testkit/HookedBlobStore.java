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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A blob store that runs something of the test's choosing in the middle of one register read.
 *
 * <p>For a race whose window is a single round trip to the store: a pass that reads a register and then acts on
 * what it read. On a filesystem the round trip is microseconds and the interleaving never happens by chance; on an
 * object store it is tens of milliseconds, and the fleet run found it. This puts the other party's work exactly
 * inside the window, every time.
 */
public final class HookedBlobStore implements BlobStore {

    private record Hook(String pathContains, String blobName, Runnable action) {
    }

    private final BlobStore delegate;
    private final AtomicReference<Hook> armed = new AtomicReference<>();
    private volatile boolean fired;

    /**
     * Wraps a store.
     *
     * @param delegate the real store
     */
    public HookedBlobStore(BlobStore delegate) {
        this.delegate = delegate;
    }

    /**
     * Runs {@code action} once, inside the next read of the named register in a container whose path contains
     * {@code pathContains} -- after the read has been asked for and before its answer is returned.
     *
     * @param pathContains a fragment of the container's path
     * @param blobName the register's name
     * @param action what the other party does in the window
     */
    public void onNextRegisterRead(String pathContains, String blobName, Runnable action) {
        fired = false;
        armed.set(new Hook(pathContains, blobName, action));
    }

    /**
     * Whether the armed action has run.
     *
     * @return true once it has
     */
    public boolean fired() {
        return fired;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new Hooked(delegate.blobContainer(path));
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    private final class Hooked implements BlobContainer {

        private final BlobContainer inner;

        Hooked(BlobContainer inner) {
            this.inner = inner;
        }

        @Override
        public BlobPath path() {
            return inner.path();
        }

        @Override
        public boolean blobExists(String blobName) throws IOException {
            return inner.blobExists(blobName);
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            return inner.readBlob(blobName);
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            return inner.readBlob(blobName, position, length);
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            inner.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            inner.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public DeleteResult delete() throws IOException {
            return inner.delete();
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
            inner.deleteBlobsIgnoringIfNotExists(blobNames);
        }

        @Override
        public Map<String, BlobMetadata> listBlobs() throws IOException {
            return inner.listBlobs();
        }

        @Override
        public Map<String, BlobContainer> children() throws IOException {
            return inner.children().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new Hooked(e.getValue())));
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
            return inner.listBlobsByPrefix(blobNamePrefix);
        }

        @Override
        public List<BlobMetadata> listBlobsByPrefix(String blobNamePrefix, String startAfter, int limit) throws IOException {
            return inner.listBlobsByPrefix(blobNamePrefix, startAfter, limit);
        }

        @Override
        public Map<String, BlobContainer> children(String startAfter, int limit) throws IOException {
            final Map<String, BlobContainer> wrapped = new java.util.LinkedHashMap<>();
            for (Map.Entry<String, BlobContainer> child : inner.children(startAfter, limit).entrySet()) {
                wrapped.put(child.getKey(), new Hooked(child.getValue()));
            }
            return wrapped;
        }

        @Override
        public List<BlobMetadata> listBlobsByPrefixInSortedOrder(String blobNamePrefix, int limit, BlobNameSortOrder order)
            throws IOException {
            return inner.listBlobsByPrefixInSortedOrder(blobNamePrefix, limit, order);
        }

        @Override
        public Optional<BlobRegister> readRegister(String blobName) throws IOException {
            final Optional<BlobRegister> read = inner.readRegister(blobName);
            final Hook hook = armed.get();
            if (hook != null
                && hook.blobName().equals(blobName)
                && inner.path().buildAsString().contains(hook.pathContains())
                && armed.compareAndSet(hook, null)) {
                hook.action().run();
                fired = true;
            }
            return read;
        }

        @Override
        public boolean deleteRegisterIfUnchanged(String blobName, long expectedGeneration) throws IOException {
            return inner.deleteRegisterIfUnchanged(blobName, expectedGeneration);
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
            throws IOException {
            return inner.compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }
    }
}
