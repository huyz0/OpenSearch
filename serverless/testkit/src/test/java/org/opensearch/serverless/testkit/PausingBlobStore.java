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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * A blob store that can be stopped in the middle of a segment upload, to hold a race window open.
 *
 * <p><b>Why this has to exist.</b> Publishing is read-manifest, upload-segments, compare-and-swap. The
 * gap between the read and the swap is where a writer that has lost its shard can overwrite a successor's
 * commit, and the compare-and-swap on the manifest generation is the only thing that prevents it — the
 * term check cannot, because when the loser read, the manifest's term was genuinely older than its own.
 *
 * <p>That gap is the width of a segment upload. On a filesystem it is microseconds, so the scenario is
 * effectively unreachable and the code protecting it is effectively untested; on an object store it is
 * however long it takes to PUT the shard. Rather than write a test that races and hopes, this opens the
 * window on demand: the first segment write blocks until the test releases it, and everything else runs
 * normally.
 *
 * <p>Only {@code writeBlob} pauses. Register reads and compare-and-swaps deliberately do not, because
 * they are what the test needs to observe happening on both sides of the window.
 */
public final class PausingBlobStore implements BlobStore {

    private final BlobStore delegate;
    private final CountDownLatch release = new CountDownLatch(1);
    private final CountDownLatch reached = new CountDownLatch(1);
    private final AtomicBoolean armed = new AtomicBoolean();

    /**
     * Wraps a store.
     *
     * @param delegate the real store
     */
    public PausingBlobStore(BlobStore delegate) {
        this.delegate = delegate;
    }

    /** Arms the trap: the next segment write blocks until {@link #release()}. */
    public void pauseNextSegmentWrite() {
        armed.set(true);
    }

    /**
     * Waits until a writer is parked inside the window.
     *
     * @param timeout how long to wait
     * @param unit the unit
     * @return true if a writer arrived
     * @throws InterruptedException if interrupted
     */
    public boolean awaitPaused(long timeout, TimeUnit unit) throws InterruptedException {
        return reached.await(timeout, unit);
    }

    /** Lets the parked writer continue. */
    public void release() {
        release.countDown();
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new Pausing(delegate.blobContainer(path));
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    private final class Pausing implements BlobContainer {

        private final BlobContainer inner;

        Pausing(BlobContainer inner) {
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
            // Segment containers only -- a write-ahead log append is also a writeBlob, and pausing one of
            // those would park the wrong writer at the wrong moment.
            if (armed.compareAndSet(true, false) && inner.path().buildAsString().contains("/t=")) {
                reached.countDown();
                try {
                    release.await(2, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while paused", e);
                }
            }
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
            return inner.children().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new Pausing(e.getValue())));
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
            return inner.listBlobsByPrefix(blobNamePrefix);
        }

        @Override
        public Optional<BlobRegister> readRegister(String blobName) throws IOException {
            return inner.readRegister(blobName);
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
            throws IOException {
            return inner.compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }
    }
}
