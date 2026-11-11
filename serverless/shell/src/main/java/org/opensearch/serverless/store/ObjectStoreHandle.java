/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.threadpool.ThreadPool;

/** A blob store and whatever had to be built to reach it. */
public final class ObjectStoreHandle implements java.io.Closeable {

    private final BlobStore blobStore;
    private final ThreadPool threadPool;
    private final java.io.Closeable[] owners;
    private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();

    ObjectStoreHandle(BlobStore blobStore, ThreadPool threadPool, java.io.Closeable... owners) {
        this.blobStore = blobStore;
        this.threadPool = threadPool;
        // A repository owns its lazy blob store; close that lifecycle rather than closing its store twice.
        this.owners = owners.length == 0 ? new java.io.Closeable[] { blobStore } : owners.clone();
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
        if (closed.compareAndSet(false, true)) {
            try {
                org.opensearch.common.util.io.IOUtils.close(owners);
            } finally {
                if (threadPool != null) {
                    ThreadPool.terminate(threadPool, 10, java.util.concurrent.TimeUnit.SECONDS);
                }
            }
        }
    }
}
