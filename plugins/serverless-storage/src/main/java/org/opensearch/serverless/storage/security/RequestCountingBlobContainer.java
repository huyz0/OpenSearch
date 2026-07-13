/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tallies every real request this plugin's production code makes against the wrapped {@link
 * BlobContainer} into a shared {@link ObjectStoreRequestCounter} -- &sect;18 risk #1's own
 * mitigation ("publish request-count metrics from day one, and treat them as SLOs"). Wired in at
 * {@code ServerlessStoragePlugin}'s own container-resolution seam, so every container this plugin
 * builds (a shard's own regular container, a dedicated WAL container, and the shared node-wide WAL
 * container) is counted the same way, regardless of which credential-scoping/encryption layers get
 * stacked on top of it afterward.
 *
 * <p>Purely additive instrumentation: never denies or alters a single call the way {@link
 * RestrictingBlobContainer} does, and (like that class) extends {@link RegisterDelegatingBlobContainer}
 * rather than a bare {@code FilterBlobContainer} so register-based operations still delegate through
 * instead of throwing {@link UnsupportedOperationException} -- see that base class's own javadoc for
 * the exact regression this avoids.
 */
public final class RequestCountingBlobContainer extends RegisterDelegatingBlobContainer {

    private final ObjectStoreRequestCounter counter;

    /**
     * Wraps {@code delegate}, tallying every request it serves into {@code counter}.
     *
     * @param delegate the real container to instrument.
     * @param counter the shared, node-wide counter to record into.
     */
    public RequestCountingBlobContainer(BlobContainer delegate, ObjectStoreRequestCounter counter) {
        super(delegate);
        this.counter = counter;
    }

    @Override
    protected BlobContainer wrapChild(BlobContainer child) {
        return new RequestCountingBlobContainer(child, counter);
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        counter.recordGet();
        return super.readBlob(blobName);
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        counter.recordGet();
        return super.readBlob(blobName, position, length);
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        counter.recordPut();
        super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        counter.recordPut();
        super.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public DeleteResult delete() throws IOException {
        counter.recordDelete();
        return super.delete();
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        counter.recordDelete();
        super.deleteBlobsIgnoringIfNotExists(blobNames);
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        counter.recordList();
        return super.listBlobs();
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        counter.recordList();
        return super.listBlobsByPrefix(blobNamePrefix);
    }

    @Override
    public Optional<BlobRegister> readRegister(String blobName) throws IOException {
        counter.recordGet();
        return super.readRegister(blobName);
    }

    @Override
    public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
        throws IOException {
        counter.recordPut();
        return super.compareAndSwapRegister(blobName, expectedGeneration, newValue);
    }
}
