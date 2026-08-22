/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.e2e;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.serverless.storage.security.RegisterDelegatingBlobContainer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Simulates rfc-serverless-opensearch.md &sect;13's own "Down" object-store state: every call
 * (read <em>and</em> write, unlike {@link ProbabilisticFailingBlobContainer}, which deliberately
 * only ever faults write-shaped calls) fails while {@link #outage} is set, and every call
 * succeeds normally once cleared -- a real caller-flippable on/off switch, not a probability, since
 * a full outage is a genuinely binary state ("the object store is unreachable"), not a flaky one.
 *
 * <p>Deliberately shared across every {@link BlobContainer} instance wrapping the same underlying
 * store, via {@link #wrapChild}: a real object-store outage affects every container backed by that
 * store at once, not one container in isolation, so a single shared {@link AtomicBoolean} is the
 * correct shape here, unlike {@code ProbabilisticFailingBlobContainer}'s own independent
 * per-instance dice roll.
 */
public final class OutageInjectingBlobContainer extends RegisterDelegatingBlobContainer {

    private final AtomicBoolean outage;

    /**
     * Wraps {@code delegate}, failing every call while {@code outage} is {@code true}.
     *
     * @param delegate the real container to inject the outage in front of.
     * @param outage the shared outage flag -- flip to {@code true}/{@code false} to simulate the
     *               object store going down/coming back up.
     */
    public OutageInjectingBlobContainer(BlobContainer delegate, AtomicBoolean outage) {
        super(delegate);
        this.outage = outage;
    }

    @Override
    protected BlobContainer wrapChild(BlobContainer child) {
        return new OutageInjectingBlobContainer(child, outage);
    }

    private void maybeFailFast(String operation) throws IOException {
        if (outage.get()) {
            throw new IOException("injected chaos outage: object store is down [" + operation + "]");
        }
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        maybeFailFast("readBlob");
        return super.readBlob(blobName);
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        maybeFailFast("readBlob(range)");
        return super.readBlob(blobName, position, length);
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        maybeFailFast("writeBlob");
        super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        maybeFailFast("writeBlobAtomic");
        super.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        maybeFailFast("listBlobs");
        return super.listBlobs();
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        maybeFailFast("listBlobsByPrefix");
        return super.listBlobsByPrefix(blobNamePrefix);
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        maybeFailFast("deleteBlobsIgnoringIfNotExists");
        super.deleteBlobsIgnoringIfNotExists(blobNames);
    }

    @Override
    public Optional<BlobRegister> readRegister(String blobName) throws IOException {
        maybeFailFast("readRegister");
        return super.readRegister(blobName);
    }

    @Override
    public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
        throws IOException {
        maybeFailFast("compareAndSwapRegister");
        return super.compareAndSwapRegister(blobName, expectedGeneration, newValue);
    }
}
