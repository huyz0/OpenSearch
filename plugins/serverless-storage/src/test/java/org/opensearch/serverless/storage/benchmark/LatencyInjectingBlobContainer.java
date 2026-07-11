/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.benchmark;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.support.FilterBlobContainer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * A {@link FilterBlobContainer} decorator that sleeps a configurable, simulated delay before
 * delegating each read/write/list operation, in order to approximate real object-store latency
 * (e.g. S3/GCS/Azure) without depending on network access to any real cloud backend.
 *
 * <p><b>This class exists purely for benchmarking and testing</b> -- specifically, to give
 * {@code BlobLatencyBenchmarkTests} a reproducible, controllable stand-in for real object-store
 * latency so this plugin's cold-start/reactivation milestones (rfc-serverless-opensearch.md
 * cold-start/p95/p99 section) can get an illustrative, repeatable signal in CI. Nothing in
 * production code should ever construct this class; it lives in the test sourceset (following
 * the same precedent as this plugin's other fault-injecting {@link FilterBlobContainer}
 * decorators, e.g. {@code WalMirroringTranslogTests.FailNTimesBlobContainer}), not {@code
 * src/main}.
 */
public final class LatencyInjectingBlobContainer extends FilterBlobContainer {

    private final BlobContainer delegate;
    private final LatencyProfile profile;

    /**
     * @param delegate the real blob container to delegate to after sleeping the simulated delay.
     * @param profile the simulated per-operation-type latency to inject.
     */
    public LatencyInjectingBlobContainer(BlobContainer delegate, LatencyProfile profile) {
        super(delegate);
        this.delegate = delegate;
        this.profile = profile;
    }

    @Override
    protected BlobContainer wrapChild(BlobContainer child) {
        return new LatencyInjectingBlobContainer(child, profile);
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        sleep(profile.sampleReadMillis());
        return delegate.readBlob(blobName);
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        sleep(profile.sampleReadMillis());
        return delegate.readBlob(blobName, position, length);
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        sleep(profile.sampleWriteMillis());
        delegate.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        sleep(profile.sampleWriteMillis());
        delegate.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        sleep(profile.sampleListMillis());
        return delegate.listBlobs();
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        sleep(profile.sampleListMillis());
        return delegate.listBlobsByPrefix(blobNamePrefix);
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        sleep(profile.sampleWriteMillis());
        delegate.deleteBlobsIgnoringIfNotExists(blobNames);
    }

    private static void sleep(long millis) throws IOException {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while simulating blob store latency", e);
        }
    }
}
