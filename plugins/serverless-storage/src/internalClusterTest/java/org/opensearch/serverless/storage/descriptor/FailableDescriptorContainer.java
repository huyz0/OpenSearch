/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/**
 * A descriptor container whose operations can be made to fail, so unavailability can be tested.
 *
 * <h2>Why this exists at all</h2>
 *
 * The two tests that need it used to make the store unavailable by closing the descriptor system index,
 * which is not a thing you can do to a bucket. The properties they assert survive the move -- an unreadable
 * store is not an empty one, and a creation must not be acknowledged when its write cannot land -- so the
 * failure injection moved rather than the tests being dropped.
 *
 * <h2>Why it extends FsBlobContainer instead of wrapping one</h2>
 *
 * {@code FilterBlobContainer} looks like the right base and is not: {@code BlobContainer} declares the
 * register operations as defaults that throw, so a filter that does not override them does not delegate
 * them either -- it throws {@code UnsupportedOperationException} while looking like a pass-through. All four
 * operations the descriptor backend uses are register operations or deletes, so a wrapper built that way
 * would fail every test for the wrong reason. Extending the real container keeps every un-overridden path
 * genuinely real.
 */
public final class FailableDescriptorContainer extends FsBlobContainer {

    /** Set to make every operation below throw, as an object store that has become unreachable would. */
    public volatile boolean failing = false;

    public FailableDescriptorContainer(FsBlobStore blobStore, BlobPath blobPath, Path path) {
        super(blobStore, blobPath, path);
    }

    private void failIfAsked() throws IOException {
        if (failing) {
            throw new IOException("descriptor container is unreachable");
        }
    }

    @Override
    public Optional<BlobRegister> readRegister(String key) throws IOException {
        failIfAsked();
        return super.readRegister(key);
    }

    @Override
    public org.opensearch.common.blobstore.BlobRegisterCasResult createRegisterIfAbsent(
        String blobName,
        org.opensearch.core.common.bytes.BytesReference value
    ) throws IOException {
        failIfAsked();
        return super.createRegisterIfAbsent(blobName, value);
    }

    @Override
    public org.opensearch.common.blobstore.BlobRegisterCasResult compareAndSwapRegister(
        String blobName,
        long expectedGeneration,
        org.opensearch.core.common.bytes.BytesReference newValue
    ) throws IOException {
        failIfAsked();
        return super.compareAndSwapRegister(blobName, expectedGeneration, newValue);
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        failIfAsked();
        super.deleteBlobsIgnoringIfNotExists(blobNames);
    }
}
