/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.util.Optional;

/**
 * Base for every {@link FilterBlobContainer} subclass in this plugin that needs its own {@code
 * delegate} reference (to call other {@link BlobContainer} methods on it directly, not just the
 * ones this class overrides), which {@link FilterBlobContainer} itself keeps private and doesn't
 * expose.
 *
 * <p>Also delegates {@link #readRegister}/{@link #compareAndSwapRegister} by default --
 * {@link FilterBlobContainer} doesn't override {@link BlobContainer}'s own default
 * implementations of those two, which unconditionally throw {@link UnsupportedOperationException}
 * regardless of what the real delegate supports (Java doesn't fall through to a superinterface
 * default through an unrelated intermediate class). Getting this wrong once already broke every
 * shard's register-based recovery in production code ({@code RestrictingBlobContainer}'s own
 * commit history) after being caught the same way in test-only code first ({@code
 * LatencyInjectingBlobContainer}'s own commit history) -- this base class exists so a third
 * independent copy of that exact bug can't happen again: every subclass gets the delegation for
 * free just by extending this instead of {@link FilterBlobContainer} directly. Deliberately not
 * {@code final}: a subclass that needs to layer its own behavior onto register operations too
 * (e.g. {@code LatencyInjectingBlobContainer} sleeping before delegating) can still override them
 * and call {@code super} for the actual delegation, rather than being forced to reimplement it.
 */
public abstract class RegisterDelegatingBlobContainer extends FilterBlobContainer {

    /** The same instance passed to {@link FilterBlobContainer}'s own constructor, exposed here since that base class keeps its own copy private. */
    protected final BlobContainer delegate;

    /**
     * Wraps {@code delegate}.
     *
     * @param delegate the real container to wrap; subclasses may call methods on this directly for
     *                 anything beyond read/write/delete/register delegation.
     */
    protected RegisterDelegatingBlobContainer(BlobContainer delegate) {
        super(delegate);
        this.delegate = delegate;
    }

    @Override
    public Optional<BlobRegister> readRegister(String blobName) throws IOException {
        return delegate.readRegister(blobName);
    }

    @Override
    public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
        throws IOException {
        return delegate.compareAndSwapRegister(blobName, expectedGeneration, newValue);
    }
}
