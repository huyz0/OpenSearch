/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

/**
 * Proves {@link RegisterDelegatingBlobContainer} itself correctly delegates {@code
 * readRegister}/{@code compareAndSwapRegister} against a real {@link BlobContainer} -- the one
 * piece of behavior every {@code FilterBlobContainer} subclass in this plugin needs and would
 * otherwise have to reimplement (see that class's own javadoc for the bug this exists to prevent
 * a third copy of), verified here directly via a minimal concrete subclass rather than only
 * indirectly through {@link RestrictingBlobContainer}/{@code EncryptingBlobContainer}'s own tests.
 */
public class RegisterDelegatingBlobContainerTests extends OpenSearchTestCase {

    /** The simplest possible concrete subclass: no extra behavior, just the base class's own delegation. */
    private static final class PlainRegisterDelegatingBlobContainer extends RegisterDelegatingBlobContainer {
        PlainRegisterDelegatingBlobContainer(BlobContainer delegate) {
            super(delegate);
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new PlainRegisterDelegatingBlobContainer(child);
        }
    }

    private BlobContainer newFsBlobContainer() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    public void testReadRegisterDelegatesToTheRealContainer() throws Exception {
        BlobContainer delegate = newFsBlobContainer();
        BlobContainer wrapped = new PlainRegisterDelegatingBlobContainer(delegate);

        assertEquals(Optional.empty(), wrapped.readRegister("r"));

        delegate.compareAndSwapRegister("r", BlobRegister.ABSENT_GENERATION, new BytesArray("v1"));

        Optional<BlobRegister> readBack = wrapped.readRegister("r");
        assertTrue(readBack.isPresent());
        assertEquals(new BytesArray("v1"), readBack.get().value());
    }

    public void testCompareAndSwapRegisterDelegatesToTheRealContainer() throws Exception {
        BlobContainer delegate = newFsBlobContainer();
        BlobContainer wrapped = new PlainRegisterDelegatingBlobContainer(delegate);

        BlobRegisterCasResult result = wrapped.compareAndSwapRegister("r", BlobRegister.ABSENT_GENERATION, new BytesArray("v1"));
        assertTrue(result.applied());

        // The write must be visible on the real delegate, not just through the wrapper.
        Optional<BlobRegister> readBackFromDelegate = delegate.readRegister("r");
        assertTrue(readBackFromDelegate.isPresent());
        assertEquals(new BytesArray("v1"), readBackFromDelegate.get().value());
    }
}
