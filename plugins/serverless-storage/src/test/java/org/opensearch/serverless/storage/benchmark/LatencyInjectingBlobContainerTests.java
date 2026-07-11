/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.benchmark;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/** Proves {@link LatencyInjectingBlobContainer} actually adds the configured delay before delegating. */
public class LatencyInjectingBlobContainerTests extends OpenSearchTestCase {

    private BlobContainer newFsBlobContainer() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    public void testWriteThenReadAreAtLeastAsSlowAsConfiguredMinimums() throws Exception {
        BlobContainer fsContainer = newFsBlobContainer();
        BlobContainer container = new LatencyInjectingBlobContainer(fsContainer, LatencyProfile.LOW);

        byte[] bytes = randomByteArrayOfLength(64);

        long writeStart = System.nanoTime();
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            container.writeBlob("blob-1", in, bytes.length, true);
        }
        long writeElapsedMillis = (System.nanoTime() - writeStart) / 1_000_000;
        assertTrue(
            "write should take at least the configured min write latency ("
                + LatencyProfile.LOW.writeMinMillis()
                + "ms), took "
                + writeElapsedMillis
                + "ms",
            writeElapsedMillis >= LatencyProfile.LOW.writeMinMillis()
        );

        long readStart = System.nanoTime();
        try (InputStream in = container.readBlob("blob-1")) {
            in.readAllBytes();
        }
        long readElapsedMillis = (System.nanoTime() - readStart) / 1_000_000;
        assertTrue(
            "read should take at least the configured min read latency ("
                + LatencyProfile.LOW.readMinMillis()
                + "ms), took "
                + readElapsedMillis
                + "ms",
            readElapsedMillis >= LatencyProfile.LOW.readMinMillis()
        );
    }

    public void testListIsAtLeastAsSlowAsConfiguredMinimum() throws Exception {
        BlobContainer fsContainer = newFsBlobContainer();
        BlobContainer container = new LatencyInjectingBlobContainer(fsContainer, LatencyProfile.LOW);

        byte[] bytes = randomByteArrayOfLength(16);
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            container.writeBlob("blob-1", in, bytes.length, true);
        }

        long start = System.nanoTime();
        container.listBlobs();
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertTrue(
            "list should take at least the configured min list latency (" + LatencyProfile.LOW.listMinMillis() + "ms)",
            elapsedMillis >= LatencyProfile.LOW.listMinMillis()
        );
    }

    public void testUnconfiguredZeroLatencyDoesNotSleep() throws Exception {
        BlobContainer fsContainer = newFsBlobContainer();
        LatencyProfile zero = new LatencyProfile(0, 0, 0, 0, 0, 0);
        BlobContainer container = new LatencyInjectingBlobContainer(fsContainer, zero);

        byte[] bytes = randomByteArrayOfLength(16);
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            container.writeBlob("blob-1", in, bytes.length, true);
        }
        try (InputStream in = container.readBlob("blob-1")) {
            assertArrayEquals(bytes, in.readAllBytes());
        }
    }
}
