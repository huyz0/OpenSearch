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
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.util.List;

public class RequestCountingBlobContainerTests extends OpenSearchTestCase {

    private BlobContainer newRawContainer() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    public void testWriteBlobIsCountedAsAPut() throws Exception {
        ObjectStoreRequestCounter counter = new ObjectStoreRequestCounter();
        RequestCountingBlobContainer container = new RequestCountingBlobContainer(newRawContainer(), counter);

        byte[] bytes = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        container.writeBlob("a", new ByteArrayInputStream(bytes), bytes.length, true);

        assertEquals(1L, counter.putCount());
        assertEquals(0L, counter.getCount());
    }

    public void testWriteBlobAtomicIsCountedAsAPut() throws Exception {
        ObjectStoreRequestCounter counter = new ObjectStoreRequestCounter();
        RequestCountingBlobContainer container = new RequestCountingBlobContainer(newRawContainer(), counter);

        byte[] bytes = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        container.writeBlobAtomic("a", new ByteArrayInputStream(bytes), bytes.length, true);

        assertEquals(1L, counter.putCount());
    }

    public void testReadBlobIsCountedAsAGet() throws Exception {
        ObjectStoreRequestCounter counter = new ObjectStoreRequestCounter();
        BlobContainer raw = newRawContainer();
        byte[] bytes = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        raw.writeBlob("a", new ByteArrayInputStream(bytes), bytes.length, true);
        RequestCountingBlobContainer container = new RequestCountingBlobContainer(raw, counter);

        try (var in = container.readBlob("a")) {
            in.readAllBytes();
        }

        assertEquals(1L, counter.getCount());
        assertEquals(0L, counter.putCount());
    }

    public void testDeleteBlobsIsCountedAsADelete() throws Exception {
        ObjectStoreRequestCounter counter = new ObjectStoreRequestCounter();
        BlobContainer raw = newRawContainer();
        byte[] bytes = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        raw.writeBlob("a", new ByteArrayInputStream(bytes), bytes.length, true);
        RequestCountingBlobContainer container = new RequestCountingBlobContainer(raw, counter);

        container.deleteBlobsIgnoringIfNotExists(List.of("a"));

        assertEquals(1L, counter.deleteCount());
    }

    public void testListBlobsIsCountedAsAList() throws Exception {
        ObjectStoreRequestCounter counter = new ObjectStoreRequestCounter();
        RequestCountingBlobContainer container = new RequestCountingBlobContainer(newRawContainer(), counter);

        container.listBlobs();

        assertEquals(1L, counter.listCount());
    }

    public void testCompareAndSwapRegisterIsCountedAsAPut() throws Exception {
        ObjectStoreRequestCounter counter = new ObjectStoreRequestCounter();
        RequestCountingBlobContainer container = new RequestCountingBlobContainer(newRawContainer(), counter);

        container.compareAndSwapRegister("register", 0L, new BytesArray("v"));

        assertEquals(1L, counter.putCount());
    }

    public void testReadRegisterIsCountedAsAGet() throws Exception {
        ObjectStoreRequestCounter counter = new ObjectStoreRequestCounter();
        RequestCountingBlobContainer container = new RequestCountingBlobContainer(newRawContainer(), counter);

        container.readRegister("register");

        assertEquals(1L, counter.getCount());
    }

    public void testCountsAreSharedAcrossMultipleWrappedContainers() throws Exception {
        ObjectStoreRequestCounter counter = new ObjectStoreRequestCounter();
        RequestCountingBlobContainer first = new RequestCountingBlobContainer(newRawContainer(), counter);
        RequestCountingBlobContainer second = new RequestCountingBlobContainer(newRawContainer(), counter);

        first.listBlobs();
        second.listBlobs();

        assertEquals("two independently-wrapped containers sharing one counter must both contribute to it", 2L, counter.listCount());
    }
}
