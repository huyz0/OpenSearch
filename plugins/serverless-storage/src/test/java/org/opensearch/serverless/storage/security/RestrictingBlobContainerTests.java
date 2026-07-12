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

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Optional;

/**
 * Proves {@link RestrictingBlobContainer} enforces rfc-serverless-opensearch.md &sect;15's
 * "compaction service needs GET+PUT but no DELETE ... GC/reconciler role is the only
 * DELETE-capable principal" against a real {@link BlobContainer} (not a mock), so a genuine
 * {@code writeBlob}/{@code readBlob}/{@code delete} actually reaches (or is refused before ever
 * reaching) real backing storage.
 */
public class RestrictingBlobContainerTests extends OpenSearchTestCase {

    private BlobContainer newFsBlobContainer() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    public void testReadAndWriteAlwaysPassThroughRegardlessOfDeletePermission() throws Exception {
        BlobContainer delegate = newFsBlobContainer();
        BlobContainer restricted = new RestrictingBlobContainer(delegate, false);

        byte[] content = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        restricted.writeBlob("a", new ByteArrayInputStream(content), content.length, true);

        assertTrue("a write through the restricted container must be visible on the real delegate", delegate.blobExists("a"));
        try (var in = restricted.readBlob("a")) {
            assertArrayEquals(content, in.readAllBytes());
        }
    }

    public void testDeleteThrowsWhenNotAllowed() throws Exception {
        BlobContainer delegate = newFsBlobContainer();
        byte[] content = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        delegate.writeBlob("a", new ByteArrayInputStream(content), content.length, true);
        BlobContainer restricted = new RestrictingBlobContainer(delegate, false);

        expectThrows(SecurityException.class, () -> restricted.deleteBlobsIgnoringIfNotExists(List.of("a")));
        expectThrows(SecurityException.class, restricted::delete);

        assertTrue("a denied delete attempt must never reach the real delegate", delegate.blobExists("a"));
    }

    public void testDeleteSucceedsWhenAllowed() throws Exception {
        BlobContainer delegate = newFsBlobContainer();
        byte[] content = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        delegate.writeBlob("a", new ByteArrayInputStream(content), content.length, true);
        BlobContainer restricted = new RestrictingBlobContainer(delegate, true);

        restricted.deleteBlobsIgnoringIfNotExists(List.of("a"));

        assertFalse("a permitted delete must actually reach the real delegate", delegate.blobExists("a"));
    }

    public void testRegisterOperationsAreDelegatedNotThrown() throws Exception {
        // FilterBlobContainer doesn't delegate BlobContainer's default readRegister/
        // compareAndSwapRegister methods on its own -- a real regression this test caught: without
        // an explicit override, both throw UnsupportedOperationException regardless of what the
        // real delegate supports, which broke BlobContainerShardStateStore (register-based) shard
        // recovery the moment this container was wired into the writer/reader engine construction
        // path.
        BlobContainer delegate = newFsBlobContainer();
        BlobContainer restricted = new RestrictingBlobContainer(delegate, false);

        assertEquals(Optional.empty(), restricted.readRegister("r"));

        BlobRegisterCasResult result = restricted.compareAndSwapRegister("r", BlobRegister.ABSENT_GENERATION, new BytesArray("v1"));
        assertTrue(result.applied());

        Optional<BlobRegister> readBack = restricted.readRegister("r");
        assertTrue(readBack.isPresent());
        assertEquals(new BytesArray("v1"), readBack.get().value());
    }

    public void testChildrenStayWrappedWithTheSamePermission() throws Exception {
        java.nio.file.Path root = createTempDir();
        FsBlobStore blobStore = new FsBlobStore(1024, root, false);
        java.nio.file.Files.createDirectory(root.resolve("nested"));
        byte[] content = "x".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        new FsBlobContainer(blobStore, BlobPath.cleanPath().add("nested"), root.resolve("nested")).writeBlob(
            "b",
            new ByteArrayInputStream(content),
            content.length,
            true
        );
        BlobContainer delegate = new FsBlobContainer(blobStore, BlobPath.cleanPath(), root);
        BlobContainer restricted = new RestrictingBlobContainer(delegate, false);

        // Any container reached via children() must itself still be delete-restricted, not a bare passthrough.
        BlobContainer wrappedChild = restricted.children().get("nested");
        assertNotNull(wrappedChild);
        expectThrows(SecurityException.class, () -> wrappedChild.deleteBlobsIgnoringIfNotExists(List.of("b")));
    }
}
