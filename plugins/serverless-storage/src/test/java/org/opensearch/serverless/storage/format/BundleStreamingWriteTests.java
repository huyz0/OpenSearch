/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Packing a bundle used to allocate one array holding the header plus every file body concatenated,
 * on top of the caller's own per-file arrays -- a second full copy of the whole commit in heap on
 * the flush thread, and a hard 2 GiB ceiling that failed the writer engine permanently once a shard
 * crossed it. The layout is now planned from metadata and the body streamed on demand.
 */
public class BundleStreamingWriteTests extends OpenSearchTestCase {

    private static List<BundleFileContent> threeFiles() {
        return List.of(
            new BundleFileContent("_0.si", "aaaa".getBytes(StandardCharsets.UTF_8)),
            new BundleFileContent("_0.cfs", "bbbbbbbb".getBytes(StandardCharsets.UTF_8)),
            new BundleFileContent("segments_1", "cc".getBytes(StandardCharsets.UTF_8))
        );
    }

    /** The streamed body must be byte-for-byte what the materialized form would have been. */
    public void testTheStreamedBundleIsIdenticalToTheMaterializedOne() throws Exception {
        SegmentBundle bundle = BundleWriter.write(threeFiles());
        byte[] materialized = bundle.bytes();
        byte[] streamed;
        try (InputStream in = bundle.openStream()) {
            streamed = in.readAllBytes();
        }
        assertArrayEquals(materialized, streamed);
        assertEquals("length() must be known without materializing anything", materialized.length, bundle.length());
    }

    /** The stream is re-openable, because a blob store's write can legitimately be retried. */
    public void testTheStreamCanBeOpenedMoreThanOnce() throws Exception {
        SegmentBundle bundle = BundleWriter.write(threeFiles());
        byte[] first;
        byte[] second;
        try (InputStream in = bundle.openStream()) {
            first = in.readAllBytes();
        }
        try (InputStream in = bundle.openStream()) {
            second = in.readAllBytes();
        }
        assertArrayEquals(first, second);
    }

    /**
     * Planning must not touch the body at all: the header, every offset and every length are
     * derivable from metadata, which is exactly why the upload no longer needs the whole commit in
     * one array.
     */
    public void testTheLayoutKnowsEveryOffsetWithoutMaterializing() {
        BundleLayout layout = BundleWriter.layout(threeFiles());
        long expectedOffset = layout.headerLength();
        for (BundleFileContent file : threeFiles()) {
            BundleFileEntry entry = layout.entries().get(file.name());
            assertNotNull(entry);
            assertEquals(expectedOffset, entry.offset());
            assertEquals(file.content().length, entry.length());
            expectedOffset += file.content().length;
        }
        assertEquals(expectedOffset, layout.totalLength());
    }

    /** And the whole thing must still round-trip through a real blob container and back out. */
    public void testAStreamedBundleRoundTripsThroughARealContainer() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore store = new BlobContainerBundleStore(blobContainer);

        SegmentBundle written = store.writeBundle("bundle-streamed", threeFiles());
        for (BundleFileContent file : threeFiles()) {
            byte[] readBack = store.readFile("bundle-streamed", written.entries().get(file.name()));
            assertArrayEquals("file " + file.name() + " must survive a streamed upload", file.content(), readBack);
        }
        assertEquals(written.length(), blobContainer.listBlobsByPrefix("bundle-streamed").get("bundle-streamed").length());
    }
}
