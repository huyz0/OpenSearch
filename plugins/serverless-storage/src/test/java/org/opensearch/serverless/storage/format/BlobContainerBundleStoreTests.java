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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Exercises {@link BlobContainerBundleStore} against a real filesystem-backed
 * {@link BlobContainer}, matching rfc-serverless-opensearch.md &sect;16 Phase 1's
 * "repository-level integration tests against FS" requirement.
 */
public class BlobContainerBundleStoreTests extends OpenSearchTestCase {

    private BlobContainer newFsBlobContainer() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    public void testWriteThenReadHeaderAndFilesRoundTrip() throws Exception {
        BlobContainerBundleStore store = new BlobContainerBundleStore(newFsBlobContainer());

        List<BundleFileContent> files = new ArrayList<>();
        int fileCount = randomIntBetween(1, 20);
        for (int i = 0; i < fileCount; i++) {
            files.add(new BundleFileContent("_" + i + ".si", randomByteArrayOfLength(randomIntBetween(0, 8192))));
        }

        SegmentBundle written = store.writeBundle("bundle-1-1", files);

        BundleHeader header = store.readHeader("bundle-1-1", written.length());
        assertEquals(files.size(), header.entries().size());

        for (BundleFileContent file : files) {
            BundleFileEntry entry = header.entries().get(file.name());
            assertNotNull(entry);
            byte[] content = store.readFile("bundle-1-1", entry);
            assertArrayEquals(file.content(), content);
        }
    }

    public void testHeaderLargerThanDefaultPrefixIsStillReadCorrectly() throws Exception {
        BlobContainerBundleStore store = new BlobContainerBundleStore(newFsBlobContainer());

        // Force the header past the 64KiB default prefix by packing many small, long-named files.
        List<BundleFileContent> files = new ArrayList<>();
        for (int i = 0; i < 4000; i++) {
            files.add(new BundleFileContent("segment-file-with-a-longer-name-" + i + ".si", new byte[] { (byte) i }));
        }

        SegmentBundle written = store.writeBundle("bundle-big-header", files);
        BundleHeader header = store.readHeader("bundle-big-header", written.length());
        assertEquals(files.size(), header.entries().size());

        BundleFileEntry lastEntry = header.entries().get(files.get(files.size() - 1).name());
        byte[] content = store.readFile("bundle-big-header", lastEntry);
        assertArrayEquals(files.get(files.size() - 1).content(), content);
    }

    public void testWritingTheSameBundleNameTwiceIsAnIdempotentNoOpNotAFailure() throws Exception {
        // ObjectStoreCommitPublisher relies on this: bundleName is fully deterministic per commit,
        // so a retry after a fault between the bundle upload and the following manifest write must
        // not fail on writeBlobAtomic's failIfAlreadyExists just because the first attempt's bundle
        // upload already succeeded.
        BlobContainerBundleStore store = new BlobContainerBundleStore(newFsBlobContainer());
        List<BundleFileContent> files = List.of(new BundleFileContent("_0.si", randomByteArrayOfLength(256)));

        SegmentBundle first = store.writeBundle("bundle-retry", files);
        SegmentBundle second = store.writeBundle("bundle-retry", files);

        assertArrayEquals(
            "a retried write of the identical bundle must not fail or corrupt the existing blob",
            first.bytes(),
            second.bytes()
        );
        BundleHeader header = store.readHeader("bundle-retry", second.length());
        assertEquals(1, header.entries().size());
    }

    public void testWritingDifferentContentUnderAnAlreadyUsedNameThrowsRatherThanSilentlyTrustingIt() throws Exception {
        // The real bug this guards: LuceneMergeCompactionPublisher#computeNewHead redoes a real
        // Lucene merge on every retry, so two independent attempts against the same unchanged
        // source compute the exact same deterministic bundleName but pack genuinely different
        // bytes (fresh random segment IDs each time) -- caught by a real sustained chaos test
        // before this guard existed, reproduced here directly and deterministically instead of
        // relying on chance.
        BlobContainerBundleStore store = new BlobContainerBundleStore(newFsBlobContainer());
        List<BundleFileContent> first = List.of(new BundleFileContent("_0.si", randomByteArrayOfLength(256)));
        List<BundleFileContent> second = List.of(new BundleFileContent("_0.si", randomByteArrayOfLength(256)));

        store.writeBundle("bundle-collision", first);

        IOException thrown = expectThrows(IOException.class, () -> store.writeBundle("bundle-collision", second));
        assertTrue(
            "the exception must clearly identify this as a content mismatch, not some other failure",
            thrown.getMessage().contains("already exists with different real content")
        );
    }

    public void testWritingDifferentContentThatHappensToBeTheSameLengthIsStillCaught() throws Exception {
        // A length-only check was tried first and found insufficient: fixed-width fields (like
        // Lucene's random segment IDs) mean two different real contents can share the exact same
        // total byte length. This test forces that exact case -- same length, different bytes --
        // proving the guard compares real content, not just size.
        BlobContainerBundleStore store = new BlobContainerBundleStore(newFsBlobContainer());
        byte[] contentA = new byte[64];
        byte[] contentB = contentA.clone();
        contentB[0] ^= 0xFF; // one bit flipped -- identical length, genuinely different content.
        List<BundleFileContent> first = List.of(new BundleFileContent("_0.si", contentA));
        List<BundleFileContent> second = List.of(new BundleFileContent("_0.si", contentB));

        store.writeBundle("bundle-same-length-collision", first);

        IOException thrown = expectThrows(IOException.class, () -> store.writeBundle("bundle-same-length-collision", second));
        assertTrue(thrown.getMessage().contains("already exists with different real content"));
    }

    public void testCorruptedBundleDetectedOnRead() throws Exception {
        BlobContainer blobContainer = newFsBlobContainer();
        BlobContainerBundleStore store = new BlobContainerBundleStore(blobContainer);

        List<BundleFileContent> files = List.of(new BundleFileContent("_0.si", randomByteArrayOfLength(512)));
        SegmentBundle written = store.writeBundle("bundle-corrupt", files);
        BundleFileEntry entry = written.entries().get("_0.si");

        // Overwrite the blob with a bit-flipped copy at the same name.
        byte[] corrupted = written.bytes().clone();
        corrupted[(int) entry.offset()] ^= 0xFF;
        blobContainer.writeBlob("bundle-corrupt", new java.io.ByteArrayInputStream(corrupted), corrupted.length, false);

        expectThrows(BundleFormatException.class, () -> store.readFile("bundle-corrupt", entry));
    }
}
