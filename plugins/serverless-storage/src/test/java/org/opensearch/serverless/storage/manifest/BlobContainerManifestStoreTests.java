/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.manifest;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class BlobContainerManifestStoreTests extends OpenSearchTestCase {

    private BlobContainer newFsBlobContainer() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    public void testWriteThenReadManifestRoundTrips() throws Exception {
        BlobContainerManifestStore store = new BlobContainerManifestStore(newFsBlobContainer());

        CommitManifest manifest = new CommitManifest(
            "index-uuid",
            0,
            1,
            3,
            "segments_3",
            Map.of("segments_3", new FileReference("bundle-1-3", 0, 100, 1L)),
            10,
            10,
            new WalPosition("epoch-1", 42),
            0,
            PruningStats.empty(),
            123456789L
        );

        store.writeManifest(manifest);
        CommitManifest read = store.readManifest(1, 3);

        assertEquals(manifest, read);
    }

    public void testManifestBlobNameMatchesCanonicalNaming() throws Exception {
        BlobContainer blobContainer = newFsBlobContainer();
        BlobContainerManifestStore store = new BlobContainerManifestStore(blobContainer);

        CommitManifest manifest = new CommitManifest(
            "idx",
            0,
            7,
            42,
            "segments_42",
            Map.of("segments_42", new FileReference("bundle-7-42", 0, 10, 1L)),
            0,
            0,
            null,
            0,
            PruningStats.empty(),
            0L
        );
        store.writeManifest(manifest);

        assertTrue(blobContainer.blobExists("manifest-7-42"));
    }
}
