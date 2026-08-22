/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.clone;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

public class BlobContainerCloneLineageStoreTests extends OpenSearchTestCase {

    private BlobContainerCloneLineageStore store;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        store = new BlobContainerCloneLineageStore(blobContainer);
    }

    public void testReadLineageIsEmptyBeforeAnyIsWritten() throws Exception {
        assertEquals(Optional.empty(), store.readLineage());
    }

    public void testWriteThenReadRoundTrips() throws Exception {
        CloneLineage lineage = new CloneLineage("source-idx", 3);
        store.writeLineage(lineage);
        assertEquals(Optional.of(lineage), store.readLineage());
    }

    public void testDeleteLineageIsANoOpWhenNoneExists() throws Exception {
        store.deleteLineage();
        assertEquals(Optional.empty(), store.readLineage());
    }

    public void testDeleteLineageRemovesAPreviouslyWrittenRecord() throws Exception {
        store.writeLineage(new CloneLineage("source-idx", 0));
        store.deleteLineage();
        assertEquals(Optional.empty(), store.readLineage());
    }
}
