/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.util.Optional;

public class BlobNameIndexCheckpointStoreTests extends OpenSearchTestCase {

    private BlobNameIndexCheckpointStore storeOver(Path directory) throws Exception {
        return new BlobNameIndexCheckpointStore(new FsBlobStore(1024, directory, false)::blobContainer, BlobPath.cleanPath());
    }

    private static byte[] uuid(int seed) {
        byte[] bytes = new byte[16];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (seed + i);
        }
        return bytes;
    }

    private static CompactNameIndex indexOf(String... names) {
        CompactNameIndexBuilder builder = new CompactNameIndexBuilder();
        int seed = 1;
        for (String name : names) {
            builder.add(name, uuid(seed++), IndexNameEntry.STATUS_OPEN);
        }
        return builder.build();
    }

    /**
     * A tier starting for the first time has no checkpoint, and that is ordinary rather than an error. It
     * rebuilds from the descriptor store, which is what keeps the checkpoint an optimisation rather than
     * the only copy.
     */
    public void testNoCheckpointYetIsAnOrdinaryOutcome() throws Exception {
        assertEquals(Optional.empty(), storeOver(createTempDir()).readLatest());
    }

    public void testACheckpointRoundTripsThroughTheStore() throws Exception {
        BlobNameIndexCheckpointStore store = storeOver(createTempDir());
        CompactNameIndex written = indexOf("logs-a", "logs-b", "metrics-c");

        store.write(written, 7L);

        BlobNameIndexCheckpointStore.Loaded loaded = store.readLatest().orElseThrow();
        assertEquals(7L, loaded.generation());
        assertEquals(written.size(), loaded.index().size());
        assertTrue(loaded.index().contains("logs-a"));
        assertTrue(loaded.index().contains("metrics-c"));
    }

    /** The newest generation wins, which is why each checkpoint is its own object rather than one key. */
    public void testTheNewestGenerationIsTheOneLoaded() throws Exception {
        BlobNameIndexCheckpointStore store = storeOver(createTempDir());
        store.write(indexOf("old"), 3L);
        store.write(indexOf("new-a", "new-b"), 11L);

        BlobNameIndexCheckpointStore.Loaded loaded = store.readLatest().orElseThrow();
        assertEquals(11L, loaded.generation());
        assertTrue(loaded.index().contains("new-a"));
        assertFalse(loaded.index().contains("old"));
    }

    /**
     * Padding is load-bearing, not cosmetic: names sort lexicographically in a listing, so generation 9
     * must not appear newer than generation 10.
     */
    public void testGenerationsSortNumericallyNotLexicographically() throws Exception {
        BlobNameIndexCheckpointStore store = storeOver(createTempDir());
        store.write(indexOf("at-nine"), 9L);
        store.write(indexOf("at-ten"), 10L);

        assertEquals(10L, store.readLatest().orElseThrow().generation());
        assertTrue(BlobNameIndexCheckpointStore.nameFor(9L).compareTo(BlobNameIndexCheckpointStore.nameFor(10L)) < 0);
    }

    /**
     * Two writers at one generation have disagreed about how far the feed has been consumed. Keeping one
     * silently would hide that, and the survivor would be arbitrary.
     */
    public void testTwoWritersAtOneGenerationIsAnError() throws Exception {
        BlobNameIndexCheckpointStore store = storeOver(createTempDir());
        store.write(indexOf("first"), 5L);

        expectThrows(FileAlreadyExistsException.class, () -> store.write(indexOf("second"), 5L));

        assertTrue("the first writer's checkpoint must survive", store.readLatest().orElseThrow().index().contains("first"));
    }
}
