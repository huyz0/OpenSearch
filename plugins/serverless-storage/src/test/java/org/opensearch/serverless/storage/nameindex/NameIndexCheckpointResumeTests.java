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

import java.nio.file.Path;

/**
 * A restarting node resumes its name index from a checkpoint instead of rebuilding the population.
 *
 * <h2>Why this needed wiring rather than writing</h2>
 *
 * {@link BlobNameIndexCheckpointStore} was complete and tested and had no production caller, so no node ever
 * wrote a checkpoint and none ever read one. A16 chose checkpointing over streaming persistence exactly to
 * keep start-up off the population's critical path, and the choice was never connected to anything, which
 * counting callers is what finds.
 *
 * <h2>Why resuming is asserted through the service rather than the store</h2>
 *
 * {@code BlobNameIndexCheckpointStoreTests} already proves the store round-trips bytes. What was missing is
 * that {@link NameIndexService} loads on construction and writes on request, so these assert the seam
 * between them: names created before a checkpoint are resolvable by a service that has only ever read the
 * store, and the generation advances so the newest checkpoint wins.
 */
public class NameIndexCheckpointResumeTests extends OpenSearchTestCase {

    private BlobNameIndexCheckpointStore storeOver(Path directory) throws Exception {
        return new BlobNameIndexCheckpointStore(new FsBlobStore(1024, directory, false)::blobContainer, BlobPath.cleanPath());
    }

    private static byte[] uuidFor(String name) {
        byte[] uuid = new byte[CompactNameIndex.UUID_LENGTH];
        byte[] source = name.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        System.arraycopy(source, 0, uuid, 0, Math.min(source.length, uuid.length));
        return uuid;
    }

    public void testANewServiceResumesTheNamesADeadOneCheckpointed() throws Exception {
        Path directory = createTempDir();

        NameIndexService first = new NameIndexService(true, storeOver(directory));
        first.getNameIndex().create("tenant-a", uuidFor("tenant-a"), (byte) 0);
        first.getNameIndex().create("tenant-b", uuidFor("tenant-b"), (byte) 0);
        assertTrue("the checkpoint must actually be written", first.checkpoint());

        // A different service over the same store, which is what a restart is.
        NameIndexService resumed = new NameIndexService(true, storeOver(directory));

        assertTrue("a name checkpointed before the restart must resolve after it", resumed.getNameIndex().contains("tenant-a"));
        assertTrue(resumed.getNameIndex().contains("tenant-b"));
        assertFalse("and nothing that was never created may appear", resumed.getNameIndex().contains("tenant-c"));
    }

    /**
     * The generation has to keep climbing across a restart, because readers take the highest one. A service
     * that resumed and then restarted its count would write a lower generation carrying more names, and the
     * next start would silently load the older, smaller checkpoint.
     */
    public void testTheGenerationKeepsClimbingAcrossARestart() throws Exception {
        Path directory = createTempDir();

        NameIndexService first = new NameIndexService(true, storeOver(directory));
        first.getNameIndex().create("tenant-a", uuidFor("tenant-a"), (byte) 0);
        first.checkpoint();
        first.getNameIndex().create("tenant-b", uuidFor("tenant-b"), (byte) 0);
        first.checkpoint();

        long afterTwo = storeOver(directory).readLatest().orElseThrow().generation();

        NameIndexService resumed = new NameIndexService(true, storeOver(directory));
        resumed.getNameIndex().create("tenant-c", uuidFor("tenant-c"), (byte) 0);
        resumed.checkpoint();

        BlobNameIndexCheckpointStore.Loaded latest = storeOver(directory).readLatest().orElseThrow();
        assertTrue("the generation must advance past what the restart loaded", latest.generation() > afterTwo);

        NameIndexService third = new NameIndexService(true, storeOver(directory));
        assertTrue("and the newest checkpoint is the one that wins", third.getNameIndex().contains("tenant-c"));
        assertTrue(third.getNameIndex().contains("tenant-a"));
    }

    /** No checkpoint is an ordinary state, not a failure: the tier starts empty and gets rebuilt. */
    public void testAnEmptyStoreStartsEmptyRatherThanFailing() throws Exception {
        NameIndexService service = new NameIndexService(true, storeOver(createTempDir()));
        assertEquals(0, service.getNameIndex().baseSize());
        assertFalse(service.getNameIndex().contains("anything"));
    }

    /** With the tier off, nothing is read or written, so an operator who never enabled it pays nothing. */
    public void testADisabledTierNeitherReadsNorWrites() throws Exception {
        Path directory = createTempDir();
        NameIndexService writer = new NameIndexService(true, storeOver(directory));
        writer.getNameIndex().create("tenant-a", uuidFor("tenant-a"), (byte) 0);
        writer.checkpoint();

        NameIndexService disabled = new NameIndexService(false, storeOver(directory));
        assertFalse("a disabled tier must not load a checkpoint", disabled.getNameIndex().contains("tenant-a"));
        assertFalse("nor write one", disabled.checkpoint());
    }

    /** With no store configured the service still works, which is what makes the checkpoint optional. */
    public void testNoStoreIsStillAWorkingTier() {
        NameIndexService service = new NameIndexService(true, null);
        service.getNameIndex().create("tenant-a", uuidFor("tenant-a"), (byte) 0);
        assertTrue(service.getNameIndex().contains("tenant-a"));
        assertFalse("with nowhere to write, checkpointing reports that it did nothing", service.checkpoint());
    }
}
