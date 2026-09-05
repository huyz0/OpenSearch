/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.storage.security.ObjectStoreRequestCounter;
import org.opensearch.serverless.storage.security.RequestCountingBlobContainer;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;

/**
 * Applying a whole pin diff in one mutation, which is what PITR reconciliation actually needs.
 *
 * <p>The behaviour has to match the one-call-at-a-time version exactly, or the batching would be a
 * correctness change wearing a performance change's clothes: same final set, same add-before-remove
 * ordering, same no-op when the diff changes nothing.
 */
public class PinDiffBatchingTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;

    private BlobContainer container;
    private ObjectStoreRequestCounter counter;
    private DurablePinRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer raw = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        counter = new ObjectStoreRequestCounter();
        container = new RequestCountingBlobContainer(raw, counter);
        registry = new BlobContainerDurablePinRegistry(container);
    }

    public void testAWholeDiffLandsInOneWrite() throws Exception {
        registry.addPin(INDEX_UUID, SHARD_ID, new PinRecord("pitr", 1, 1));
        registry.addPin(INDEX_UUID, SHARD_ID, new PinRecord("pitr", 1, 2));
        long writesBefore = counter.putCount();

        registry.applyPinDiff(
            INDEX_UUID,
            SHARD_ID,
            List.of(new PinRecord("pitr", 1, 3), new PinRecord("pitr", 1, 4)),
            List.of(new PinRecord("pitr", 1, 1))
        );

        assertEquals("two additions and a removal are one mutation, not three", writesBefore + 1, counter.putCount());
        Set<PinRecord> pins = registry.getPins(INDEX_UUID, SHARD_ID);
        assertEquals(3, pins.size());
        assertFalse(pins.contains(new PinRecord("pitr", 1, 1)));
        assertTrue(pins.contains(new PinRecord("pitr", 1, 2)));
        assertTrue(pins.contains(new PinRecord("pitr", 1, 3)));
        assertTrue(pins.contains(new PinRecord("pitr", 1, 4)));
    }

    /** A diff that changes nothing must not write: the steady state of a reconcile is "nothing moved". */
    public void testADiffThatChangesNothingWritesNothing() throws Exception {
        registry.addPin(INDEX_UUID, SHARD_ID, new PinRecord("pitr", 1, 1));
        long writesBefore = counter.putCount();

        registry.applyPinDiff(INDEX_UUID, SHARD_ID, List.of(new PinRecord("pitr", 1, 1)), List.of(new PinRecord("pitr", 1, 9)));

        assertEquals("re-adding a pin that exists and removing one that does not is a no-op", writesBefore, counter.putCount());
        assertEquals(1, registry.getPins(INDEX_UUID, SHARD_ID).size());
    }

    /** Pins unrelated to the diff -- a snapshot, a clone -- must be untouched by it. */
    public void testADiffLeavesOtherReasonsAlone() throws Exception {
        registry.addPin(INDEX_UUID, SHARD_ID, new PinRecord("nightly", 1, 1));
        registry.applyPinDiff(INDEX_UUID, SHARD_ID, List.of(new PinRecord("pitr", 1, 2)), List.of());

        Set<PinRecord> pins = registry.getPins(INDEX_UUID, SHARD_ID);
        assertTrue(pins.contains(new PinRecord("nightly", 1, 1)));
        assertTrue(pins.contains(new PinRecord("pitr", 1, 2)));
    }
}
