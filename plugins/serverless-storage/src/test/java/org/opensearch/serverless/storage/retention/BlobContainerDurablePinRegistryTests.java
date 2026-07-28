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
import org.opensearch.serverless.storage.gc.ManifestId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class BlobContainerDurablePinRegistryTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;

    private DurablePinRegistry newRegistry() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        return new BlobContainerDurablePinRegistry(blobContainer);
    }

    public void testNoPinsInitially() throws Exception {
        DurablePinRegistry registry = newRegistry();
        assertEquals(Set.of(), registry.getPins(INDEX_UUID, SHARD_ID));
    }

    public void testAddThenGetReturnsThePin() throws Exception {
        DurablePinRegistry registry = newRegistry();
        PinRecord pin = new PinRecord("snapshot-1", 1, 5);
        registry.addPin(INDEX_UUID, SHARD_ID, pin);

        assertEquals(Set.of(pin), registry.getPins(INDEX_UUID, SHARD_ID));
        assertEquals(Set.of(new ManifestId(1, 5)), registry.getPinnedManifestIds(INDEX_UUID, SHARD_ID));
    }

    public void testAddingTheSamePinTwiceIsIdempotent() throws Exception {
        DurablePinRegistry registry = newRegistry();
        PinRecord pin = new PinRecord("snapshot-1", 1, 5);
        registry.addPin(INDEX_UUID, SHARD_ID, pin);
        registry.addPin(INDEX_UUID, SHARD_ID, pin);

        assertEquals(Set.of(pin), registry.getPins(INDEX_UUID, SHARD_ID));
    }

    public void testMultipleIndependentPinsCoexist() throws Exception {
        DurablePinRegistry registry = newRegistry();
        PinRecord snapshot1 = new PinRecord("snapshot-1", 1, 5);
        PinRecord snapshot2 = new PinRecord("snapshot-2", 1, 10);
        PinRecord pitr = new PinRecord("pitr", 1, 3);

        registry.addPin(INDEX_UUID, SHARD_ID, snapshot1);
        registry.addPin(INDEX_UUID, SHARD_ID, snapshot2);
        registry.addPin(INDEX_UUID, SHARD_ID, pitr);

        assertEquals(Set.of(snapshot1, snapshot2, pitr), registry.getPins(INDEX_UUID, SHARD_ID));
    }

    public void testRemovingOnePinLeavesOthersIntact() throws Exception {
        DurablePinRegistry registry = newRegistry();
        PinRecord snapshot1 = new PinRecord("snapshot-1", 1, 5);
        PinRecord snapshot2 = new PinRecord("snapshot-2", 1, 10);
        registry.addPin(INDEX_UUID, SHARD_ID, snapshot1);
        registry.addPin(INDEX_UUID, SHARD_ID, snapshot2);

        registry.removePin(INDEX_UUID, SHARD_ID, "snapshot-1");

        assertEquals(Set.of(snapshot2), registry.getPins(INDEX_UUID, SHARD_ID));
    }

    public void testRemovingNonexistentPinIsANoOp() throws Exception {
        DurablePinRegistry registry = newRegistry();
        registry.addPin(INDEX_UUID, SHARD_ID, new PinRecord("snapshot-1", 1, 5));

        registry.removePin(INDEX_UUID, SHARD_ID, "never-existed");

        assertEquals(1, registry.getPins(INDEX_UUID, SHARD_ID).size());
    }

    public void testRemovingByExactPinRecordLeavesOtherGenerationsOfTheSameReasonIntact() throws Exception {
        // PITR pins many generations under the SAME pinId ("pitr") -- removePin(String pinId)
        // would wipe all of them out at once, which is correct for a single-generation reason
        // like a snapshot but wrong here. removePin(PinRecord) must remove only the one generation.
        DurablePinRegistry registry = newRegistry();
        PinRecord pitrGen3 = new PinRecord("pitr", 1, 3);
        PinRecord pitrGen5 = new PinRecord("pitr", 1, 5);
        registry.addPin(INDEX_UUID, SHARD_ID, pitrGen3);
        registry.addPin(INDEX_UUID, SHARD_ID, pitrGen5);

        registry.removePin(INDEX_UUID, SHARD_ID, pitrGen3);

        assertEquals(Set.of(pitrGen5), registry.getPins(INDEX_UUID, SHARD_ID));
    }

    public void testRemovingAnExactPinRecordThatWasNeverPresentIsANoOp() throws Exception {
        DurablePinRegistry registry = newRegistry();
        PinRecord existing = new PinRecord("pitr", 1, 3);
        registry.addPin(INDEX_UUID, SHARD_ID, existing);

        registry.removePin(INDEX_UUID, SHARD_ID, new PinRecord("pitr", 1, 999));

        assertEquals(Set.of(existing), registry.getPins(INDEX_UUID, SHARD_ID));
    }

    public void testDifferentShardsAreIndependent() throws Exception {
        DurablePinRegistry registry = newRegistry();
        registry.addPin(INDEX_UUID, 0, new PinRecord("snapshot-1", 1, 5));
        registry.addPin(INDEX_UUID, 1, new PinRecord("snapshot-1", 1, 7));

        assertEquals(Set.of(new PinRecord("snapshot-1", 1, 5)), registry.getPins(INDEX_UUID, 0));
        assertEquals(Set.of(new PinRecord("snapshot-1", 1, 7)), registry.getPins(INDEX_UUID, 1));
    }

    // Concurrent independent pin operations on the SAME shard must not clobber each other -- this
    // is exactly the scenario the CAS-retry loop in BlobContainerDurablePinRegistry exists for:
    // two snapshots being created around the same time on one shard.
    public void testConcurrentAddsFromDifferentPinnersAllSucceed() throws Exception {
        DurablePinRegistry registry = newRegistry();
        int pinCount = 20;
        ExecutorService executor = Executors.newFixedThreadPool(pinCount);
        CountDownLatch startLine = new CountDownLatch(1);

        try {
            List<Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < pinCount; i++) {
                final int pinIndex = i;
                futures.add(executor.submit(() -> {
                    try {
                        startLine.await();
                        registry.addPin(INDEX_UUID, SHARD_ID, new PinRecord("pin-" + pinIndex, 1, pinIndex));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            startLine.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
        }

        assertEquals(pinCount, registry.getPins(INDEX_UUID, SHARD_ID).size());
    }

    public void testConcurrentAddAndRemoveOfDifferentPinsBothSucceed() throws Exception {
        DurablePinRegistry registry = newRegistry();
        registry.addPin(INDEX_UUID, SHARD_ID, new PinRecord("to-be-removed", 1, 1));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> adder = executor.submit(() -> {
                try {
                    registry.addPin(INDEX_UUID, SHARD_ID, new PinRecord("to-be-added", 1, 2));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Future<?> remover = executor.submit(() -> {
                try {
                    registry.removePin(INDEX_UUID, SHARD_ID, "to-be-removed");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            adder.get(10, TimeUnit.SECONDS);
            remover.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdown();
        }

        assertEquals(Set.of(new PinRecord("to-be-added", 1, 2)), registry.getPins(INDEX_UUID, SHARD_ID));
    }

    // Regression test for a real bug: TransportSnapshotPinAction used to implement
    // create-or-replace as addPin(newPin) followed by a separate read-then-removePin loop over
    // every OTHER pin sharing the same pinId. Under two concurrent calls for the same pinId (e.g.
    // a client retry racing the original request), each call's independent removal pass could
    // observe and remove the OTHER call's just-added pin -- both calls report success, but zero
    // pins survive. replacePin does the add-and-strip in one atomic CAS mutation instead.
    public void testConcurrentReplacePinsForTheSameReasonNeverBothLosesTheirPin() throws Exception {
        DurablePinRegistry registry = newRegistry();
        int attempts = 20;
        ExecutorService executor = Executors.newFixedThreadPool(attempts);
        CountDownLatch startLine = new CountDownLatch(1);

        try {
            List<Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < attempts; i++) {
                final int generation = i;
                futures.add(executor.submit(() -> {
                    try {
                        startLine.await();
                        registry.replacePin(INDEX_UUID, SHARD_ID, new PinRecord("snapshot-1", 1, generation));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            startLine.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
        }

        Set<PinRecord> pins = registry.getPins(INDEX_UUID, SHARD_ID);
        assertEquals("exactly one pin must survive concurrent replacePin calls for the same pinId -- never zero", 1, pins.size());
        assertEquals("snapshot-1", pins.iterator().next().pinId());
    }
}
