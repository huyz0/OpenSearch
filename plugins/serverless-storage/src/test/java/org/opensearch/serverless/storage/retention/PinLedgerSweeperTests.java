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
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.serverless.storage.clone.ShardCloner;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * The decision {@link PinLedgerSweeper} exists to make: does a ledger's absence-of-pins hold across
 * every shard it names, not just the one whose container the sweep happens to have. Real {@code
 * FsBlobStore} containers throughout -- one per shard, resolved by {@link
 * ShardCloner.ContainerResolver} the same way production would, so the "shard 0 alone is not enough"
 * property this class exists for is exercised for real rather than assumed.
 */
public class PinLedgerSweeperTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx-uuid-1";

    private Map<Integer, BlobContainer> shardContainers;
    private BlobContainerPinLedgerStore ledgerStore;
    private ShardCloner.ContainerResolver resolver;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        shardContainers = new HashMap<>();
        Path root = createTempDir();
        FsBlobStore blobStore = new FsBlobStore(1024, root, false);
        for (int shardId = 0; shardId < 3; shardId++) {
            shardContainers.put(shardId, blobStore.blobContainer(BlobPath.cleanPath().add("shard-" + shardId)));
        }
        ledgerStore = new BlobContainerPinLedgerStore(shardContainers.get(0));
        resolver = (indexUuid, shardId) -> shardContainers.get(shardId);
    }

    private void pin(int shardId, String pinId, long expiresAtMillis) throws IOException {
        new BlobContainerDurablePinRegistry(shardContainers.get(shardId)).addPin(
            INDEX_UUID,
            shardId,
            new PinRecord(pinId, 1, 1, "owner", expiresAtMillis)
        );
    }

    /** Nothing pinned anywhere the ledger names: the ledger is cleared. */
    public void testALedgerWithNoLivePinsOnAnyShardIsCleared() throws Exception {
        ledgerStore.write(new PinLedger("pin-1", "idx", INDEX_UUID, 3, 0));

        PinLedgerSweeper.SweepResult result = new PinLedgerSweeper(ledgerStore, resolver).sweepOnce(1000, Long.MAX_VALUE);

        assertEquals(1, result.ledgersSeen());
        assertEquals(1, result.ledgersCleared());
        assertTrue(result.abandonedPastTtl().isEmpty());
        assertTrue("a cleared ledger must actually be gone from the store", ledgerStore.list().isEmpty());
    }

    /**
     * A live pin on a shard OTHER than 0 must hold the ledger. This is the property the whole class
     * exists for: a sweep that only ever checked shard 0's own container would see nothing and clear
     * this ledger wrongly.
     */
    public void testALivePinOnAShardOtherThanZeroKeepsTheLedger() throws Exception {
        ledgerStore.write(new PinLedger("pin-1", "idx", INDEX_UUID, 3, 0));
        pin(2, "pin-1", PinRecord.NEVER_EXPIRES);

        PinLedgerSweeper.SweepResult result = new PinLedgerSweeper(ledgerStore, resolver).sweepOnce(1000, Long.MAX_VALUE);

        assertEquals(0, result.ledgersCleared());
        assertTrue("the ledger must still be readable, unreleased", ledgerStore.read("pin-1").isPresent());
    }

    /** A pin that has lapsed on its own does not count as live -- PinRecord#isLiveAt is what decides, not mere presence. */
    public void testAnExpiredPinDoesNotKeepTheLedgerAlive() throws Exception {
        ledgerStore.write(new PinLedger("pin-1", "idx", INDEX_UUID, 3, 0));
        pin(1, "pin-1", 500); // expires at 500

        PinLedgerSweeper.SweepResult result = new PinLedgerSweeper(ledgerStore, resolver).sweepOnce(1000, Long.MAX_VALUE);

        assertEquals(1, result.ledgersCleared());
        assertTrue(ledgerStore.list().isEmpty());
    }

    /** A pin under a DIFFERENT id on a shard this ledger names must not be mistaken for this ledger's own. */
    public void testAPinUnderADifferentIdDoesNotKeepThisLedgerAlive() throws Exception {
        ledgerStore.write(new PinLedger("pin-1", "idx", INDEX_UUID, 3, 0));
        pin(1, "some-other-pin", PinRecord.NEVER_EXPIRES);

        PinLedgerSweeper.SweepResult result = new PinLedgerSweeper(ledgerStore, resolver).sweepOnce(1000, Long.MAX_VALUE);

        assertEquals(1, result.ledgersCleared());
    }

    /** Still live and older than the TTL: reported, not deleted -- this class releases nothing. */
    public void testAStillLiveLedgerPastTtlIsReportedNotDeleted() throws Exception {
        ledgerStore.write(new PinLedger("pin-1", "idx", INDEX_UUID, 3, 100));
        pin(0, "pin-1", PinRecord.NEVER_EXPIRES);

        PinLedgerSweeper.SweepResult result = new PinLedgerSweeper(ledgerStore, resolver).sweepOnce(100_000, 1000);

        assertEquals(0, result.ledgersCleared());
        assertEquals(java.util.List.of("pin-1"), result.abandonedPastTtl());
        assertTrue("still live, so the record must survive", ledgerStore.read("pin-1").isPresent());
    }

    /** Still live but under the TTL: neither deleted nor reported -- an ordinary in-flight ledger. */
    public void testAStillLiveLedgerUnderTtlIsNeitherClearedNorReported() throws Exception {
        ledgerStore.write(new PinLedger("pin-1", "idx", INDEX_UUID, 3, 100));
        pin(0, "pin-1", PinRecord.NEVER_EXPIRES);

        PinLedgerSweeper.SweepResult result = new PinLedgerSweeper(ledgerStore, resolver).sweepOnce(500, 1000);

        assertEquals(0, result.ledgersCleared());
        assertTrue(result.abandonedPastTtl().isEmpty());
    }

    /**
     * The fail-safe direction, and the mutation this class exists to guard against: a shard that
     * cannot be read must not be treated as a shard with nothing pinned, or a real pin on it would be
     * silently lost the moment its ledger is deleted out from under it.
     */
    public void testAnUnreadableShardKeepsTheLedgerRatherThanClearingIt() throws Exception {
        ledgerStore.write(new PinLedger("pin-1", "idx", INDEX_UUID, 3, 0));
        BlobContainer unreadableShard1 = new FilterBlobContainer(shardContainers.get(1)) {
            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return child;
            }

            @Override
            public java.util.Optional<org.opensearch.common.blobstore.BlobRegister> readRegister(String blobName) throws IOException {
                // BlobContainerDurablePinRegistry#getPins reads through readRegister, not
                // readBlob/listBlobsByPrefix -- the pin set for a shard lives in one register blob.
                throw new IOException("simulated unreadable container");
            }
        };
        ShardCloner.ContainerResolver flakyResolver = (indexUuid, shardId) -> shardId == 1
            ? unreadableShard1
            : shardContainers.get(shardId);

        PinLedgerSweeper.SweepResult result = new PinLedgerSweeper(ledgerStore, flakyResolver).sweepOnce(1000, Long.MAX_VALUE);

        assertEquals("an unreadable shard must not count as cleared", 0, result.ledgersCleared());
        assertTrue("must not be reported as abandoned either -- it was skipped, not judged", result.abandonedPastTtl().isEmpty());
        assertTrue(ledgerStore.read("pin-1").isPresent());
    }
}
