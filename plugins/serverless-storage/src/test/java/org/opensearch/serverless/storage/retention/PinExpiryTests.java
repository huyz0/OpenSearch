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
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.serverless.storage.gc.ManifestId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Set;

/**
 * That a pin's expiry is honoured where it has to be, and that adding it did not make existing pins
 * unreadable.
 *
 * <h2>The one place expiry has to be read</h2>
 *
 * {@code getPinnedManifestIds} is the only question garbage collection asks of the pin registry, so it is
 * the only place an expiry can be honoured -- and the only place it can be quietly forgotten, which would
 * leave a feature that looks implemented and holds nothing back. The assertion below is on that method
 * rather than on {@code isLiveAt}, for exactly that reason: a predicate nobody consults is decoration.
 *
 * <h2>The format, which is durable</h2>
 *
 * Pin registers live in the object store, so a deployment upgrading into this change has pins on disk
 * written before pins had an owner or an expiry. Reading five fields out of a three-field record does not
 * fail cleanly -- it reads into the next record and produces nonsense -- and for the registry that decides
 * what may be deleted, nonsense is the worst available outcome. So the old shape is still read, and read as
 * never expiring, which is the only safe interpretation of a pin taken when nothing could expire.
 */
public class PinExpiryTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";

    public void testAnExpiredPinStopsHoldingItsGenerationForGarbageCollection() throws IOException {
        DurablePinRegistry registry = registry();
        registry.addPin(INDEX_UUID, 0, new PinRecord("lapses", 1, 10, "node-a", 5_000));
        registry.addPin(INDEX_UUID, 0, new PinRecord("permanent", 1, 11, "node-a", PinRecord.NEVER_EXPIRES));

        Set<ManifestId> beforeExpiry = registry.getPinnedManifestIds(INDEX_UUID, 0, 4_999);
        assertEquals("both pins hold their generations while neither has lapsed", 2, beforeExpiry.size());

        Set<ManifestId> atExpiry = registry.getPinnedManifestIds(INDEX_UUID, 0, 5_000);
        assertEquals(
            "the boundary releases: a pin that expires at an instant is not holding anything at that "
                + "instant, which is the reading that makes a lapsed pin definitely collectable rather "
                + "than collectable one millisecond later",
            1,
            atExpiry.size()
        );

        Set<ManifestId> afterExpiry = registry.getPinnedManifestIds(INDEX_UUID, 0, 9_999);
        assertEquals("the permanent one is unaffected by any clock", 1, afterExpiry.size());
        assertTrue(afterExpiry.contains(new ManifestId(1, 11)));
        assertFalse(
            "and the lapsed one must be absent, or garbage collection still cannot touch what it held",
            afterExpiry.contains(new ManifestId(1, 10))
        );
        assertEquals(
            "the record itself is still there -- expiry stops it counting, it does not delete it, so a "
                + "sweep can still say who left it behind",
            2,
            registry.getPins(INDEX_UUID, 0).size()
        );
    }

    public void testConfirmingAPinMakesItPermanentWithoutMovingWhatItPins() throws IOException {
        DurablePinRegistry registry = registry();
        registry.addPin(INDEX_UUID, 0, new PinRecord("two-phase", 3, 42, "node-a", 5_000));

        registry.confirmPin(INDEX_UUID, 0, "two-phase", PinRecord.NEVER_EXPIRES);

        PinRecord confirmed = registry.getPins(INDEX_UUID, 0).iterator().next();
        assertEquals(PinRecord.NEVER_EXPIRES, confirmed.expiresAtMillis());
        assertEquals("confirming must not move the generation it pins", 42, confirmed.generation());
        assertEquals("nor the term", 3, confirmed.primaryTerm());
        assertEquals("nor forget who took it", "node-a", confirmed.ownerId());
        assertEquals(
            "and it must still hold its generation long after the expiry it used to have",
            1,
            registry.getPinnedManifestIds(INDEX_UUID, 0, 1_000_000).size()
        );
    }

    public void testConfirmingAPinThatIsNotThereDoesNothing() throws IOException {
        DurablePinRegistry registry = registry();
        registry.addPin(INDEX_UUID, 0, new PinRecord("other", 1, 1, "node-a", 5_000));

        registry.confirmPin(INDEX_UUID, 0, "never-taken", PinRecord.NEVER_EXPIRES);

        assertEquals(1, registry.getPins(INDEX_UUID, 0).size());
        assertEquals(
            "an unrelated pin must not be made permanent by confirming a different one",
            5_000L,
            registry.getPins(INDEX_UUID, 0).iterator().next().expiresAtMillis()
        );
    }

    /**
     * A register written before pins had an owner or an expiry, byte for byte, read back by today's code.
     * Written by hand rather than by an old class, because the old class no longer exists to write it.
     */
    public void testPinsWrittenBeforeExpiryExistedAreStillReadableAndNeverExpire() throws IOException {
        BlobContainer container = container();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(2); // the old format's collection size, and no version marker
            for (long generation : new long[] { 7, 8 }) {
                out.writeString("old-pin");
                out.writeVLong(1);
                out.writeVLong(generation);
            }
            // Written as a register rather than a plain blob, because that is what the registry reads --
            // a register carries a generation for compare-and-swap and is not simply a blob with bytes in
            // it. Getting this wrong is how the first version of this test failed, and it failed loudly,
            // which is the right way round for a fixture that is pretending to be old data.
            container.compareAndSwapRegister("pins-" + INDEX_UUID + "-0", BlobRegister.ABSENT_GENERATION, out.bytes());
        }

        DurablePinRegistry registry = new BlobContainerDurablePinRegistry(container);
        Set<PinRecord> pins = registry.getPins(INDEX_UUID, 0);
        assertEquals("both old records must come back, not one record and some nonsense", 2, pins.size());
        for (PinRecord pin : pins) {
            assertEquals("old-pin", pin.pinId());
            assertEquals(
                "a pin taken when nothing could expire never consented to expiring, so it must not",
                PinRecord.NEVER_EXPIRES,
                pin.expiresAtMillis()
            );
        }
        assertEquals(
            "and they must still hold their generations at any instant",
            2,
            registry.getPinnedManifestIds(INDEX_UUID, 0, Long.MAX_VALUE - 1).size()
        );
    }

    public void testARegisterWrittenTodayRoundTrips() throws IOException {
        DurablePinRegistry registry = registry();
        registry.addPin(INDEX_UUID, 0, new PinRecord("round-trip", 2, 20, "node-b", 12_345));

        PinRecord read = registry.getPins(INDEX_UUID, 0).iterator().next();
        assertEquals("round-trip", read.pinId());
        assertEquals(2, read.primaryTerm());
        assertEquals(20, read.generation());
        assertEquals("node-b", read.ownerId());
        assertEquals(12_345L, read.expiresAtMillis());
    }

    private DurablePinRegistry registry() throws IOException {
        return new BlobContainerDurablePinRegistry(container());
    }

    private BlobContainer container() throws IOException {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }
}
