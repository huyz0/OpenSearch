/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.ConditionalDeleteProbe;
import org.opensearch.serverless.metadata.DescriptorStore;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.ReclaimQueue;
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Deleting an index without a tombstone, where the store honours a conditional delete.
 *
 * <p>A tombstone existed for one reason: a register's generation restarted at 1 when it was deleted and
 * created again, so a compare-and-swap carrying a generation read before a delete landed on what was created
 * after it. Generations now start at random, and a delete is conditional on the generation the caller read,
 * so the descriptor is simply removed -- no tombstone, no marker, no quarantine, no claim -- and a reclaim
 * intent finishes whatever a writer that missed the delete leaves behind. These pin each of the conditions
 * that makes that safe, and the probe that keeps a store which ignores the condition on tombstones.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only, plus a wrapper that ignores the condition.
 */
public class ServerlessConditionalDeleteTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private static IndexDescriptor index(String name, String uuid) {
        return new IndexDescriptor(name, uuid, 1, MAPPING, null);
    }

    /** The filesystem store honours the condition, so a delete leaves no tombstone, no marker and one intent. */
    public void testADeleteLeavesNoTombstoneAndOneIntent() throws Exception {
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), new AtomicLong(1_000L)::get, TTL);
        assertTrue("the filesystem store must pass the probe", plane.descriptors().conditionalDelete());
        plane.createIndex(index("alpha", "uuid-alpha-1"));
        assertTrue(plane.deleteIndex("alpha"));

        final BlobContainer indices = store.blobContainer(RegisterMap.indices(BlobPath.cleanPath()));
        assertTrue("no tombstone: the register is gone", indices.readRegister("alpha").isEmpty());
        assertTrue("no marker", real(store.blobContainer(RegisterMap.tombstones(BlobPath.cleanPath())).children()).isEmpty());
        final Map<String, BlobContainer> due = real(store.blobContainer(RegisterMap.reclaim(BlobPath.cleanPath())).children());
        assertEquals("one reclaim intent", 1, due.size());
        assertEquals(List.of("alpha#uuid-alpha-1"), List.copyOf(real(due.values().iterator().next().listBlobs()).keySet()));
    }

    /**
     * A compare-and-swap carrying a generation read before a delete does not land on the index created
     * after it -- the reason tombstones existed. The generations of the two incarnations differ.
     */
    public void testAStaleSwapCannotLandOnARecreatedName() throws Exception {
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane slow = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        final MetadataPlane fast = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        slow.createIndex(index("alpha", "uuid-alpha-old"));
        final DescriptorStore.Resolution before = slow.descriptors().resolve("alpha");

        assertTrue(fast.deleteIndex("alpha"));
        fast.createIndex(index("alpha", "uuid-alpha-new"));
        final long after = fast.descriptors().generationOf("alpha");
        assertNotEquals("a recreated register must not reuse its predecessor's generation", before.generation(), after);

        // A mapping update prepared against the old incarnation, arriving now.
        final IndexDescriptor stale = before.index();
        assertTrue(
            "a swap carrying the old incarnation's generation must be refused",
            slow.descriptors().update(stale, before.generation()).isEmpty()
        );
        assertEquals("and the new incarnation is untouched", "uuid-alpha-new", fast.describe("alpha").orElseThrow().uuid());
    }

    /** A head a writer re-created after the delete, and blobs it published, are gone once the intent falls due. */
    public void testWhatAWriterLeftAfterTheDeleteIsReclaimed() throws Exception {
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(index("alpha", "uuid-alpha-dead"));
        assertTrue(plane.deleteIndex("alpha"));

        // A writer that had not noticed: it re-takes its head and completes a publish it had begun.
        assertTrue(plane.heads().acquire("alpha", 0, "zombie", "zombie-eph", "uuid-alpha-dead").acquired());
        final BlobContainer segments = store.blobContainer(
            RegisterMap.shardData(BlobPath.cleanPath(), "alpha", "uuid-alpha-dead", 0).add("t=1")
        );
        segments.writeBlob("_0.cfs", new ByteArrayInputStream(new byte[] { 1, 2, 3 }), 3, false);

        assertEquals("nothing is due before the delay", 0, drain(plane, clock.get()));
        clock.addAndGet(ReclaimQueue.DEFAULT_DELAY_MILLIS + 61_000L);
        assertTrue("the due intent must be taken", drain(plane, clock.get()) >= 1);

        assertTrue("the zombie's head must be gone", plane.heads().read("alpha", 0).isEmpty());
        assertTrue(
            "and its published blob",
            real(store.blobContainer(RegisterMap.shardData(BlobPath.cleanPath(), "alpha", "uuid-alpha-dead", 0).add("t=1")).listBlobs())
                .isEmpty()
        );
        assertTrue("and the intent", real(store.blobContainer(RegisterMap.reclaim(BlobPath.cleanPath())).children()).isEmpty());
    }

    /**
     * With a writer holding a shard at the delete, its bytes are purged once -- at reclaim, after that writer
     * can have stopped -- and not at the delete, where a publish it had begun could land after the purge.
     */
    public void testAnIndexWithAWriterIsPurgedOnceAtReclaim() throws Exception {
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(index("alpha", "uuid-alpha-busy"));
        assertTrue(plane.heads().acquire("alpha", 0, "writer", "writer-eph", "uuid-alpha-busy").acquired());
        final BlobPath segments = RegisterMap.shardData(BlobPath.cleanPath(), "alpha", "uuid-alpha-busy", 0).add("t=1");
        store.blobContainer(segments).writeBlob("_0.cfs", new ByteArrayInputStream(new byte[] { 1 }), 1, false);

        assertTrue(plane.deleteIndex("alpha"));
        assertTrue("its head goes at once", plane.heads().read("alpha", 0).isEmpty());
        assertFalse("its bytes wait for the writer to have stopped", real(store.blobContainer(segments).listBlobs()).isEmpty());

        clock.addAndGet(ReclaimQueue.DEFAULT_DELAY_MILLIS + 61_000L);
        assertTrue(drain(plane, clock.get()) >= 1);
        assertTrue("and go at reclaim", real(store.blobContainer(segments).listBlobs()).isEmpty());
    }

    /** With no writer at the delete, the bytes go at once. */
    public void testAnIdleIndexIsPurgedAtTheDelete() throws Exception {
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), new AtomicLong(1_000L)::get, TTL);
        plane.createIndex(index("alpha", "uuid-alpha-idle"));
        final BlobPath segments = RegisterMap.shardData(BlobPath.cleanPath(), "alpha", "uuid-alpha-idle", 0).add("t=1");
        store.blobContainer(segments).writeBlob("_0.cfs", new ByteArrayInputStream(new byte[] { 1 }), 1, false);
        assertTrue(plane.deleteIndex("alpha"));
        assertTrue(real(store.blobContainer(segments).listBlobs()).isEmpty());
    }

    /**
     * The reclaim of an old incarnation leaves the new one's head alone, though both are named alike: a head
     * deleted by name here would let a third node take a shard its owner still holds.
     */
    public void testReclaimingAnOldIncarnationLeavesTheNewOnesHead() throws Exception {
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(index("alpha", "uuid-alpha-dead"));
        assertTrue(plane.deleteIndex("alpha"));
        plane.createIndex(index("alpha", "uuid-alpha-live"));
        assertTrue(plane.heads().acquire("alpha", 0, "owner", "owner-eph", "uuid-alpha-live").acquired());

        clock.addAndGet(ReclaimQueue.DEFAULT_DELAY_MILLIS + 61_000L);
        assertTrue("the due intent must be taken", drain(plane, clock.get()) >= 1);
        assertEquals("the new incarnation's head must survive", "owner", plane.heads().read("alpha", 0).orElseThrow().ownerNodeId());
        assertEquals("uuid-alpha-live", plane.describe("alpha").orElseThrow().uuid());
    }

    /**
     * A store that accepts a conditional delete and ignores the condition fails the probe, and deletes then
     * leave tombstones rather than removing registers a racing write may have just replaced.
     */
    public void testAStoreIgnoringTheConditionKeepsTombstones() throws Exception {
        final BlobStore careless = new IgnoresConditionalDelete(new FsBlobStore(1024, createTempDir(), false));
        assertFalse(
            "a store that deletes at a stale generation must fail the probe",
            ConditionalDeleteProbe.honoured(careless, RegisterMap.probe(BlobPath.cleanPath()))
        );

        final MetadataPlane plane = new MetadataPlane(careless, BlobPath.cleanPath(), new AtomicLong(1_000L)::get, TTL);
        assertFalse(plane.descriptors().conditionalDelete());
        plane.createIndex(index("alpha", "uuid-alpha-1"));
        assertTrue(plane.deleteIndex("alpha"));
        final Optional<BlobRegister> left = careless.blobContainer(RegisterMap.indices(BlobPath.cleanPath())).readRegister("alpha");
        assertTrue("the fallback leaves a tombstone in the register's place", left.isPresent());
        assertTrue(plane.describe("alpha").isEmpty());
    }

    private static int drain(MetadataPlane plane, long now) throws IOException {
        return plane.reclaimQueue().drain(now, bucket -> true, plane::reclaim);
    }

    /** Leaves out the stray "extra" files and directories the test filesystem adds at random. */
    private static <V> Map<String, V> real(Map<String, V> listed) {
        final Map<String, V> copy = new java.util.HashMap<>(listed);
        copy.keySet().removeIf(name -> name.startsWith("extra"));
        return copy;
    }

    /** A store whose conditional delete deletes whatever is there, as an S3-compatible store ignoring If-Match would. */
    private static final class IgnoresConditionalDelete implements BlobStore {

        private final BlobStore delegate;

        IgnoresConditionalDelete(BlobStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new DelegatingBlobContainer(delegate.blobContainer(path)) {
                @Override
                public boolean deleteRegisterIfUnchanged(String blobName, long expectedGeneration) throws IOException {
                    final boolean existed = readRegister(blobName).isPresent();
                    deleteBlobsIgnoringIfNotExists(List.of(blobName));
                    return existed;
                }
            };
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
