/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Whether a descriptor write that was queued before a delete can bring the deleted index back.
 *
 * <h2>Why this is reachable</h2>
 *
 * {@code DescriptorBackedIndexLifecycle.recordChange} writes descriptors fire-and-forget, off the cluster
 * state thread, because a blocking write there deadlocks. So a write for index "orders" can still be
 * sitting on GENERIC when the client's delete of "orders" is issued, acknowledged and durable. The write
 * then runs: {@link BlobDescriptorBackend#put} read the live register, found it absent, defaulted to
 * {@code ABSENT_GENERATION}, and compare-and-swapped at it -- which against an absent key is a
 * <em>create</em>. The index came back, permanently: reads consult the live prefix first and find it, so
 * the tombstone is never looked at again, and nothing else in the system removes a live descriptor.
 *
 * <p>{@code createIndex} documents and fixes this exact hazard for the prefix half, having observed it --
 * "a tombstoned name reading back OPEN". These are the same hazard on the point half.
 */
public class DescriptorResurrectionTests extends OpenSearchTestCase {

    private BlobDescriptorBackend backendOver(Path directory) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, directory, false);
        BlobContainer container = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        return new BlobDescriptorBackend(container);
    }

    private static IndexDescriptor descriptor(String name, String uuid) {
        return new IndexDescriptor(
            name,
            uuid,
            1,
            0,
            true,
            IndexDescriptor.State.OPEN,
            List.of(),
            org.opensearch.Version.CURRENT.id,
            false,
            false,
            false,
            false,
            0L,
            0L
        );
    }

    private static void awaitTombstone(BlobDescriptorBackend backend, IndexDescriptor tombstone) throws Exception {
        CountDownLatch durable = new CountDownLatch(1);
        backend.putTombstoneAsync(tombstone, ActionListener.wrap(ignored -> durable.countDown(), failure -> durable.countDown()));
        assertTrue("the tombstone must be durable before the test continues", durable.await(30, TimeUnit.SECONDS));
    }

    /**
     * The finding: a put that lands after the delete must write nothing.
     *
     * <p>Ordered by hand rather than raced, because the interleaving is not a narrow window -- it is
     * "whenever GENERIC is busier than the client is slow" -- and a test that had to win a race to see it
     * would not be a test of anything.
     */
    public void testAPutThatLandsAfterTheDeleteDoesNotBringTheIndexBack() throws Exception {
        BlobDescriptorBackend backend = backendOver(createTempDir());
        IndexDescriptor orders = descriptor("orders", "uuid-A");
        assertTrue(backend.create(orders));

        // The delete: tombstone durable, live key removed.
        awaitTombstone(backend, orders.tombstoned(System.currentTimeMillis()));
        IndexDescriptor afterDelete = backend.get("orders");
        assertNotNull("a deleted name answers with its tombstone, not with null", afterDelete);
        assertFalse(afterDelete.exists());

        // The write that was queued before it, arriving late.
        backend.put(orders);

        IndexDescriptor afterLatePut = backend.get("orders");
        assertNotNull(afterLatePut);
        assertFalse(
            "the late write must not have recreated the live descriptor; if it does, the index is live "
                + "again for the life of the cluster and the tombstone beside it now lies",
            afterLatePut.exists()
        );
    }

    /**
     * A put for one incarnation must not overwrite another's, which is the same hazard from the other side.
     *
     * <p>Delete "orders" (uuid A), recreate it as uuid B, and let A's queued write land. The live key
     * exists this time and holds a different uuid, so the tombstone is consulted -- and it names A, which
     * is this write's own incarnation, so the write is refused. Without that, the live index's shard count,
     * aliases and mapping generation are silently replaced by a dead index's.
     */
    public void testAPutForAPreviousIncarnationDoesNotOverwriteTheCurrentOne() throws Exception {
        BlobDescriptorBackend backend = backendOver(createTempDir());
        IndexDescriptor first = descriptor("orders", "uuid-A");
        assertTrue(backend.create(first));
        awaitTombstone(backend, first.tombstoned(System.currentTimeMillis()));

        IndexDescriptor second = descriptor("orders", "uuid-B");
        assertTrue("the name is free again, so the recreation must win it", backend.create(second));

        backend.put(first);

        IndexDescriptor live = backend.get("orders");
        assertNotNull(live);
        assertTrue(live.exists());
        assertEquals("the live incarnation must be untouched by the dead one's write", "uuid-B", live.uuid());
    }

    /**
     * A legitimate recreation is still recorded, which is what stops the guard above from being a freeze.
     *
     * <p>The tombstone under a name is not by itself a reason to refuse: after a delete and a recreate, the
     * name's tombstone still names the <em>old</em> uuid, and the new incarnation's descriptor has every
     * right to be written. Only a tombstone naming this very uuid says the index this write describes is
     * gone.
     */
    public void testANewIncarnationIsStillRecordedOverAnOldTombstone() throws Exception {
        BlobDescriptorBackend backend = backendOver(createTempDir());
        IndexDescriptor first = descriptor("orders", "uuid-A");
        assertTrue(backend.create(first));
        awaitTombstone(backend, first.tombstoned(System.currentTimeMillis()));

        // No create this time: recordChange's put is the first write the new incarnation gets in the case
        // where creation went through a path that does not create the descriptor itself.
        IndexDescriptor second = descriptor("orders", "uuid-B");
        backend.put(second);

        IndexDescriptor live = backend.get("orders");
        assertNotNull("a new incarnation must still be recordable over an old tombstone", live);
        assertTrue(live.exists());
        assertEquals("uuid-B", live.uuid());
    }

    /**
     * A differing uuid with no tombstone behind it is still an ordinary overwrite, and must stay one.
     *
     * <p>The first version of this guard refused on the uuid alone, and that is too broad. A differing uuid
     * does not say which side is stale, and the only production caller of {@code put} is the elected
     * cluster manager recording an index that is in the cluster state it is applying -- so the ordinary
     * reading of a mismatch is that the <em>store</em> is behind and this write is the repair. Refusing it
     * would leave a live index's descriptor permanently wrong, with nothing else that ever rewrites it, and
     * it broke {@code BlobDescriptorBackendTests.testPutOverwritesWhereCreateWouldHaveLost}, which pins the
     * unconditional semantics {@code put}'s own javadoc promises.
     *
     * <p>The tombstone is what distinguishes the two cases, because the deletion writes it. This test is
     * here so the narrowing does not get quietly widened again.
     */
    public void testADifferingUuidWithNoTombstoneBehindItIsStillAnOrdinaryOverwrite() throws Exception {
        BlobDescriptorBackend backend = backendOver(createTempDir());
        assertTrue(backend.create(descriptor("orders", "uuid-A")));

        backend.put(descriptor("orders", "uuid-B"));

        IndexDescriptor live = backend.get("orders");
        assertNotNull(live);
        assertTrue(live.exists());
        assertEquals("nothing says uuid-B's write is stale, so it must apply", "uuid-B", live.uuid());
    }

    /** And an ordinary put of an index that is simply present still works, unchanged. */
    public void testAnOrdinaryPutOfALiveIndexStillApplies() throws Exception {
        BlobDescriptorBackend backend = backendOver(createTempDir());
        assertTrue(backend.create(descriptor("orders", "uuid-A")));

        IndexDescriptor changed = new IndexDescriptor(
            "orders",
            "uuid-A",
            3,
            0,
            true,
            IndexDescriptor.State.OPEN,
            List.of("orders-alias"),
            org.opensearch.Version.CURRENT.id,
            false,
            false,
            false,
            false,
            0L,
            0L
        );
        backend.put(changed);

        IndexDescriptor live = backend.get("orders");
        assertEquals(3, live.shardCount());
        assertEquals(List.of("orders-alias"), live.aliases());
    }
}
