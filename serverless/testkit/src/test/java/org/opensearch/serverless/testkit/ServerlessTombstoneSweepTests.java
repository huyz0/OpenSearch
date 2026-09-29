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
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.cluster.Rendezvous;
import org.opensearch.serverless.metadata.DescriptorStore;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.NameBeingReclaimedException;
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The descriptor tombstone sweep: what it costs, and that it never removes a name someone has recreated.
 *
 * <p>The sweep used to list and read every descriptor on every node, hourly, and delete the expired
 * tombstones in one batch at the end of that walk -- so its cost was the population, and a name recreated
 * during the walk lost its new descriptor. These pin both halves of the replacement: cost set by what was
 * deleted, and a claim a create must respect before the unconditional delete lands.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessTombstoneSweepTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final long QUARANTINE = DescriptorStore.DEFAULT_TOMBSTONE_QUARANTINE_MILLIS;
    /** Far enough on that every bucket written so far is wholly past the quarantine. */
    private static final long PAST_QUARANTINE = QUARANTINE + DescriptorStore.TOMBSTONE_BUCKET_MILLIS + 60_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    /** The tombstone path is the fallback for a store that does not honour a conditional delete; these test it. */
    private static MetadataPlane tombstoning(MetadataPlane plane) {
        plane.descriptors().setConditionalDelete(false);
        return plane;
    }

    private static IndexDescriptor index(String name) {
        return new IndexDescriptor(name, org.opensearch.common.UUIDs.randomBase64UUID(), 1, MAPPING, null);
    }

    /**
     * The sweep reads the names that were deleted, and nothing else, however many live ones there are.
     *
     * <p>Measured at two populations twenty times apart; the register reads must be identical.
     */
    public void testTheSweepCostsWhatWasDeletedNotWhatExists() throws Exception {
        final long small = sweepReadsWithPopulation(10);
        final long large = sweepReadsWithPopulation(200);
        assertEquals("three deleted names, three reads, at either population", 3L, small);
        assertEquals("a population twenty times larger must not cost the sweep one read more", small, large);
    }

    private long sweepReadsWithPopulation(int live) throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = tombstoning(new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL));
        for (int i = 0; i < live; i++) {
            plane.createIndex(index("live-" + i));
        }
        for (int i = 0; i < 3; i++) {
            plane.createIndex(index("gone-" + i));
            assertTrue(plane.deleteIndex("gone-" + i));
        }
        clock.addAndGet(PAST_QUARANTINE);
        // The test filesystem drops stray "extra" files into directories at random, and the sweep reads
        // each as a marker it then finds nothing behind. They are the filesystem's, not the population's.
        long strays = 0;
        for (BlobContainer bucket : markerBuckets(store).values()) {
            strays += bucket.listBlobs().keySet().stream().filter(name -> name.startsWith("extra")).count();
        }
        store.reset();
        assertEquals(3, plane.descriptors().sweepTombstones(clock.get(), QUARANTINE).size());
        final long reads = store.registerReads() - strays;
        assertEquals("only the names that were deleted are gone", List.of(), plane.namesWithPrefix("gone-", 10));
        assertEquals(live, plane.namesWithPrefix("live-", live + 1).size());
        return reads;
    }

    /**
     * A create that arrives between the sweep claiming a tombstone and deleting it is refused, not
     * silently deleted with it.
     *
     * <p>The delete cannot be made conditional, so this is the one ordering that loses an acknowledged
     * index if the create is allowed to swap over the claim. The hook runs the create exactly there.
     */
    public void testACreateInsideTheSweepsWindowIsRefusedRatherThanLost() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final HookedStore store = new HookedStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = tombstoning(new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL));
        plane.createIndex(index("reused"));
        assertTrue(plane.deleteIndex("reused"));
        clock.addAndGet(PAST_QUARANTINE);

        final AtomicReference<Exception> createInWindow = new AtomicReference<>();
        store.beforeDescriptorDelete = () -> {
            // Once: the create below may itself delete descriptors, and must not re-enter this.
            store.beforeDescriptorDelete = null;
            try {
                plane.createIndex(index("reused"));
            } catch (Exception e) {
                createInWindow.set(e);
            }
        };
        assertEquals(List.of("reused"), plane.descriptors().sweepTombstones(clock.get(), QUARANTINE));
        store.beforeDescriptorDelete = null;

        assertTrue(
            "a create racing the sweep's delete must be told to retry, got " + createInWindow.get(),
            createInWindow.get() instanceof NameBeingReclaimedException
        );
        // And the retry, after the delete, simply works.
        plane.createIndex(index("reused"));
        assertTrue("the recreated index must exist", plane.describe("reused").isPresent());
    }

    /**
     * A create that lands between the sweep reading a tombstone and claiming it wins, and the sweep, having
     * lost the claim, deletes nothing.
     *
     * <p>This is the race the walk it replaced lost outright: it read, moved on, and deleted at the end.
     */
    public void testACreateBeforeTheClaimWinsAndTheSweepDeletesNothing() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final HookedStore store = new HookedStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = tombstoning(new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL));
        plane.createIndex(index("raced"));
        assertTrue(plane.deleteIndex("raced"));
        clock.addAndGet(PAST_QUARANTINE);

        store.afterDescriptorRead = name -> {
            if (name.equals("raced")) {
                store.afterDescriptorRead = null;
                plane.createIndex(index("raced"));
            }
        };
        assertTrue(
            "the sweep lost its claim and must remove nothing",
            plane.descriptors().sweepTombstones(clock.get(), QUARANTINE).isEmpty()
        );
        assertNull("the create must have run inside the window", store.afterDescriptorRead);
        assertTrue("the index created in the window must survive the sweep", plane.describe("raced").isPresent());
    }

    /**
     * A sweep that finds its claim older than the deadline does not delete. The claim then holds creates
     * off until the handoff, after which a create takes the name and a later sweep leaves it alone.
     */
    public void testAClaimPastItsDeadlineIsAbandonedNotActedOn() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final HookedStore store = new HookedStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = tombstoning(new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL));
        plane.createIndex(index("slow"));
        assertTrue(plane.deleteIndex("slow"));
        clock.addAndGet(PAST_QUARANTINE);

        // The sweep stalls for the whole deadline right after its claim lands.
        store.afterDescriptorSwap = () -> clock.addAndGet(DescriptorStore.TOMBSTONE_REAP_DEADLINE_MILLIS);
        assertTrue("a stalled sweep must not delete", plane.descriptors().sweepTombstones(clock.get(), QUARANTINE).isEmpty());
        store.afterDescriptorSwap = null;

        expectThrows(NameBeingReclaimedException.class, () -> plane.createIndex(index("slow")));
        clock.addAndGet(DescriptorStore.TOMBSTONE_REAP_HANDOFF_MILLIS);
        plane.createIndex(index("slow"));

        assertTrue(
            "a later sweep finds a live name behind the marker",
            plane.descriptors().sweepTombstones(clock.get(), QUARANTINE).isEmpty()
        );
        assertTrue("and leaves it alone", plane.describe("slow").isPresent());
        assertTrue("the marker is gone, so no sweep reads it again", markerBuckets(store).isEmpty());
    }

    /** A delete that lost its swap leaves a marker behind it, which the sweep drops without touching the index. */
    public void testAMarkerFromALostDeleteIsDroppedAndTheIndexKept() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = tombstoning(new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL));
        plane.createIndex(index("kept"));
        final long stale = plane.descriptors().generationOf("kept") + 7;
        assertFalse(plane.descriptors().deleteIfUnchanged("kept", stale, clock.get()));
        assertEquals("the marker is written before the swap", 1, markerBuckets(store).size());

        clock.addAndGet(PAST_QUARANTINE);
        assertTrue(plane.descriptors().sweepTombstones(clock.get(), QUARANTINE).isEmpty());
        assertTrue(plane.describe("kept").isPresent());
        assertTrue("an emptied bucket is removed", markerBuckets(store).isEmpty());
    }

    /** A bucket another node owns is not listed, let alone read. */
    public void testABucketThisNodeDoesNotOwnCostsNothing() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = tombstoning(new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL));
        plane.createIndex(index("other"));
        assertTrue(plane.deleteIndex("other"));
        clock.addAndGet(PAST_QUARANTINE);

        store.reset();
        assertTrue(plane.descriptors().sweepTombstones(clock.get(), QUARANTINE, bucket -> false).isEmpty());
        assertEquals("not this node's bucket: no register read", 0L, store.registerReads());
        assertEquals("one listing of the buckets and nothing else", 1L, store.reads());
        assertEquals(List.of("other"), plane.descriptors().sweepTombstones(clock.get(), QUARANTINE, bucket -> true));
    }

    /** A bucket holding more markers than one page is drained completely, a page at a time. */
    public void testABucketLargerThanAPageIsDrainedInPages() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = tombstoning(
            new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL)
        );
        plane.descriptors().setMarkerPage(3);
        final int deleted = 10;
        for (int i = 0; i < deleted; i++) {
            plane.createIndex(index("paged-" + i));
            assertTrue(plane.deleteIndex("paged-" + i));
        }
        clock.addAndGet(PAST_QUARANTINE);
        assertEquals(
            "every marker must be reached, not only the first page",
            deleted,
            plane.descriptors().sweepTombstones(clock.get(), QUARANTINE).size()
        );
        assertEquals(List.of(), plane.namesWithPrefix("paged-", deleted + 1));
    }

    /**
     * A pass keeps its cadence while a sweep is draining: the sweep runs beside the pass, not inside it.
     *
     * <p>The sweep is held open on its first read of a deleted name, and the passes that follow must still
     * complete. Run inline, the pass that started the sweep would wait for it -- and with it the node's
     * idle release, backstop publish and view reaping.
     */
    public void testAPassKeepsItsCadenceWhileASweepDrains() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final java.util.concurrent.CountDownLatch reached = new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        final HookedStore store = new HookedStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = tombstoning(new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL));
        plane.createIndex(index("held"));
        plane.createIndex(index("drained"));
        assertTrue(plane.deleteIndex("drained"));
        clock.addAndGet(PAST_QUARANTINE);

        try (ServerlessNode node = new ServerlessNode(nodeSettings())) {
            node.start();
            node.setMetadataPlane(plane);
            final org.opensearch.serverless.reconcile.BackgroundReconciler loop =
                new org.opensearch.serverless.reconcile.BackgroundReconciler(node, plane);
            loop.want("held", 0);
            store.afterDescriptorRead = name -> {
                if (name.equals("drained")) {
                    reached.countDown();
                    try {
                        release.await(60, java.util.concurrent.TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            };
            final int passes = org.opensearch.serverless.reconcile.BackgroundReconciler.REAP_EVERY_PASSES
                * org.opensearch.serverless.reconcile.BackgroundReconciler.TOMBSTONE_SWEEP_EVERY_REAPS + 10;
            final java.util.concurrent.ExecutorService ticker = java.util.concurrent.Executors.newSingleThreadExecutor();
            try {
                final java.util.concurrent.Future<?> ticks = ticker.submit(() -> {
                    for (int pass = 0; pass < passes; pass++) {
                        clock.addAndGet(TTL / 3);
                        loop.tick(clock.get());
                    }
                    return null;
                });
                assertTrue("a sweep must have started and be held open", reached.await(60, java.util.concurrent.TimeUnit.SECONDS));
                ticks.get(30, java.util.concurrent.TimeUnit.SECONDS);
                assertFalse("the sweep must still be draining while the passes completed", loop.tombstoneSweep().isDone());
            } finally {
                release.countDown();
                ticker.shutdown();
                assertTrue(ticker.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS));
            }
            loop.tombstoneSweep().get(30, java.util.concurrent.TimeUnit.SECONDS);
            store.afterDescriptorRead = null;
            // The passes moved the clock on by many minutes while the sweep was held, so its claim was past
            // the deadline by the time it could delete: a paused sweeper must leave the tombstone, not delete.
            assertEquals("a sweep paused past its deadline must not delete", List.of("drained"), plane.namesWithPrefix("drained", 5));
            assertTrue("and the name still reads as absent", plane.describe("drained").isEmpty());
            // Once the abandoned claim is past the handoff, the next sweep takes it.
            clock.addAndGet(DescriptorStore.TOMBSTONE_REAP_HANDOFF_MILLIS);
            assertEquals(List.of("drained"), plane.descriptors().sweepTombstones(clock.get(), QUARANTINE));
            assertEquals(List.of(), plane.namesWithPrefix("drained", 5));
        }
    }

    private org.opensearch.common.settings.Settings nodeSettings() {
        return org.opensearch.common.settings.Settings.builder()
            .put("node.name", "sweep-cadence")
            .put("cluster.name", "serverless-sweep")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** Every key has exactly one owner, the same one whichever node asks, and losing a node moves only its keys. */
    public void testRendezvousGivesEveryKeyOneStableOwner() {
        final List<String> nodes = List.of("node-a", "node-b", "node-c", "node-d");
        final Map<String, String> owners = new HashMap<>();
        for (int hour = 0; hour < 200; hour++) {
            final String key = "tombstones/" + hour;
            final List<String> shuffled = new ArrayList<>(nodes);
            java.util.Collections.shuffle(shuffled, random());
            final String owner = Rendezvous.owner(key, shuffled);
            assertEquals("the answer must not depend on the order members are seen in", owner, Rendezvous.owner(key, nodes));
            owners.put(key, owner);
        }
        assertEquals("every node gets some of the work", 4, new java.util.HashSet<>(owners.values()).size());
        final List<String> withoutC = List.of("node-a", "node-b", "node-d");
        for (Map.Entry<String, String> entry : owners.entrySet()) {
            if (entry.getValue().equals("node-c") == false) {
                assertEquals("a key whose owner survived stays put", entry.getValue(), Rendezvous.owner(entry.getKey(), withoutC));
            }
        }
    }

    /** The marker buckets, less the stray "extra" directories the test filesystem adds at random. */
    private static Map<String, BlobContainer> markerBuckets(BlobStore store) throws IOException {
        final Map<String, BlobContainer> buckets = new HashMap<>(
            store.blobContainer(RegisterMap.tombstones(BlobPath.cleanPath())).children()
        );
        buckets.keySet().removeIf(name -> name.startsWith("extra"));
        return buckets;
    }

    /** Runs a hook at the two points of the sweep a race can land in: after its claim, before its delete. */
    private static final class HookedStore implements BlobStore {

        private final BlobStore delegate;
        volatile IoRunnable beforeDescriptorDelete;
        volatile Runnable afterDescriptorSwap;
        volatile IoConsumer afterDescriptorRead;

        HookedStore(BlobStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            final BlobContainer inner = delegate.blobContainer(path);
            if (path.equals(RegisterMap.indices(BlobPath.cleanPath())) == false) {
                return inner;
            }
            return new DelegatingBlobContainer(inner) {
                @Override
                public java.util.Optional<org.opensearch.common.blobstore.BlobRegister> readRegister(String blobName) throws IOException {
                    final java.util.Optional<org.opensearch.common.blobstore.BlobRegister> read = super.readRegister(blobName);
                    final IoConsumer hook = afterDescriptorRead;
                    if (hook != null) {
                        hook.accept(blobName);
                    }
                    return read;
                }

                @Override
                public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
                    final IoRunnable hook = beforeDescriptorDelete;
                    if (hook != null) {
                        hook.run();
                    }
                    super.deleteBlobsIgnoringIfNotExists(blobNames);
                }

                @Override
                public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
                    throws IOException {
                    final BlobRegisterCasResult result = super.compareAndSwapRegister(blobName, expectedGeneration, newValue);
                    final Runnable hook = afterDescriptorSwap;
                    if (hook != null && result.applied()) {
                        hook.run();
                    }
                    return result;
                }
            };
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    @FunctionalInterface
    private interface IoRunnable {
        void run() throws IOException;
    }

    @FunctionalInterface
    private interface IoConsumer {
        void accept(String name) throws IOException;
    }
}
