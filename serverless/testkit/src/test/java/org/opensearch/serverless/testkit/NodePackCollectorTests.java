/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.ConditionalBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.serverless.reconcile.NodePackCollector;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.FileLocation;
import org.opensearch.serverless.store.NodePackLedger;
import org.opensearch.serverless.store.NodePackStore;
import org.opensearch.serverless.store.NodePackWriter;
import org.opensearch.serverless.store.SegmentPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A node's packs are deleted once no shard's manifest names them, found through the node's ledger and each pack's
 * header rather than any listing, and never sooner than the grace allows.
 */
public class NodePackCollectorTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final long MINUTE = 60_000L;

    private AtomicLong clock;
    private CountingBlobStore store;
    private MetadataPlane plane;
    private NodePackStore packs;
    private NodePackLedger ledger;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clock = new AtomicLong(1_000_000_000L);
        store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false));
        plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        packs = new NodePackStore(store, BlobPath.cleanPath(), "node-1");
        ledger = new NodePackLedger(store, BlobPath.cleanPath(), "node-1");
    }

    @Override
    public void tearDown() throws Exception {
        store.close();
        super.tearDown();
    }

    private static NodePackStore.Part part(String index, String uuid, int shard, String name) {
        final byte[] content = ("bytes of " + name).getBytes(StandardCharsets.UTF_8);
        return new NodePackStore.Part(uuid, shard, name, content.length, () -> new ByteArrayInputStream(content), index);
    }

    /** Points a shard's manifest at the given landed blocks, one file per block. */
    private void manifest(String index, String uuid, int shard, List<NodePackWriter.Landed> files) throws Exception {
        final Map<String, String> located = new LinkedHashMap<>();
        final Map<String, Long> lengths = new LinkedHashMap<>();
        int n = 0;
        for (NodePackWriter.Landed landed : files) {
            final String file = "_" + (n++) + ".cfs";
            located.put(file, landed.fileAt(0L, landed.block().length()).encode());
            lengths.put(file, landed.block().length());
        }
        final CommitManifest manifest = new CommitManifest(1L, located, "writer", lengths);
        final ConditionalBlobContainer container = ConditionalBlobContainer.of(
            store.blobContainer(RegisterMap.shardData(BlobPath.cleanPath(), index, uuid, shard))
        );
        final Optional<BlobRegister> current = container.readRegister(SegmentPublisher.MANIFEST);
        if (current.isEmpty()) {
            container.createRegisterIfAbsent(SegmentPublisher.MANIFEST, manifest.toBytes());
        } else {
            container.compareAndSwapRegister(SegmentPublisher.MANIFEST, current.get().generation(), manifest.toBytes());
        }
    }

    /** What reading the live views and snapshots costs in listings, which every sweep pays once per pass. */
    private long viewAndSnapshotListings() throws Exception {
        final long before = store.listings();
        plane.livePointsInTime(clock.get());
        plane.liveSnapshots();
        final long cost = store.listings() - before;
        store.reset();
        return cost;
    }

    private NodePackCollector collector() {
        return new NodePackCollector(packs, ledger, plane, clock::get, MINUTE);
    }

    private NodePackWriter writer(long epoch) throws Exception {
        final NodePackWriter writer = new NodePackWriter(packs, epoch, 0L, Long.MAX_VALUE);
        writer.recordIn(ledger);
        return writer;
    }

    public void testAPackGoesOnlyWhenNoShardInItNamesItAndNeverBeforeTheGrace() throws Exception {
        final NodePackWriter writer = writer(5L);
        final NodePackCollector collector = collector();
        collector.follow(writer);
        // Pack 0 holds a block of each of two indices; pack 1 holds only alpha's.
        final List<NodePackWriter.Landed> zero = writer.add(
            List.of(part("alpha", "uuid-a", 0, "_0.seg"), part("bravo", "uuid-b", 1, "_0.seg"))
        );
        final List<NodePackWriter.Landed> one = writer.add(List.of(part("alpha", "uuid-a", 0, "_1.seg")));
        manifest("alpha", "uuid-a", 0, List.of(one.get(0)));
        manifest("bravo", "uuid-b", 1, List.of(zero.get(1)));

        collector.run();
        store.reset();
        assertEquals("a pass with nothing due makes no requests", new NodePackCollector.Pass(0, 0, 0, 0, 0), collector.run());
        assertEquals(0L, store.blobReads() + store.registerReads() + store.blobWrites() + store.registerWrites());

        // Unreferenced but young: a publish may still be about to name it.
        manifest("bravo", "uuid-b", 1, List.of());
        clock.addAndGet(NodePackWriter.COLLECTABLE_AFTER_MILLIS - 1);
        assertEquals(0, collector.run().judged());

        clock.addAndGet(1);
        store.reset();
        NodePackCollector.Pass pass = collector.run();
        assertEquals("both packs judged, each shard's manifest read once", new NodePackCollector.Pass(2, 0, 2, 0, 2), pass);
        logger.info("a judging pass over 2 packs of 2 shards: {}", store.byPlace());
        assertEquals("no listing of packs or shards", 0L, store.listings() - viewAndSnapshotListings());
        assertEquals("first sight of pack 0 unreferenced only marks it", 2, collector.trackedPacks());

        clock.addAndGet(MINUTE);
        store.reset();
        pass = collector.run();
        logger.info("a pass deleting 1 pack and keeping 1: {}", store.byPlace());
        assertEquals("seen unreferenced twice, a minute apart", 1, pass.deleted());
        assertTrue(packs.readHeader(5L, 0L).isEmpty());
        assertTrue("alpha still names pack 1", packs.readHeader(5L, 1L).isPresent());
        assertEquals("every slot below 1 is collected", new NodePackLedger.Epoch(5L, 1L, 2L), ledger.read().epoch(5L).orElseThrow());

        // A pack found referenced is looked at less and less often.
        clock.addAndGet(MINUTE / 2);
        assertEquals(0, collector.run().judged());
    }

    public void testAPackIsKeptWhileAnotherShardInItStillNamesIt() throws Exception {
        final NodePackWriter writer = writer(1L);
        final NodePackCollector collector = collector();
        collector.follow(writer);
        final List<NodePackWriter.Landed> landed = writer.add(
            List.of(part("alpha", "uuid-a", 0, "_0.seg"), part("bravo", "uuid-b", 0, "_0.seg"))
        );
        manifest("alpha", "uuid-a", 0, List.of());
        manifest("bravo", "uuid-b", 0, List.of(landed.get(1)));
        for (int pass = 0; pass < 5; pass++) {
            clock.addAndGet(NodePackWriter.COLLECTABLE_AFTER_MILLIS);
            assertEquals(0, collector.run().deleted());
        }
        assertTrue(packs.readHeader(1L, 0L).isPresent());
    }

    public void testAPackWithoutIndexNamesIsNeverJudgedUnreferenced() throws Exception {
        final NodePackWriter writer = writer(1L);
        final NodePackCollector collector = collector();
        collector.follow(writer);
        final byte[] bytes = "old".getBytes(StandardCharsets.UTF_8);
        writer.add(List.of(new NodePackStore.Part("uuid-a", 0, "_0.seg", bytes.length, () -> new ByteArrayInputStream(bytes))));
        for (int pass = 0; pass < 3; pass++) {
            clock.addAndGet(NodePackWriter.COLLECTABLE_AFTER_MILLIS);
            assertEquals(0, collector.run().deleted());
        }
        assertTrue(packs.readHeader(1L, 0L).isPresent());
    }

    public void testAfterARestartTheOldEpochIsFoundFromTheLedgerAndCollectedToTheEnd() throws Exception {
        // The old process: three packs, the ledger recording only how far its last pass got.
        final NodePackWriter old = writer(7L);
        final NodePackCollector oldCollector = collector();
        oldCollector.follow(old);
        final List<NodePackWriter.Landed> first = old.add(List.of(part("alpha", "uuid-a", 0, "_0.seg")));
        oldCollector.run();
        final List<NodePackWriter.Landed> second = old.add(List.of(part("alpha", "uuid-a", 0, "_1.seg")));
        old.add(List.of(part("alpha", "uuid-a", 0, "_2.seg")));
        manifest("alpha", "uuid-a", 0, List.of(first.get(0), second.get(0)));
        assertEquals("the old process recorded one pack", 1L, ledger.read().epoch(7L).orElseThrow().high());

        // The new process starts its own epoch; its collector finds the old one's packs by name.
        final NodePackWriter fresh = writer(8L);
        final NodePackCollector collector = collector();
        collector.follow(fresh);
        store.reset();
        final NodePackCollector.Pass found = collector.run();
        assertEquals("the recorded pack, and the two written after it", 3, found.discovered());
        assertEquals("no listing", 0L, store.listings());

        // The new process writes too, so its epoch is in the ledger and the old one is no longer the newest.
        final List<NodePackWriter.Landed> fresher = fresh.add(List.of(part("alpha", "uuid-a", 0, "_3.seg")));
        manifest("alpha", "uuid-a", 0, List.of(fresher.get(0)));
        clock.addAndGet(NodePackWriter.COLLECTABLE_AFTER_MILLIS);
        collector.run();
        clock.addAndGet(MINUTE);
        assertEquals(3, collector.run().deleted());
        for (long ordinal = 0; ordinal < 3; ordinal++) {
            assertTrue(packs.readHeader(7L, ordinal).isEmpty());
        }
        final NodePackLedger.State state = ledger.read();
        assertTrue("a finished old epoch leaves the ledger: " + state, state.epoch(7L).isEmpty());
        assertTrue(state.epoch(8L).isPresent());
        assertTrue("the new process's pack is still named", packs.readHeader(8L, 0L).isPresent());
    }

    public void testANodePackLocationIsWhatTheCollectorLooksFor() {
        final FileLocation file = FileLocation.nodePacked("node-1", 5L, 3L, 100L, 7L);
        assertEquals(FileLocation.nodePacked("node-1", 5L, 3L, 0L, 0L).referencedBlob(), file.referencedBlob());
    }
}
