/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.DescriptorStore;
import org.opensearch.serverless.metadata.DigestRollups;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What a crashed node leaves behind is cleaned up, and a fully published shard is given back rather than taken.
 *
 * <p>A crash leaves every shard the node held named to it: the head, the node's claim, and a rollup mark that keeps the
 * shard from ever being ruled out of a wide search. The only cleanup was a takeover -- open, replay, hold -- which put
 * a dead node's mostly idle shards on survivors near their cap, and the marks of shards nobody took stayed for good:
 * a fleet that had crashed a few times could not rule 47,000 indices out of a search for next month.
 *
 * <p>Each node has its own metadata plane over one shared store, as separate processes would.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessCrashCleanupTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-crash-cleanup")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private static MetadataPlane plane(BlobStore store, AtomicLong clock) {
        return new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
    }

    private static IndexDescriptor descriptor(String index) {
        return new IndexDescriptor(index, "uuid-" + index + "-000000000000".substring(index.length()), 1, MAPPING, null);
    }

    /** The term the rollups mark the shard owned at; zero if none. */
    private static long markedTerm(MetadataPlane plane, String index) throws Exception {
        final Map<String, DigestRollups.Entry> group = plane.rollups().readGroup(DigestRollups.group(index), new DescriptorStore.Reads() {
            @Override
            public <T> List<T> runAll(List<Callable<T>> tasks) throws InterruptedException {
                final List<T> out = new ArrayList<>();
                for (Callable<T> task : tasks) {
                    try {
                        out.add(task.call());
                    } catch (Exception e) {
                        out.add(null);
                    }
                }
                return out;
            }
        });
        final DigestRollups.Entry entry = group.get(index);
        return entry == null ? 0L : entry.states().get(0).ownerTerm();
    }

    /**
     * A dead node's shards, written to by it before it died: one published, one with its last write only in the log.
     * The node never releases anything; its lease runs out on the shared clock.
     */
    private void crashWithOnePublishedAndOneNot(BlobStore store, AtomicLong clock, ServerlessNode dying) throws Exception {
        final MetadataPlane plane = plane(store, clock);
        dying.start();
        dying.setMetadataPlane(plane);
        dying.renewLease(plane);
        final BackgroundReconciler loop = new BackgroundReconciler(dying, plane).setDemandDrivenActivation(true);
        loop.activateOnDemand(List.of(Map.entry("published", 0), Map.entry("unpublished", 0)));
        for (ShardId shardId : dying.reconciler().openShards()) {
            dying.index(shardId, "first", "{\"msg\":\"published\",\"n\":1}");
        }
        loop.publishAll();
        for (ShardId shardId : dying.reconciler().openShards()) {
            if (shardId.getIndexName().equals("unpublished")) {
                dying.index(shardId, "second", "{\"msg\":\"only in the log\",\"n\":2}");
            }
        }
        assertTrue("marked while held", markedTerm(plane, "published") > 0L);
    }

    /** A survivor that hears the departure gives back the published shard -- head, mark and claim -- and takes only the other. */
    public void testADeadNodesPublishedShardIsGivenBackAndOnlyTheOtherIsTaken() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final BlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane setup = plane(store, clock);
        setup.createIndex(descriptor("published"));
        setup.createIndex(descriptor("unpublished"));
        try (
            ServerlessNode dying = new ServerlessNode(nodeSettings("cleanup-dying"));
            ServerlessNode survivor = new ServerlessNode(nodeSettings("cleanup-survivor"))
        ) {
            crashWithOnePublishedAndOneNot(store, clock, dying);
            final String dead = dying.localNode().getId();
            clock.addAndGet(2 * TTL);

            final MetadataPlane plane = plane(store, clock);
            survivor.start();
            survivor.setMetadataPlane(plane);
            survivor.renewLease(plane);
            plane.membership().refresh();
            final BackgroundReconciler loop = new BackgroundReconciler(survivor, plane).setDemandDrivenActivation(true);
            loop.takeOverFrom(dead);

            assertBusy(() -> assertEquals(1, survivor.reconciler().openShards().size()), 30, TimeUnit.SECONDS);
            final ShardId taken = survivor.reconciler().openShards().iterator().next();
            assertEquals("only the shard with unpublished writes is taken", "unpublished", taken.getIndexName());
            assertTrue("and its log is replayed", survivor.get(taken, "second").found());

            final var head = plane.heads().read("published", 0).orElseThrow();
            assertNull("the published shard is given back, not taken", head.ownerNodeId());
            assertEquals("its mark is cleared", 0L, markedTerm(plane, "published"));
            assertFalse("and the dead node's claim on it forgotten", plane.claimsOf(dead).contains(Map.entry("published", 0)));
            assertEquals("the survivor counts it", 1L, loop.settledClean());
        }
    }

    /** With no departure heard -- the whole fleet crashed -- the janitor cleans up the same way, later. */
    public void testTheJanitorCleansUpAfterAFleetNobodySawDie() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final BlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane setup = plane(store, clock);
        setup.createIndex(descriptor("published"));
        setup.createIndex(descriptor("unpublished"));
        try (
            ServerlessNode dying = new ServerlessNode(nodeSettings("janitor-dying"));
            ServerlessNode later = new ServerlessNode(nodeSettings("janitor-later"))
        ) {
            crashWithOnePublishedAndOneNot(store, clock, dying);
            final String dead = dying.localNode().getId();
            // Long after: past the window a live departure is handled in.
            clock.addAndGet(10 * TTL);

            final MetadataPlane plane = plane(store, clock);
            later.start();
            later.setMetadataPlane(plane);
            later.renewLease(plane);
            plane.membership().refresh();
            final BackgroundReconciler loop = new BackgroundReconciler(later, plane).setDemandDrivenActivation(true);
            final BackgroundReconciler.JanitorPass pass = loop.sweepAbandoned(100, 8);
            assertEquals("the published shard is given back: " + pass, 1, pass.released());
            assertEquals("the other is taken to be replayed: " + pass, 1, pass.replays());

            assertBusy(() -> assertEquals(1, later.reconciler().openShards().size()), 30, TimeUnit.SECONDS);
            final ShardId taken = later.reconciler().openShards().iterator().next();
            assertEquals("unpublished", taken.getIndexName());
            assertTrue("what the dead node acknowledged is there", later.get(taken, "second").found());
            assertNull(plane.heads().read("published", 0).orElseThrow().ownerNodeId());
            assertEquals(0L, markedTerm(plane, "published"));
            assertTrue("the dead node's claims are all gone", plane.claimsOf(dead).isEmpty());
        }
    }

    /**
     * A mark left under a head its owner gave back -- the owner released and could not clear it -- is cleared by the
     * janitor once everything is published, and only then.
     */
    public void testTheJanitorClearsAMarkLeftUnderAReleasedHead() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final BlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = plane(store, clock);
        plane.createIndex(descriptor("leaked"));
        try (ServerlessNode node = new ServerlessNode(nodeSettings("janitor-leaked"))) {
            node.start();
            node.setMetadataPlane(plane);
            node.renewLease(plane);
            plane.membership().refresh();
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true);
            loop.activateOnDemand(List.of(Map.entry("leaked", 0)));
            final ShardId shardId = node.reconciler().openShards().iterator().next();
            node.index(shardId, "doc", "{\"msg\":\"x\",\"n\":1}");
            loop.publishAll();
            final long term = plane.heads().read("leaked", 0).orElseThrow().term();
            // The head given back, the mark not cleared: what a failed clear leaves.
            node.reconciler().releaseShard(shardId, "test: released without clearing its mark");
            assertTrue(plane.heads().release("leaked", 0, node.localNode().getId(), term));
            assertEquals(term, markedTerm(plane, "leaked"));

            // Every bucket of every group, as passes would over time.
            final int slots = plane.rollups().groups().size() * DigestRollups.BUCKETS;
            int released = 0;
            for (int i = 0; i < slots; i++) {
                released += loop.sweepAbandoned(100, 8).released();
            }
            assertEquals("the leaked mark is cleared once", 1, released);
            assertEquals(0L, markedTerm(plane, "leaked"));
        }
    }
}
