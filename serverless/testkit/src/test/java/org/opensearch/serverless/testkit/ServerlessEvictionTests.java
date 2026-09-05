/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A node at its shard cap makes room rather than refusing forever.
 *
 * <p>The cap exists to bound what a node holds, not to decide <em>which</em> shards it holds. Before this,
 * a node that reached it served whatever it happened to acquire first for as long as it ran — the working
 * set frozen at whatever arrived earliest, which is the opposite of what a demand-driven node is for. It
 * would refuse new shards while holding ones nobody had touched for minutes, waiting on an idle sweep that
 * runs on its own schedule.
 *
 * <p><b>The grace period is the interesting half.</b> Evicting the least recently used shard
 * unconditionally means a node holding one more hot shard than it has room for evicts one to serve another
 * on every request, and spends its life opening and closing shards instead of answering. So a full node
 * whose shards are all in use still refuses — and that refusal is a real capacity signal rather than
 * thrash. Both halves are tested, because only having the first would be a node that thrashes and only
 * having the second is where this started.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessEvictionTests extends OpenSearchTestCase {

    private static final long TTL = 300_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-eviction")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** At the cap, the shard nobody has touched is the one that goes. */
    public void testAFullNodeEvictsTheLeastRecentlyUsedShard() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        for (String index : List.of("first", "second", "third")) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index + "-0000000000".substring(index.length()), 1, MAPPING, null));
        }

        try (ServerlessNode node = new ServerlessNode(nodeSettings("evict-lru"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true)
                .setMaxShardsHeld(2)
                .setEvictAfterMillis(30_000L);

            // Two shards, filling the node.
            loop.activateOnDemand(List.of(Map.entry("first", 0), Map.entry("second", 0)));
            assertEquals("the node must be full", 2, node.reconciler().openShards().size());

            // "first" is used; "second" is left alone. Both are then old enough to evict, so the choice
            // between them is the thing being tested rather than a side effect of timing.
            clock.addAndGet(60_000L);
            node.markUsed(shard(node, "first"));

            loop.activateOnDemand(List.of(Map.entry("third", 0)));

            assertEquals("still at the cap, not over it", 2, node.reconciler().openShards().size());
            assertTrue("the new shard must have been taken, held=" + node.reconciler().openShards(), holds(node, "third"));
            assertTrue("the recently used one must have been kept", holds(node, "first"));
            assertFalse("and the least recently used one given up", holds(node, "second"));
        }
    }

    /** A node whose shards are all in use refuses, which is a capacity signal rather than thrash. */
    public void testAFullNodeOfBusyShardsRefusesRatherThanThrashing() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        for (String index : List.of("hot-a", "hot-b", "wanted")) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index + "-000000000".substring(index.length() - 1), 1, MAPPING, null));
        }

        try (ServerlessNode node = new ServerlessNode(nodeSettings("evict-busy"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true)
                .setMaxShardsHeld(2)
                .setEvictAfterMillis(30_000L);

            loop.activateOnDemand(List.of(Map.entry("hot-a", 0), Map.entry("hot-b", 0)));
            assertEquals(2, node.reconciler().openShards().size());

            // Both touched just now, so neither is old enough to give up.
            clock.addAndGet(5_000L);
            node.markUsed(shard(node, "hot-a"));
            node.markUsed(shard(node, "hot-b"));

            final var taken = loop.activateOnDemand(List.of(Map.entry("wanted", 0)));
            assertTrue("a node whose shards are all in use must refuse: " + taken, taken.isEmpty());
            assertEquals("and must not have evicted anything", 2, node.reconciler().openShards().size());
            assertTrue(holds(node, "hot-a"));
            assertTrue(holds(node, "hot-b"));
        }
    }

    /** With eviction turned off, a full node behaves as it did before: it refuses. */
    public void testEvictionCanBeTurnedOff() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        for (String index : List.of("kept-a", "kept-b", "denied")) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index + "-00000000".substring(index.length() - 2), 1, MAPPING, null));
        }

        try (ServerlessNode node = new ServerlessNode(nodeSettings("evict-off"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true)
                .setMaxShardsHeld(2)
                .setEvictAfterMillis(0L);

            loop.activateOnDemand(List.of(Map.entry("kept-a", 0), Map.entry("kept-b", 0)));
            clock.addAndGet(600_000L);   // ancient, and still not evicted

            assertTrue("with eviction off a full node refuses", loop.activateOnDemand(List.of(Map.entry("denied", 0))).isEmpty());
            assertEquals(2, node.reconciler().openShards().size());
        }
    }

    /**
     * A burst of new demand clears room for more than the one shard that triggered it, so the next several
     * arrivals do not each pay their own full eviction scan.
     *
     * <p>This is the property headroom exists for, asserted directly rather than inferred from timing: a
     * node at its cap, asked for one more shard while every held shard is idle, must give up more than
     * one — down to the floor headroom sets — and the arrivals right after must find room already there
     * with nothing more evicted.
     */
    public void testMakeRoomClearsHeadroomNotJustOneSlot() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final List<String> resident = List.of("r0", "r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8", "r9");
        for (String index : resident) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index, 1, MAPPING, null));
        }
        for (String index : List.of("a1", "a2", "a3")) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index, 1, MAPPING, null));
        }

        try (ServerlessNode node = new ServerlessNode(nodeSettings("evict-headroom"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true)
                .setMaxShardsHeld(10)
                .setEvictAfterMillis(30_000L)
                .setEvictHeadroomFraction(0.3);   // floor(10 * 0.3) = 3

            for (String index : resident) {
                loop.activateOnDemand(List.of(Map.entry(index, 0)));
            }
            assertEquals("the node must be full", 10, node.reconciler().openShards().size());
            clock.addAndGet(60_000L);   // every resident shard is now old enough to evict; none is touched

            final var takenFirst = loop.activateOnDemand(List.of(Map.entry("a1", 0)));
            assertEquals("the triggering shard must have been taken", 1, takenFirst.size());
            assertEquals(
                "one pass must clear the floor (10 - 3 headroom + 1 new = 8), not merely one slot",
                8,
                node.reconciler().openShards().size()
            );

            // Two more arrivals, within the headroom already cleared, must find room without evicting
            // anything else -- this is the whole benefit headroom exists to provide.
            final int heldBeforeMore = node.reconciler().openShards().size();
            final var takenSecond = loop.activateOnDemand(List.of(Map.entry("a2", 0), Map.entry("a3", 0)));
            assertEquals("both further arrivals must have been taken from existing headroom", 2, takenSecond.size());
            assertEquals(
                "no further eviction should have been needed: held grew by exactly the new arrivals",
                heldBeforeMore + 2,
                node.reconciler().openShards().size()
            );
        }
    }

    /** A headroom fraction of zero is exactly the original, one-slot-at-a-time behaviour. */
    public void testZeroHeadroomEvictsExactlyOneSlot() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        for (String index : List.of("first", "second", "third")) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index, 1, MAPPING, null));
        }

        try (ServerlessNode node = new ServerlessNode(nodeSettings("evict-zero-headroom"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true)
                .setMaxShardsHeld(2)
                .setEvictAfterMillis(30_000L)
                .setEvictHeadroomFraction(0.0);

            loop.activateOnDemand(List.of(Map.entry("first", 0), Map.entry("second", 0)));
            clock.addAndGet(60_000L);

            loop.activateOnDemand(List.of(Map.entry("third", 0)));
            assertEquals("exactly one slot's worth must have been cleared, still at the cap", 2, node.reconciler().openShards().size());
        }
    }

    /** A pinned index's shard survives eviction under cap pressure, even as the oldest, least-used one. */
    public void testAPinnedIndexIsNeverEvictedForRoom() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        for (String index : List.of("pinned", "ordinary", "newcomer")) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index, 1, MAPPING, null));
        }

        try (ServerlessNode node = new ServerlessNode(nodeSettings("evict-pinned"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true)
                .setMaxShardsHeld(2)
                .setEvictAfterMillis(30_000L)
                .setEvictHeadroomFraction(0.0);
            loop.pin("pinned");

            loop.activateOnDemand(List.of(Map.entry("pinned", 0), Map.entry("ordinary", 0)));
            assertEquals(2, node.reconciler().openShards().size());
            clock.addAndGet(60_000L);   // both are old enough to evict on age alone

            final var taken = loop.activateOnDemand(List.of(Map.entry("newcomer", 0)));
            assertTrue("the newcomer must have been taken: " + taken, holds(node, "newcomer"));
            assertTrue("the pinned index must have survived, being the only evictable candidate otherwise", holds(node, "pinned"));
            assertFalse("the ordinary index must have been the one given up", holds(node, "ordinary"));
        }
    }

    /** A pinned index's shard is exempt from idle release too, not only from cap pressure. */
    public void testAPinnedIndexIsNeverReleasedForIdleness() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("pinned", "uuid-pinned", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("ordinary", "uuid-ordinary", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("evict-pinned-idle"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setIdleAfterMillis(30_000L);
            loop.pin("pinned");
            loop.want("pinned", 0);
            loop.want("ordinary", 0);
            loop.tick(clock.get());
            assertEquals(2, node.reconciler().openShards().size());

            clock.addAndGet(60_000L);
            final var released = loop.releaseIdle(clock.get());
            assertTrue("only the ordinary index's shard must have been released: " + released, holds(node, "pinned"));
            assertFalse(holds(node, "ordinary"));
        }
    }

    /** Unpinning makes a shard an ordinary candidate again -- the exemption is not permanent. */
    public void testUnpinningMakesAShardEvictableAgain() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        for (String index : List.of("was-pinned", "ordinary", "newcomer")) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index, 1, MAPPING, null));
        }

        try (ServerlessNode node = new ServerlessNode(nodeSettings("evict-unpin"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true)
                .setMaxShardsHeld(2)
                .setEvictAfterMillis(30_000L)
                .setEvictHeadroomFraction(0.0);
            loop.pin("was-pinned");

            loop.activateOnDemand(List.of(Map.entry("was-pinned", 0), Map.entry("ordinary", 0)));
            clock.addAndGet(60_000L);
            node.markUsed(shard(node, "ordinary"));   // ordinary is the recently-used one now

            assertFalse("isPinned must report the current state", loop.isPinned("ordinary"));
            assertTrue(loop.isPinned("was-pinned"));
            loop.unpin("was-pinned");
            assertFalse(loop.isPinned("was-pinned"));

            final var taken = loop.activateOnDemand(List.of(Map.entry("newcomer", 0)));
            assertTrue("the newcomer must have been taken: " + taken, holds(node, "newcomer"));
            assertFalse("the unpinned index must now be evictable, and is the only idle candidate", holds(node, "was-pinned"));
            assertTrue("the recently-used one must survive on its own merits", holds(node, "ordinary"));
        }
    }

    private static boolean holds(ServerlessNode node, String index) {
        return node.reconciler().openShards().stream().anyMatch(s -> s.getIndexName().equals(index));
    }

    private static ShardId shard(ServerlessNode node, String index) {
        return node.reconciler()
            .openShards()
            .stream()
            .filter(s -> s.getIndexName().equals(index))
            .findFirst()
            .orElseThrow(() -> new AssertionError("not holding " + index));
    }
}
