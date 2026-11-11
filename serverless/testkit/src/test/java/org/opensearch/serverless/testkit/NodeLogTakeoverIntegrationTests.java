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
import org.opensearch.serverless.metadata.Acquisition;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.ShardHead;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/** With one write log per node: a crashed writer's unpublished writes reach whoever takes its shards, and stay there. */
public class NodeLogTakeoverIntegrationTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int SHARDS = 4;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-node-log-takeover")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.window_millis", 20)
            .build();
    }

    private static BackgroundReconciler.TickResult takeAll(ServerlessNode node, MetadataPlane plane, AtomicLong clock) throws Exception {
        node.renewLease(plane);
        plane.membership().refresh();
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (int shard = 0; shard < SHARDS; shard++) {
            loop.want("alpha", shard);
        }
        final BackgroundReconciler.TickResult pass = loop.tick(clock.get());
        assertEquals("one pass opens every shard it wants", SHARDS, node.reconciler().openShards().size());
        return pass;
    }

    /** Opens every shard, writes to each, publishes, then writes one more to each that is never published. */
    private static Map<Integer, String> writeAndLeaveUnpublished(ServerlessNode node, MetadataPlane plane, AtomicLong clock, String tag)
        throws Exception {
        takeAll(node, plane, clock);
        final List<ShardId> held = new ArrayList<>(node.reconciler().openShards());
        for (int i = 0; i < 12; i++) {
            node.index(held.get(i % SHARDS), tag + "-published-" + i, "{\"msg\":\"p\",\"n\":" + i + "}");
        }
        for (ShardId shardId : held) {
            node.reconciler().publish(shardId, node.reconciler().shard(shardId).getOperationPrimaryTerm());
        }
        final Map<Integer, String> unpublished = new HashMap<>();
        for (ShardId shardId : held) {
            final String id = tag + "-unpublished-" + shardId.id();
            node.index(shardId, id, "{\"msg\":\"u\",\"n\":" + shardId.id() + "}");
            unpublished.put(shardId.id(), id);
        }
        return unpublished;
    }

    private static void assertFound(ServerlessNode node, Map<Integer, String> ids) throws Exception {
        for (ShardId shardId : node.reconciler().openShards()) {
            assertTrue("the unpublished write to " + shardId + " was replayed", node.get(shardId, ids.get(shardId.id())).found());
        }
    }

    private static void closeQuietly(ServerlessNode node) {
        try {
            node.close();
        } catch (Exception e) {
            // It lost every shard it held; closing it may report that.
        }
    }

    public void testACrashedWritersUnpublishedWritesSurviveTwoTakeovers() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", SHARDS, MAPPING, null));

        final ServerlessNode first = new ServerlessNode(nodeSettings("node-log-first"));
        final ServerlessNode second = new ServerlessNode(nodeSettings("node-log-second"));
        try {
            first.start();
            first.setMetadataPlane(plane);
            final Map<Integer, String> fromFirst = writeAndLeaveUnpublished(first, plane, clock, "first");
            assertTrue("the first writer logged through its node log", first.nodeLogWriter().committer().blobs() > 0);
            assertFalse(
                "and its lease names the epochs a successor must fence",
                plane.membership().read(first.localNode().getId()).orElseThrow().nodeLog().isEmpty()
            );

            // The first writer never lets go; its lease runs out on the shared clock.
            clock.addAndGet(2 * TTL);
            second.start();
            second.setMetadataPlane(plane);
            assertEquals(
                "what the open replayed it published before serving, and the pass that took it does not publish it again",
                Set.of(),
                takeAll(second, plane, clock).published()
            );
            assertFound(second, fromFirst);
            for (ShardId shardId : second.reconciler().openShards()) {
                second.index(shardId, "second-" + shardId.id(), "{\"msg\":\"s\",\"n\":" + shardId.id() + "}");
            }

            // The second writer crashes too. What it replayed from the first was published before it served, so a third
            // finds it without the first's log; what the second wrote itself it replays from the second's.
            clock.addAndGet(2 * TTL);
            try (ServerlessNode third = new ServerlessNode(nodeSettings("node-log-third"))) {
                third.start();
                third.setMetadataPlane(plane);
                takeAll(third, plane, clock);
                assertFound(third, fromFirst);
                for (ShardId shardId : third.reconciler().openShards()) {
                    assertTrue(third.get(shardId, "second-" + shardId.id()).found());
                }
            }
        } finally {
            closeQuietly(second);
            closeQuietly(first);
        }
    }

    public void testShardsLetGoOfCleanlyNeedNoNodeLog() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", SHARDS, MAPPING, null));
        final Map<Integer, String> written;
        try (ServerlessNode first = new ServerlessNode(nodeSettings("node-log-clean-first"))) {
            first.start();
            first.setMetadataPlane(plane);
            written = writeAndLeaveUnpublished(first, plane, clock, "clean");
            final BackgroundReconciler loop = new BackgroundReconciler(first, plane);
            loop.setIdleAfterMillis(1);
            for (int shard = 0; shard < SHARDS; shard++) {
                loop.stopWanting("alpha", shard);
            }
            loop.releaseIdle(System.currentTimeMillis() + 60_000L);
            assertTrue("given back", first.reconciler().openShards().isEmpty());
            first.renewLease(plane);
            assertEquals(
                "a release publishes, so nothing in its log is still needed",
                List.of(),
                plane.membership()
                    .read(first.localNode().getId())
                    .orElseThrow()
                    .nodeLog()
                    .stream()
                    .filter(e -> e.trimPoint() < e.landed())
                    .toList()
            );
        }
        try (ServerlessNode second = new ServerlessNode(nodeSettings("node-log-clean-second"))) {
            second.start();
            second.setMetadataPlane(plane);
            takeAll(second, plane, clock);
            assertFound(second, written);
        }
    }

    public void testASurvivorSettlingADeadWritersShardsLeavesThoseItsLogHoldsForAReplay() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", SHARDS, MAPPING, null));
        final ServerlessNode first = new ServerlessNode(nodeSettings("node-log-settle-first"));
        try {
            first.start();
            first.setMetadataPlane(plane);
            final Map<Integer, String> unpublished = writeAndLeaveUnpublished(first, plane, clock, "settle");
            final String dead = first.localNode().getId();

            clock.addAndGet(2 * TTL);
            for (int shard = 0; shard < SHARDS; shard++) {
                assertEquals(
                    "the shard's own log is published, but the dead writer's holds what it never published",
                    MetadataPlane.Settled.NEEDS_REPLAY,
                    plane.settleAbandoned("alpha", shard, dead)
                );
                final ShardHead head = plane.heads().read("alpha", shard).orElseThrow();
                assertNull("given back", head.ownerNodeId());
                assertEquals("naming whose log to read", List.of(dead), head.writers());
            }

            // Whoever takes them next reads the dead writer's log, fenced, since it is not live.
            try (ServerlessNode second = new ServerlessNode(nodeSettings("node-log-settle-second"))) {
                second.start();
                second.setMetadataPlane(plane);
                takeAll(second, plane, clock);
                assertFound(second, unpublished);
            }
        } finally {
            closeQuietly(first);
        }
    }

    public void testASettledShardWhoseWritesWerePublishedAndTrimmedIsReleasedClean() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", SHARDS, MAPPING, null));
        final ServerlessNode first = new ServerlessNode(nodeSettings("node-log-settle-clean"));
        try {
            first.start();
            first.setMetadataPlane(plane);
            writeAndLeaveUnpublished(first, plane, clock, "trimmed");
            for (ShardId shardId : new ArrayList<>(first.reconciler().openShards())) {
                first.reconciler().publish(shardId, first.reconciler().shard(shardId).getOperationPrimaryTerm());
            }
            // The renewal records the trim point the publishes moved; nothing below it is needed.
            first.renewLease(plane);
            final String dead = first.localNode().getId();
            clock.addAndGet(2 * TTL);
            for (int shard = 0; shard < SHARDS; shard++) {
                assertEquals(MetadataPlane.Settled.RELEASED_CLEAN, plane.settleAbandoned("alpha", shard, dead));
            }
        } finally {
            closeQuietly(first);
        }
    }

    public void testASuccessorThatDiesBeforeItPublishesLeavesTheNextOneEveryLogToRead() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", SHARDS, MAPPING, null));
        final ServerlessNode first = new ServerlessNode(nodeSettings("node-log-chain-first"));
        try {
            first.start();
            first.setMetadataPlane(plane);
            final Map<Integer, String> unpublished = writeAndLeaveUnpublished(first, plane, clock, "chain");

            // A second node takes every head and dies before it opens any shard, so it replays and publishes nothing.
            clock.addAndGet(2 * TTL);
            for (int shard = 0; shard < SHARDS; shard++) {
                final Acquisition taken = plane.heads()
                    .acquire("alpha", shard, "node-log-chain-second", "eph-second", "uuid-alpha-00000000");
                assertTrue(taken.acquired());
                assertEquals(List.of(first.localNode().getId()), taken.head().writers());
            }

            clock.addAndGet(2 * TTL);
            try (ServerlessNode third = new ServerlessNode(nodeSettings("node-log-chain-third"))) {
                third.start();
                third.setMetadataPlane(plane);
                takeAll(third, plane, clock);
                assertFound(third, unpublished);
                for (int shard = 0; shard < SHARDS; shard++) {
                    assertEquals(
                        "replayed and published, the head names no other log",
                        List.of(),
                        plane.heads().read("alpha", shard).orElseThrow().writers()
                    );
                }
            }
        } finally {
            closeQuietly(first);
        }
    }

    public void testAHeadNamesEveryLogItMayNeedUntilItsOwnerHasPublished() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        final String uuid = "uuid-alpha-00000000";
        final Acquisition a = plane.heads().acquire("alpha", 0, "a", "eph-a", uuid);
        assertEquals("a fresh head names no log", List.of(), a.head().writers());
        assertTrue(plane.heads().release("alpha", 0, "a", a.head().term(), uuid, "eph-a"));
        assertEquals("given back, it names the releaser", List.of("a"), plane.heads().read("alpha", 0).orElseThrow().writers());

        final Acquisition b = plane.heads().acquire("alpha", 0, "b", "eph-b", uuid);
        assertTrue(b.acquired());
        assertEquals("taken, it goes on naming the log its owner has yet to publish", List.of("a"), b.head().writers());
        assertEquals("and a renewal keeps it", List.of("a"), plane.heads().renew("alpha", 0, "b").orElseThrow().writers());

        // b dies before it publishes: whoever takes the shard from it reads a's log as well as b's, oldest first.
        clock.addAndGet(2 * TTL);
        final Acquisition c = plane.heads().acquire("alpha", 0, "c", "eph-c", uuid);
        assertTrue(c.acquired());
        assertEquals("b", c.previousOwner());
        assertEquals(List.of("a", "b"), c.head().writers());

        assertTrue("once c has replayed and published, the head names none", plane.heads().clearWriters("alpha", 0, "c", c.head().term()));
        assertEquals(List.of(), plane.heads().read("alpha", 0).orElseThrow().writers());
        assertFalse("only the owner at its term clears them", plane.heads().clearWriters("alpha", 0, "b", c.head().term()));
        assertTrue(plane.heads().release("alpha", 0, "c", c.head().term(), uuid, "eph-c"));
        assertEquals(List.of("c"), plane.heads().read("alpha", 0).orElseThrow().writers());
        assertEquals(
            "it survives a round trip",
            List.of("c"),
            ShardHead.fromStream(plane.heads().read("alpha", 0).orElseThrow().toBytes().streamInput()).writers()
        );
    }
}
