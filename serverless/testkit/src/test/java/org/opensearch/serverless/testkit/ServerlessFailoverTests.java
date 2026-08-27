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
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.StaleWriterException;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Phase 6: activation, lease expiry, and what a zombie can and cannot do.
 *
 * <p>Failure detection here runs on the node that might be failing, not on one watching it. A node that
 * is paused, partitioned or dead simply stops heartbeating; its lease lapses and someone else acquires
 * by compare-and-swap. Nothing has to agree that it died, which is why there is no failure detector and
 * no quorum anywhere in this.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only. No durability claim for S3 or GCS until R11 runs.
 */
public class ServerlessFailoverTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, String roles) {
        final Settings.Builder b = Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-p6")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0");
        if (roles != null) {
            b.put("serverless.roles", roles);
        }
        return b.build();
    }

    private MetadataPlane planeOver(Path dir, AtomicLong clock) throws Exception {
        return new MetadataPlane(new FsBlobStore(1024, dir, false), BlobPath.cleanPath(), clock::get, TTL);
    }

    private MetadataPlane freshIndex(AtomicLong clock) throws Exception {
        final MetadataPlane plane = planeOver(createTempDir(), clock);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        return plane;
    }

    public void testActivationIsExclusiveAndTheLoserIsNotAnError() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock);

        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("p6-a", "ingest"));
            ServerlessNode b = new ServerlessNode(nodeSettings("p6-b", "ingest"))
        ) {
            a.start();
            b.start();

            assertTrue("A should have acquired the shard", a.activateWriter(plane, "alpha", 0).isPresent());

            // Losing is a routing instruction, not a failure.
            final Optional<ShardId> lost = b.activateWriter(plane, "alpha", 0);
            assertTrue("B must not acquire a shard A holds", lost.isEmpty());
            assertTrue("B must not have opened anything", b.reconciler().openShards().isEmpty());
            assertEquals(a.localNode().getId(), plane.heads().read("alpha", 0).orElseThrow().ownerNodeId());
        }
    }

    public void testHeartbeatKeepsOwnershipAcrossTheTtl() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock);

        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("p6-hb", "ingest"));
            ServerlessNode b = new ServerlessNode(nodeSettings("p6-hb2", "ingest"))
        ) {
            a.start();
            b.start();
            assertTrue(a.activateWriter(plane, "alpha", 0).isPresent());

            // Three TTLs pass, but A keeps heartbeating.
            for (int i = 1; i <= 3; i++) {
                clock.set(1_000L + i * (TTL - 1_000L));
                assertTrue("a live heartbeat must release nothing", a.heartbeat(plane).isEmpty());
            }

            assertTrue("B took a shard whose lease was being renewed", b.activateWriter(plane, "alpha", 0).isEmpty());
            assertEquals(1, a.reconciler().openShards().size());
        }
    }

    public void testAStoppedHeartbeatLosesTheShardAndTheNodeNoticesAndLetsGo() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock);

        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("p6-stop", "ingest"));
            ServerlessNode b = new ServerlessNode(nodeSettings("p6-take", "ingest"))
        ) {
            a.start();
            b.start();
            final ShardId shardId = a.activateWriter(plane, "alpha", 0).orElseThrow();

            // A stops heartbeating -- paused, partitioned, or simply busy. The clock does the rest.
            clock.set(1_000L + TTL);
            assertTrue("B should take an expired shard", b.activateWriter(plane, "alpha", 0).isPresent());

            // A is still holding the shard open at this instant. It finds out on its next heartbeat.
            assertEquals(1, a.reconciler().openShards().size());
            assertEquals("A did not release the shard it lost", java.util.Set.of(shardId), a.heartbeat(plane));
            assertTrue(a.reconciler().openShards().isEmpty());
        }
    }

    /** kill -9: no release, no drain, no handover. Published data survives; the shard moves. */
    public void testKillNineLosesNoPublishedData() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock);

        final ServerlessNode a = new ServerlessNode(nodeSettings("p6-kill", "ingest"));
        a.start();
        final ShardId shardId = a.activateWriter(plane, "alpha", 0).orElseThrow();
        for (int i = 1; i <= 4; i++) {
            ShardOps.indexDoc(a.reconciler().shard(shardId), String.valueOf(i), "{\"msg\":\"durable\",\"n\":" + i + "}");
        }
        a.reconciler().shard(shardId).refresh("p6");
        a.publishShard(shardId, plane.heads().read("alpha", 0).orElseThrow().term());

        // The process dies. Nothing is released and nothing is told.
        a.close();

        clock.set(1_000L + TTL);
        try (ServerlessNode b = new ServerlessNode(nodeSettings("p6-kill2", "ingest"))) {
            b.start();
            final ShardId onB = b.activateWriter(plane, "alpha", 0).orElseThrow();
            assertEquals("published data did not survive an abrupt death", 4L, ShardOps.hits(b.searchService(), onB, "msg", "durable"));
        }
    }

    /**
     * The zombie. R9's actual scenario, which kill -9 cannot produce: a writer that is not dead, only
     * paused past its lease, and that wakes up still believing it owns the shard.
     */
    public void testAZombieWriterCannotCorruptTheShardItLost() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock);

        try (
            ServerlessNode zombie = new ServerlessNode(nodeSettings("p6-zombie", "ingest"));
            ServerlessNode successor = new ServerlessNode(nodeSettings("p6-successor", "ingest"))
        ) {
            zombie.start();
            successor.start();

            final ShardId onZombie = zombie.activateWriter(plane, "alpha", 0).orElseThrow();
            final long zombieTerm = plane.heads().read("alpha", 0).orElseThrow().term();
            ShardOps.indexDoc(zombie.reconciler().shard(onZombie), "1", "{\"msg\":\"before the pause\",\"n\":1}");
            zombie.reconciler().shard(onZombie).refresh("p6");
            zombie.publishShard(onZombie, zombieTerm);

            // The JVM pauses. No heartbeat, no release, no awareness of anything.
            clock.set(1_000L + TTL);
            final ShardId onSuccessor = successor.activateWriter(plane, "alpha", 0).orElseThrow();
            final long successorTerm = plane.heads().read("alpha", 0).orElseThrow().term();
            assertTrue("the successor must hold a higher term", successorTerm > zombieTerm);
            ShardOps.indexDoc(successor.reconciler().shard(onSuccessor), "2", "{\"msg\":\"after takeover\",\"n\":2}");
            successor.reconciler().shard(onSuccessor).refresh("p6");
            successor.publishShard(onSuccessor, successorTerm);

            // The zombie wakes. It has no idea it lost anything, and it keeps working.
            ShardOps.indexDoc(zombie.reconciler().shard(onZombie), "3", "{\"msg\":\"from the zombie\",\"n\":3}");
            zombie.reconciler().shard(onZombie).refresh("p6");

            // Its local writes are real -- to itself. This is the honest part: nothing stopped them.
            assertEquals(1L, ShardOps.hits(zombie.searchService(), onZombie, "msg", "zombie"));

            // But it cannot make anyone else believe them.
            final StaleWriterException fenced = expectThrows(StaleWriterException.class, () -> zombie.publishShard(onZombie, zombieTerm));
            assertTrue(fenced.getMessage().contains("term " + zombieTerm));

            // The successor's commit is untouched: still its term, and still what a reader would get.
            assertEquals(successorTerm, plane.segmentPublisher("alpha", 0).readManifest().orElseThrow().term());

            // And on its next heartbeat the zombie discovers the truth and lets go. Its unpublished
            // document goes with it -- lost, not corrupting, which is phase 4's WAL gap and not a fence
            // failure. Distinguishing those two is the whole point of the test.
            assertEquals(java.util.Set.of(onZombie), zombie.heartbeat(plane));
            assertTrue(zombie.reconciler().openShards().isEmpty());
        }

        // What survives is exactly what was published: the successor's view of the shard.
        try (ServerlessNode reader = new ServerlessNode(nodeSettings("p6-verify", "search"))) {
            reader.start();
            final ShardId shardId = reader.serveAsReader(plane, "alpha", 0);
            assertEquals("the pre-pause document must survive", 1L, ShardOps.hits(reader.searchService(), shardId, "msg", "before"));
            assertEquals("the successor's document must survive", 1L, ShardOps.hits(reader.searchService(), shardId, "msg", "takeover"));
            assertEquals(
                "the zombie's unpublished write must NOT be visible to anyone",
                0L,
                ShardOps.hits(reader.searchService(), shardId, "msg", "zombie")
            );
        }
    }
}
