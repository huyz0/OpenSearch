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
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.reconcile.GarbageCollector;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Closing the gaps phase 8 named for itself: §7's batched lease renewal, and a deployment-wide sweep.
 *
 * <p>Phase 8 measured 3 object-store operations per reconciliation tick per shard and observed that the
 * single write was a per-shard lease renewal — a node holding a hundred shards paying a hundred writes
 * per TTL to say one thing, that it was still there. This is that measurement acted on, and re-measured.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessBatchedLeaseTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-p8b")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private static NodeLease leaseFor(ServerlessNode node) {
        return new NodeLease(node.localNode().getId(), node.localNode().getEphemeralId(), "127.0.0.1:9300", Set.of("ingest"), 0L);
    }

    /** With batched liveness, a node that keeps its lease alive keeps every shard it holds. */
    public void testOneRenewalKeepsEveryShard() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final BlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL, true);
        assertTrue(plane.usesNodeLeaseLiveness());
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 4, MAPPING, null));

        try (ServerlessNode a = new ServerlessNode(nodeSettings("p8b-a")); ServerlessNode b = new ServerlessNode(nodeSettings("p8b-b"))) {
            a.start();
            b.start();
            plane.membership().renew(leaseFor(a));
            for (int shard = 0; shard < 4; shard++) {
                assertTrue(a.activateWriter(plane, "alpha", shard).isPresent());
            }
            assertEquals(4, a.reconciler().openShards().size());

            // Two TTLs pass. One renewal per tick covers all four shards.
            for (int i = 1; i <= 2; i++) {
                clock.set(1_000L + i * (TTL - 1_000L));
                assertTrue("a live node must keep its shards", a.heartbeat(plane).isEmpty());
            }
            for (int shard = 0; shard < 4; shard++) {
                assertTrue("B took a shard from a live node", b.activateWriter(plane, "alpha", shard).isEmpty());
            }
        }
    }

    /** And when the one lease lapses, every shard becomes acquirable — no per-head expiry involved. */
    public void testOneLapsedLeaseReleasesEveryShard() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final BlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL, true);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 3, MAPPING, null));

        try (ServerlessNode a = new ServerlessNode(nodeSettings("p8b-l1")); ServerlessNode b = new ServerlessNode(nodeSettings("p8b-l2"))) {
            a.start();
            b.start();
            plane.membership().renew(leaseFor(a));
            for (int shard = 0; shard < 3; shard++) {
                a.activateWriter(plane, "alpha", shard);
            }

            // A stops heartbeating entirely. One lease expiring frees three shards.
            clock.set(1_000L + TTL);
            plane.membership().renew(leaseFor(b));
            for (int shard = 0; shard < 3; shard++) {
                assertTrue(
                    "shard " + shard + " should be acquirable once A's node lease lapsed",
                    b.activateWriter(plane, "alpha", shard).isPresent()
                );
            }
            assertEquals("A must let go of all three on its next tick", 3, a.heartbeat(plane).size());
        }
    }

    /** A restarted node has a new ephemeral id, and must not inherit the dead process's claim. */
    public void testARestartedNodeDoesNotInheritTheOldProcessesClaim() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final BlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL, true);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode a = new ServerlessNode(nodeSettings("p8b-r"))) {
            a.start();
            plane.membership().renew(leaseFor(a));
            assertTrue(plane.activate("alpha", 0, a.localNode().getId(), a.localNode().getEphemeralId()).acquired());

            // Same node id, different process. Its lease is live, but it is not the same holder.
            final NodeLease restarted = new NodeLease(
                a.localNode().getId(),
                "a-different-ephemeral-id",
                "127.0.0.1:9300",
                Set.of("ingest"),
                0L
            );
            plane.membership().renew(restarted);

            assertTrue(
                "a restarted process must be able to re-take a shard its predecessor held",
                plane.activate("alpha", 0, a.localNode().getId(), "a-different-ephemeral-id").acquired()
            );
        }
    }

    /** The deployment-wide sweep, and expired-lease tidying. */
    public void testSweepCollectsEveryShardAndExpiredLeases() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final BlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        final GarbageCollector gc = new GarbageCollector(store, BlobPath.cleanPath());
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 2, MAPPING, null));
        plane.createIndex(new IndexDescriptor("beta", "uuid-beta-000000000", 1, MAPPING, null));

        // A sweep over a deployment with nothing published must delete nothing and must not throw.
        assertTrue(gc.collectAll(plane).isEmpty());

        try (ServerlessNode a = new ServerlessNode(nodeSettings("p8b-sweep"))) {
            a.start();
            final ShardId shardId = a.activateWriter(plane, "alpha", 0).orElseThrow();
            ShardOps.indexDoc(a.reconciler().shard(shardId), "1", "{\"msg\":\"kept\"}");
            a.reconciler().shard(shardId).refresh("p8b");
            final var manifest = a.publishShard(shardId, plane.heads().read("alpha", 0).orElseThrow().term());

            final Map<String, List<String>> swept = gc.collectAll(plane);
            logger.info("phase 8 sweep: {} shards had orphans across 2 indices", swept.size());
            for (Map.Entry<String, String> file : manifest.files().entrySet()) {
                assertTrue(
                    "the sweep deleted a file the live commit depends on",
                    store.blobContainer(
                        org.opensearch.serverless.metadata.RegisterMap.shardData(BlobPath.cleanPath(), "alpha", 0).add(file.getValue())
                    ).blobExists(file.getKey())
                );
            }
        }

        // Expired leases are tidied; live ones are not.
        plane.membership().renew(new NodeLease("dead-node", "eph", "127.0.0.1:9300", Set.of("ingest"), 0L));
        assertTrue("a live lease must survive collection", gc.collectExpiredLeases(plane, clock.get()).isEmpty());
        clock.set(1_000L + TTL);
        assertEquals(List.of("dead-node"), gc.collectExpiredLeases(plane, clock.get()));
        assertTrue(plane.membership().read("dead-node").isEmpty());
    }

    /** Re-measure: the write per shard should be gone, and the saving should grow with shard count. */
    public void testBatchingRemovesThePerShardWrite() throws Exception {
        final int shards = 4;
        final long[] perShard = measureOps(false, shards);
        final long[] batched = measureOps(true, shards);

        logger.info(
            "phase 8 re-measurement at {} shards -- per-shard: {} reads + {} writes; batched: {} reads + {} writes (per tick)",
            shards,
            perShard[0],
            perShard[1],
            batched[0],
            batched[1]
        );

        // The specific claim: writes stop scaling with shard count. Reads do not, and are not claimed to.
        assertEquals("without batching a node writes once per shard it holds", shards, perShard[1]);
        assertEquals("with batching a node writes once, however many shards it holds", 1L, batched[1]);
        assertTrue("the total should fall too", batched[0] + batched[1] < perShard[0] + perShard[1]);
    }

    /**
     * @return reads and writes per tick, in that order
     */
    private long[] measureOps(boolean batched, int shards) throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final CountingBlobStore counter = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(counter, BlobPath.cleanPath(), clock::get, TTL, batched);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", shards, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("p8b-m-" + batched))) {
            node.start();
            plane.membership().renew(leaseFor(node));
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            for (int shard = 0; shard < shards; shard++) {
                loop.want("alpha", shard);
            }
            loop.tick(clock.get());     // acquiring tick, not measured
            assertEquals("the measurement is meaningless unless the shards were acquired", shards, node.reconciler().openShards().size());

            counter.reset();
            final int passes = 10;
            for (int i = 0; i < passes; i++) {
                clock.set(clock.get() + 1_000L);
                loop.tick(clock.get());
            }
            return new long[] { counter.reads() / passes, counter.writes() / passes };
        }
    }
}
