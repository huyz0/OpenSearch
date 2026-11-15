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
import org.opensearch.serverless.reconcile.TickResult;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Durability. Publication happens on its own, and a write is durable before it is acknowledged.
 *
 * <p>Two gaps close here, and they are different. <b>Automatic publication</b>: until now {@code publishShard} was only
 * ever called by hand, so the loss window was not "since the last commit" — it was unbounded, and a node
 * could index for hours and lose all of it. <b>The write-ahead log</b>: even with automatic publication, everything
 * written since the last publish was lost, which is what a zombie test measured as the durability gap.
 *
 * <p>Runs against {@code FsBlobContainer} only.
 */
public class ServerlessDurabilityTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-m10")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private MetadataPlane freshIndex(AtomicLong clock, java.nio.file.Path dir) throws Exception {
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, dir, false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        return plane;
    }

    /** Isolate the codec from the storage: does a record survive a round trip at all? */
    public void testWalRecordRoundTrip() throws Exception {
        final var original = new org.opensearch.serverless.nodelog.WalRecord("7", "{\"msg\":\"durable\",\"n\":7}");
        final byte[] bytes = org.opensearch.core.common.bytes.BytesReference.toBytes(original.toBytes());
        logger.info("m10 wal record bytes: {}", new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
        final var parsed = org.opensearch.serverless.nodelog.WalRecord.fromStream(new java.io.ByteArrayInputStream(bytes));
        assertEquals(original.id(), parsed.id());
        assertEquals(original.source(), parsed.source());
    }

    /** Automatic publication: a tick publishes what changed, and publishes nothing when nothing did. */
    public void testATickPublishesWithoutAnyoneAskingIt() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir());

        try (ServerlessNode a = new ServerlessNode(nodeSettings("m10-auto"))) {
            a.startup().start();
            final BackgroundReconciler loop = new BackgroundReconciler(a, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = a.reconciler().openShards().iterator().next();

            assertTrue(
                "nothing should be published before anything is written",
                plane.segmentPublisher("alpha", 0).readManifest().isEmpty()
            );

            a.writes().index(shardId, "1", "{\"msg\":\"auto\",\"n\":1}");
            a.writes().index(shardId, "2", "{\"msg\":\"auto\",\"n\":2}");

            final TickResult published = loop.tick(clock.get() + 1_000);
            assertEquals("the tick should have published the shard", 1, published.published().size());
            assertTrue("a commit should now exist in the object store", plane.segmentPublisher("alpha", 0).readManifest().isPresent());

            // An idle tick must not publish. Otherwise an untouched shard uploads a fresh commit every
            // tick forever -- an object-store bill for saying nothing happened.
            final TickResult idle = loop.tick(clock.get() + 2_000);
            assertTrue("an idle tick must publish nothing", idle.published().isEmpty());
        }
    }

    /**
     * Durability end to end: nobody calls publish, the node dies mid-stream, and nothing is lost.
     *
     * <p>Without automatic publication and the write-ahead log, this loses data.
     */
    public void testNothingIsLostWhenAWriterDiesBetweenPublications() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final java.nio.file.Path objectStore = createTempDir();
        final MetadataPlane plane = freshIndex(clock, objectStore);

        final ServerlessNode a = new ServerlessNode(nodeSettings("m10-writer"));
        a.startup().start();
        final BackgroundReconciler loop = new BackgroundReconciler(a, plane);
        loop.want("alpha", 0);
        loop.tick(clock.get());
        final ShardId shardId = a.reconciler().openShards().iterator().next();

        // Five writes, then a tick publishes them. Note: publishShard is never called by this test.
        for (int i = 1; i <= 5; i++) {
            a.writes().index(shardId, String.valueOf(i), "{\"msg\":\"durable\",\"n\":" + i + "}");
        }
        assertEquals(1, loop.tick(clock.get() + 1_000).published().size());

        // Three more writes that no publication will ever cover.
        for (int i = 6; i <= 8; i++) {
            a.writes().index(shardId, String.valueOf(i), "{\"msg\":\"durable\",\"n\":" + i + "}");
        }

        // The process dies. No release, no drain, no final flush.
        a.close();

        clock.set(1_000L + TTL);
        try (ServerlessNode b = new ServerlessNode(nodeSettings("m10-successor"))) {
            b.startup().start();
            final ShardId onB = b.activation().activateWriter(plane, "alpha", 0).orElseThrow();
            assertEquals(
                "writes made after the last publication were lost: the WAL did not replay",
                8L,
                ShardOps.hits(b.searchService(), onB, "msg", "durable")
            );
        }
    }

    /** Replay is idempotent, so a successor that replays already-committed records duplicates nothing. */
    public void testReplayingRecordsAlreadyInTheCommitChangesNothing() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir());

        final ServerlessNode a = new ServerlessNode(nodeSettings("m10-idem"));
        a.startup().start();
        final BackgroundReconciler loop = new BackgroundReconciler(a, plane);
        loop.want("alpha", 0);
        loop.tick(clock.get());
        final ShardId shardId = a.reconciler().openShards().iterator().next();
        for (int i = 1; i <= 4; i++) {
            a.writes().index(shardId, String.valueOf(i), "{\"msg\":\"same\",\"n\":" + i + "}");
        }
        // Published, so all four are in the commit -- and still in the WAL, because truncation drops
        // only what the PREVIOUS publish saw.
        loop.tick(clock.get() + 1_000);
        a.close();

        clock.set(1_000L + TTL);
        try (ServerlessNode b = new ServerlessNode(nodeSettings("m10-idem2"))) {
            b.startup().start();
            final ShardId onB = b.activation().activateWriter(plane, "alpha", 0).orElseThrow();
            assertEquals(
                "replay duplicated documents that were already committed",
                4L,
                ShardOps.hits(b.searchService(), onB, "msg", "same")
            );
        }
    }

    /**
     * The records a log lets go of were in somebody's commit, proved by recovering after it.
     *
     * <p>Three failovers deep, and the middle two activate without publishing -- {@code activateWriter}
     * rather than a reconcile tick, because a tick publishes in the same pass it activates in. So after
     * A and B die, their writes are in their node logs alone. C replays both, publishes, writes once more
     * and dies. D needs C's commit and C's log together, so a log that let go of records no commit held
     * would leave D short.
     */
    public void testReclaimedRecordsAreAlreadyInACommit() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final java.nio.file.Path objectStore = createTempDir();
        final MetadataPlane plane = freshIndex(clock, objectStore);

        final ServerlessNode a = new ServerlessNode(nodeSettings("m15-chain-a"));
        a.startup().start();
        final ShardId onA = a.activation().activateWriter(plane, "alpha", 0).orElseThrow();
        for (int i = 1; i <= 3; i++) {
            a.writes().index(onA, String.valueOf(i), "{\"msg\":\"chain\",\"n\":" + i + "}");
        }
        a.close();

        clock.set(1_000L + TTL);
        final ServerlessNode b = new ServerlessNode(nodeSettings("m15-chain-b"));
        b.startup().start();
        final ShardId onB = b.activation().activateWriter(plane, "alpha", 0).orElseThrow();
        assertEquals("the second writer did not replay", 3L, ShardOps.hits(b.searchService(), onB, "msg", "chain"));
        b.writes().index(onB, "4", "{\"msg\":\"chain\",\"n\":4}");
        b.writes().index(onB, "5", "{\"msg\":\"chain\",\"n\":5}");
        b.close();

        clock.set(1_000L + 2 * TTL);
        final ServerlessNode c = new ServerlessNode(nodeSettings("m15-chain-c"));
        c.startup().start();
        final BackgroundReconciler loopC = new BackgroundReconciler(c, plane);
        loopC.want("alpha", 0);
        // Activates, replaying both older terms, then publishes.
        loopC.tick(clock.get());
        loopC.publishAll();
        final ShardId onC = c.reconciler().openShards().iterator().next();
        assertEquals("the third writer did not replay both older terms", 5L, ShardOps.hits(c.searchService(), onC, "msg", "chain"));

        // One more write that no publication will ever cover, so the last failover has to combine a
        // commit with a log rather than reading either one alone.
        c.writes().index(onC, "6", "{\"msg\":\"chain\",\"n\":6}");
        c.close();

        clock.set(1_000L + 3 * TTL);
        try (ServerlessNode d = new ServerlessNode(nodeSettings("m15-chain-d"))) {
            d.startup().start();
            final ShardId onD = d.activation().activateWriter(plane, "alpha", 0).orElseThrow();
            assertEquals(
                "reclaiming the older terms lost writes that were supposed to be in the commit",
                6L,
                ShardOps.hits(d.searchService(), onD, "msg", "chain")
            );
        }
    }
}
