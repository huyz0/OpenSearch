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

import java.util.concurrent.atomic.AtomicLong;

/**
 * M10: durability. Publication happens on its own, and a write is durable before it is acknowledged.
 *
 * <p>Two gaps close here, and they are different. <b>M10-a</b>: until now {@code publishShard} was only
 * ever called by hand, so the loss window was not "since the last commit" — it was unbounded, and a node
 * could index for hours and lose all of it. <b>M10-b</b>: even with automatic publication, everything
 * written since the last publish was lost, which is what phase 6's zombie test measured and recorded as
 * the WAL gap.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
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
        final var original = new org.opensearch.serverless.store.WalRecord("7", "{\"msg\":\"durable\",\"n\":7}");
        final byte[] bytes = org.opensearch.core.common.bytes.BytesReference.toBytes(original.toBytes());
        logger.info("m10 wal record bytes: {}", new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
        final var parsed = org.opensearch.serverless.store.WalRecord.fromStream(new java.io.ByteArrayInputStream(bytes));
        assertEquals(original.id(), parsed.id());
        assertEquals(original.source(), parsed.source());
    }

    /** M10-a: a tick publishes what changed, and publishes nothing when nothing did. */
    public void testATickPublishesWithoutAnyoneAskingIt() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir());

        try (ServerlessNode a = new ServerlessNode(nodeSettings("m10-auto"))) {
            a.start();
            final BackgroundReconciler loop = new BackgroundReconciler(a, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = a.reconciler().openShards().iterator().next();

            assertTrue(
                "nothing should be published before anything is written",
                plane.segmentPublisher("alpha", 0).readManifest().isEmpty()
            );

            a.index(shardId, "1", "{\"msg\":\"auto\",\"n\":1}");
            a.index(shardId, "2", "{\"msg\":\"auto\",\"n\":2}");

            final BackgroundReconciler.TickResult published = loop.tick(clock.get() + 1_000);
            assertEquals("the tick should have published the shard", 1, published.published().size());
            assertTrue("a commit should now exist in the object store", plane.segmentPublisher("alpha", 0).readManifest().isPresent());

            // An idle tick must not publish. Otherwise an untouched shard uploads a fresh commit every
            // tick forever -- an object-store bill for saying nothing happened.
            final BackgroundReconciler.TickResult idle = loop.tick(clock.get() + 2_000);
            assertTrue("an idle tick must publish nothing", idle.published().isEmpty());
        }
    }

    /**
     * M10 end to end: nobody calls publish, the node dies mid-stream, and nothing is lost.
     *
     * <p>This is the test phase 4 could not write and phase 6 could only measure the absence of.
     */
    public void testNothingIsLostWhenAWriterDiesBetweenPublications() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final java.nio.file.Path objectStore = createTempDir();
        final MetadataPlane plane = freshIndex(clock, objectStore);

        final ServerlessNode a = new ServerlessNode(nodeSettings("m10-writer"));
        a.start();
        final BackgroundReconciler loop = new BackgroundReconciler(a, plane);
        loop.want("alpha", 0);
        loop.tick(clock.get());
        final ShardId shardId = a.reconciler().openShards().iterator().next();

        // Five writes, then a tick publishes them. Note: publishShard is never called by this test.
        for (int i = 1; i <= 5; i++) {
            a.index(shardId, String.valueOf(i), "{\"msg\":\"durable\",\"n\":" + i + "}");
        }
        assertEquals(1, loop.tick(clock.get() + 1_000).published().size());

        // Three more writes that no publication will ever cover.
        for (int i = 6; i <= 8; i++) {
            a.index(shardId, String.valueOf(i), "{\"msg\":\"durable\",\"n\":" + i + "}");
        }

        // The process dies. No release, no drain, no final flush.
        a.close();

        clock.set(1_000L + TTL);
        try (ServerlessNode b = new ServerlessNode(nodeSettings("m10-successor"))) {
            b.start();
            final ShardId onB = b.activateWriter(plane, "alpha", 0).orElseThrow();
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
        a.start();
        final BackgroundReconciler loop = new BackgroundReconciler(a, plane);
        loop.want("alpha", 0);
        loop.tick(clock.get());
        final ShardId shardId = a.reconciler().openShards().iterator().next();
        for (int i = 1; i <= 4; i++) {
            a.index(shardId, String.valueOf(i), "{\"msg\":\"same\",\"n\":" + i + "}");
        }
        // Published, so all four are in the commit -- and still in the WAL, because truncation drops
        // only what the PREVIOUS publish saw.
        loop.tick(clock.get() + 1_000);
        a.close();

        clock.set(1_000L + TTL);
        try (ServerlessNode b = new ServerlessNode(nodeSettings("m10-idem2"))) {
            b.start();
            final ShardId onB = b.activateWriter(plane, "alpha", 0).orElseThrow();
            assertEquals(
                "replay duplicated documents that were already committed",
                4L,
                ShardOps.hits(b.searchService(), onB, "msg", "same")
            );
        }
    }

    /** Truncation drops a full cycle behind, never what the current publish saw. */
    public void testTruncationLagsOnePublishCycle() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir());

        try (ServerlessNode a = new ServerlessNode(nodeSettings("m10-trunc"))) {
            a.start();
            final BackgroundReconciler loop = new BackgroundReconciler(a, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = a.reconciler().openShards().iterator().next();
            final var wal = a.reconciler().wal(shardId);

            a.index(shardId, "1", "{\"msg\":\"cycle\",\"n\":1}");
            loop.tick(clock.get() + 1_000);
            assertEquals("the first publish must retain what it just saw", 1, wal.replayable().size());

            a.index(shardId, "2", "{\"msg\":\"cycle\",\"n\":2}");
            loop.tick(clock.get() + 2_000);
            assertEquals("the second publish should drop the first cycle and keep its own", 1, wal.replayable().size());
        }
    }
}
