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

    /**
     * Where the WAL's records actually are, by term, read off the filesystem rather than through the API.
     *
     * <p>{@code replayable()} cannot answer this: it returns records with no term attached, so it cannot
     * distinguish "the predecessor's log was reclaimed" from "the predecessor's log was replayed and its
     * documents rewritten under a new term". Deleting blobs leaves the container behind, so a term that
     * has been reclaimed shows up here as present with zero records -- which is the distinction that
     * matters, and is why this counts files instead of directories.
     */
    private static java.util.SortedMap<Long, Integer> walRecordsByTerm(java.nio.file.Path objectStore) throws Exception {
        final java.util.SortedMap<Long, Integer> byTerm = new java.util.TreeMap<>();
        final java.util.List<java.nio.file.Path> termDirs;
        try (var tree = java.nio.file.Files.walk(objectStore)) {
            termDirs = tree.filter(java.nio.file.Files::isDirectory)
                // Segments live under t=N too, one level up; only the ones inside wal/ are records.
                .filter(p -> p.getFileName().toString().startsWith("t="))
                .filter(p -> p.getParent() != null && p.getParent().getFileName().toString().equals("wal"))
                .collect(java.util.stream.Collectors.toList());
        }
        for (java.nio.file.Path dir : termDirs) {
            try (var files = java.nio.file.Files.list(dir)) {
                final int records = (int) files.filter(p -> p.getFileName().toString().matches("\\d{20}")).count();
                byTerm.put(Long.parseLong(dir.getFileName().toString().substring(2)), records);
            }
        }
        return byTerm;
    }

    /**
     * A publish at a higher term reclaims the log of the writer that died holding a lower one.
     *
     * <p>Same-term truncation trims only the publishing writer's own container, so before this a dead
     * predecessor's records were trimmed by nobody: every successor replayed them, forever, and the log
     * only grew. Harmless for correctness and unbounded all the same.
     */
    public void testASuccessorsPublishReclaimsTheDeadPredecessorsLog() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final java.nio.file.Path objectStore = createTempDir();
        final MetadataPlane plane = freshIndex(clock, objectStore);

        final ServerlessNode a = new ServerlessNode(nodeSettings("m15-reclaim-a"));
        a.start();
        final BackgroundReconciler loopA = new BackgroundReconciler(a, plane);
        loopA.want("alpha", 0);
        loopA.tick(clock.get());
        final ShardId onA = a.reconciler().openShards().iterator().next();
        for (int i = 1; i <= 3; i++) {
            a.index(onA, String.valueOf(i), "{\"msg\":\"reclaim\",\"n\":" + i + "}");
        }
        a.close(); // dies with three records and no publication behind them

        final var stranded = walRecordsByTerm(objectStore);
        assertEquals("the dead writer should have left exactly one term of records: " + stranded, 1, stranded.size());
        final long deadTerm = stranded.firstKey();
        assertEquals("the dead writer's records should still be on disk", 3, (int) stranded.get(deadTerm));

        clock.set(1_000L + TTL);
        try (ServerlessNode b = new ServerlessNode(nodeSettings("m15-reclaim-b"))) {
            b.start();
            final BackgroundReconciler loopB = new BackgroundReconciler(b, plane);
            loopB.want("alpha", 0);
            loopB.tick(clock.get());
            final ShardId onB = b.reconciler().openShards().iterator().next();
            assertEquals("the successor did not replay", 3L, ShardOps.hits(b.searchService(), onB, "msg", "reclaim"));

            b.index(onB, "4", "{\"msg\":\"reclaim\",\"n\":4}");
            assertEquals("nothing published, so nothing could be reclaimed", 1, loopB.tick(clock.get() + 1_000).published().size());

            final var afterPublish = walRecordsByTerm(objectStore);
            assertEquals(
                "the predecessor's records survived a publish at a higher term: " + afterPublish,
                0,
                (int) afterPublish.get(deadTerm)
            );
            // And the successor's own term is NOT reclaimed by its own publish -- the conservative
            // same-term rule still lags a cycle. Reclaiming across terms must not have widened it.
            assertTrue(
                "the publishing writer trimmed its own term, which the one-cycle lag forbids: " + afterPublish,
                afterPublish.entrySet().stream().anyMatch(e -> e.getKey() > deadTerm && e.getValue() > 0)
            );
        }
    }

    /**
     * The records reclaimed across terms were in somebody's commit, proved by recovering after they are gone.
     *
     * <p>Three failovers deep, and the middle two activate without publishing -- {@code activateWriter}
     * rather than a reconcile tick, because a tick publishes in the same pass it activates in. So after
     * A and B die, <em>nothing</em> has been reclaimed: reclamation is a consequence of publishing, not
     * of replaying, and a successor that tidied up the log it had just read would strand A's writes right
     * here. C then publishes, which drops both older terms, writes once more and dies. D can obtain A's
     * and B's documents only from C's commit, so if the drop had run against records that were not yet
     * durable, D would come up short.
     */
    public void testReclaimedRecordsAreAlreadyInACommit() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final java.nio.file.Path objectStore = createTempDir();
        final MetadataPlane plane = freshIndex(clock, objectStore);

        final ServerlessNode a = new ServerlessNode(nodeSettings("m15-chain-a"));
        a.start();
        final ShardId onA = a.activateWriter(plane, "alpha", 0).orElseThrow();
        for (int i = 1; i <= 3; i++) {
            a.index(onA, String.valueOf(i), "{\"msg\":\"chain\",\"n\":" + i + "}");
        }
        a.close();

        clock.set(1_000L + TTL);
        final ServerlessNode b = new ServerlessNode(nodeSettings("m15-chain-b"));
        b.start();
        final ShardId onB = b.activateWriter(plane, "alpha", 0).orElseThrow();
        assertEquals("the second writer did not replay", 3L, ShardOps.hits(b.searchService(), onB, "msg", "chain"));
        b.index(onB, "4", "{\"msg\":\"chain\",\"n\":4}");
        b.index(onB, "5", "{\"msg\":\"chain\",\"n\":5}");
        b.close();

        final var beforeAnyPublish = walRecordsByTerm(objectStore);
        assertEquals("both dead writers' terms should hold records: " + beforeAnyPublish, 2, beforeAnyPublish.size());
        assertEquals(
            "a replay must not reclaim the log it replayed -- only a publish may: " + beforeAnyPublish,
            5,
            beforeAnyPublish.values().stream().mapToInt(Integer::intValue).sum()
        );

        clock.set(1_000L + 2 * TTL);
        final ServerlessNode c = new ServerlessNode(nodeSettings("m15-chain-c"));
        c.start();
        final BackgroundReconciler loopC = new BackgroundReconciler(c, plane);
        loopC.want("alpha", 0);
        // Activates, replaying both older terms, and publishes in the same pass.
        assertEquals(1, loopC.tick(clock.get()).published().size());
        final ShardId onC = c.reconciler().openShards().iterator().next();
        assertEquals("the third writer did not replay both older terms", 5L, ShardOps.hits(c.searchService(), onC, "msg", "chain"));

        final var afterPublish = walRecordsByTerm(objectStore);
        for (long olderTerm : beforeAnyPublish.keySet()) {
            assertEquals(
                "term " + olderTerm + " was not reclaimed by the publish above it: " + afterPublish,
                0,
                (int) afterPublish.getOrDefault(olderTerm, 0)
            );
        }

        // One more write that no publication will ever cover, so the last failover has to combine a
        // commit with a log rather than reading either one alone.
        c.index(onC, "6", "{\"msg\":\"chain\",\"n\":6}");
        c.close();

        clock.set(1_000L + 3 * TTL);
        try (ServerlessNode d = new ServerlessNode(nodeSettings("m15-chain-d"))) {
            d.start();
            final ShardId onD = d.activateWriter(plane, "alpha", 0).orElseThrow();
            assertEquals(
                "reclaiming the older terms lost writes that were supposed to be in the commit",
                6L,
                ShardOps.hits(d.searchService(), onD, "msg", "chain")
            );
        }
    }
}
