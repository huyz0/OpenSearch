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
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.reconcile.GarbageCollector;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Phase 8: background reconciliation, garbage collection, and routing hints.
 *
 * <p>Nothing here is privileged. Every node runs the same loop, any number may run it at once, and a
 * node that stops running it harms only its own shards — because every action goes through the same
 * compare-and-swap as everyone else.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only. No durability claim for S3 or GCS until R11 runs.
 */
public class ServerlessReconcileTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-p8")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private static final class Deployment {
        final BlobStore store;
        final BlobPath base = BlobPath.cleanPath();
        final MetadataPlane plane;

        Deployment(BlobStore store, AtomicLong clock) {
            this.store = store;
            this.plane = new MetadataPlane(store, base, clock::get, TTL);
        }
    }

    private Deployment deploy(Path dir, AtomicLong clock, boolean counting) throws Exception {
        final BlobStore fs = new FsBlobStore(1024, dir, false);
        return new Deployment(counting ? new CountingBlobStore(fs) : fs, clock);
    }

    // ---- reconciliation -------------------------------------------------------------------------

    /** The gap phase 6 named: something has to notice an unowned shard and pick it up. */
    public void testATickReacquiresAShardAfterTheLeaseIsLost() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Deployment d = deploy(createTempDir(), clock, false);
        d.plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("p8-a"));
            ServerlessNode thief = new ServerlessNode(nodeSettings("p8-thief"))
        ) {
            a.start();
            thief.start();
            final BackgroundReconciler loop = new BackgroundReconciler(a, d.plane);
            loop.want("alpha", 0);

            final BackgroundReconciler.TickResult first = loop.tick(clock.get());
            assertEquals("the first tick should acquire the wanted shard", 1, first.activated().size());
            assertEquals(1, first.hintCount());

            // The lease lapses and another node takes it. A is still holding the shard open.
            clock.set(1_000L + TTL);
            assertTrue(thief.activateWriter(d.plane, "alpha", 0).isPresent());

            // One tick: A notices the loss, lets go, and does not silently re-take it from the thief.
            final BackgroundReconciler.TickResult second = loop.tick(clock.get());
            assertEquals("A did not release the shard it lost", 1, second.released().size());
            assertEquals("A stole back a shard with a live lease", 0, second.activated().size());
            assertTrue(a.reconciler().openShards().isEmpty());

            // The thief goes quiet. A takes it back on the tick after expiry -- automatically, which is
            // the whole point of the loop.
            clock.set(1_000L + 2 * TTL);
            final BackgroundReconciler.TickResult third = loop.tick(clock.get());
            assertEquals("A did not reacquire an expired shard", 1, third.activated().size());
        }
    }

    public void testHintsFollowTheHeadAndAStaleHintIsNotAnError() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Deployment d = deploy(createTempDir(), clock, false);
        d.plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode a = new ServerlessNode(nodeSettings("p8-h1")); ServerlessNode b = new ServerlessNode(nodeSettings("p8-h2"))) {
            a.start();
            b.start();
            final BackgroundReconciler loop = new BackgroundReconciler(a, d.plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            assertEquals(a.localNode().getId(), loop.hints().nodeFor("alpha", 0).orElseThrow());

            // Ownership moves without A being told. Its hint is now wrong, and that is allowed.
            clock.set(1_000L + TTL);
            b.activateWriter(d.plane, "alpha", 0);
            assertEquals(
                "a hint must not update by magic; being stale is the normal case",
                a.localNode().getId(),
                loop.hints().nodeFor("alpha", 0).orElseThrow()
            );

            // A refresh corrects it. Nothing was broken in between -- a wrong hint costs a retry.
            loop.tick(clock.get());
            assertEquals(b.localNode().getId(), loop.hints().nodeFor("alpha", 0).orElseThrow());
        }
    }

    // ---- garbage collection ---------------------------------------------------------------------

    /**
     * The mistake this collector is shaped to make impossible: a failover inherits segment files rather
     * than re-uploading them, so files a live commit depends on sit in older term containers. Collecting
     * by term alone would delete exactly what the current writer is serving.
     */
    public void testGcKeepsInheritedFilesAndDeletesAZombiesOrphans() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Deployment d = deploy(createTempDir(), clock, false);
        d.plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        final GarbageCollector gc = new GarbageCollector(d.store, d.base);

        try (
            ServerlessNode zombie = new ServerlessNode(nodeSettings("p8-gc-z"));
            ServerlessNode successor = new ServerlessNode(nodeSettings("p8-gc-s"))
        ) {
            zombie.start();
            successor.start();

            final ShardId onZombie = zombie.activateWriter(d.plane, "alpha", 0).orElseThrow();
            final long term1 = d.plane.heads().read("alpha", 0).orElseThrow().term();
            ShardOps.indexDoc(zombie.reconciler().shard(onZombie), "1", "{\"msg\":\"kept\",\"n\":1}");
            zombie.reconciler().shard(onZombie).refresh("p8");
            final CommitManifest firstCommit = zombie.publishShard(onZombie, term1);
            assertFalse(firstCommit.files().isEmpty());

            // Successor takes over and publishes, INHERITING term 1's files rather than re-uploading.
            clock.set(1_000L + TTL);
            final ShardId onSuccessor = successor.activateWriter(d.plane, "alpha", 0).orElseThrow();
            final long term2 = d.plane.heads().read("alpha", 0).orElseThrow().term();
            ShardOps.indexDoc(successor.reconciler().shard(onSuccessor), "2", "{\"msg\":\"kept\",\"n\":2}");
            successor.reconciler().shard(onSuccessor).refresh("p8");
            final CommitManifest liveCommit = successor.publishShard(onSuccessor, term2);

            final long inheritedFromTerm1 = liveCommit.files().values().stream().filter(t -> t.equals("t=" + term1)).count();
            assertTrue("this test is meaningless unless the live commit actually inherits from the old term", inheritedFromTerm1 > 0);

            // The zombie wakes and writes more. It cannot publish, so these files are orphans.
            ShardOps.indexDoc(zombie.reconciler().shard(onZombie), "3", "{\"msg\":\"orphan\",\"n\":3}");
            zombie.reconciler().shard(onZombie).flush(new org.opensearch.action.admin.indices.flush.FlushRequest().force(true));
            final int term1BlobsBefore = d.store.blobContainer(d.plane.shardData("alpha", 0).add("t=" + term1)).listBlobs().size();

            final List<String> deleted = gc.collectShard(d.plane, "alpha", 0);

            // Whatever was collected, every file the live commit names must still be readable.
            for (Map.Entry<String, String> file : liveCommit.files().entrySet()) {
                assertTrue(
                    "GC deleted a file the live commit depends on: " + file.getValue() + "/" + file.getKey(),
                    d.store.blobContainer(d.plane.shardData("alpha", 0).add(file.getValue())).blobExists(file.getKey())
                );
            }
            assertTrue("GC must not have deleted every term-1 blob", term1BlobsBefore > 0);
            logger.info("phase 8 GC: deleted {} orphan blobs, kept {} inherited files", deleted.size(), inheritedFromTerm1);

            // And the successor's data is still served after collection.
            assertEquals(2L, ShardOps.hits(successor.searchService(), onSuccessor, "msg", "kept"));
        }
    }

    /**
     * A reconcile pass sweeps what it published — after a grace, so a reader cannot lose the commit it is
     * reading.
     *
     * <p><b>Nothing in a running deployment ever ran the collector.</b> It was reachable from a test and
     * from an operator, and the garbage a failover leaves accumulated for the life of the deployment.
     * Storage that only grows is not something anybody is paged for, which is why it lasted.
     *
     * <p><b>The grace is the part that needs a test rather than a comment.</b> A reader serves the commit
     * it opened until a pass notices that commit has moved on, and the files it is reading in the meantime
     * are exactly what the collector's rule marks as collectable. Deleting on the first sweep would turn
     * that into a reader failing mid-query. So a blob must be seen unreferenced by two consecutive sweeps,
     * and the interval between them is the grace.
     */
    public void testAPassSweepsWhatItPublishedOnlyAfterAGrace() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Deployment d = deploy(createTempDir(), clock, false);
        d.plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode zombie = new ServerlessNode(nodeSettings("p8-sweep-z"));
            ServerlessNode successor = new ServerlessNode(nodeSettings("p8-sweep-s"))
        ) {
            zombie.start();
            successor.start();

            final ShardId onZombie = zombie.activateWriter(d.plane, "alpha", 0).orElseThrow();
            final long term1 = d.plane.heads().read("alpha", 0).orElseThrow().term();
            ShardOps.indexDoc(zombie.reconciler().shard(onZombie), "1", "{\"msg\":\"kept\",\"n\":1}");
            zombie.reconciler().shard(onZombie).refresh("p8");
            zombie.publishShard(onZombie, term1);

            clock.set(1_000L + TTL);
            final ShardId onSuccessor = successor.activateWriter(d.plane, "alpha", 0).orElseThrow();
            final long term2 = d.plane.heads().read("alpha", 0).orElseThrow().term();
            ShardOps.indexDoc(successor.reconciler().shard(onSuccessor), "2", "{\"msg\":\"kept\",\"n\":2}");
            successor.reconciler().shard(onSuccessor).refresh("p8");
            final CommitManifest liveCommit = successor.publishShard(onSuccessor, term2);

            // The zombie wakes and writes. It cannot publish, so these are orphans in a dead term.
            ShardOps.indexDoc(zombie.reconciler().shard(onZombie), "3", "{\"msg\":\"orphan\",\"n\":3}");
            zombie.reconciler().shard(onZombie).flush(new org.opensearch.action.admin.indices.flush.FlushRequest().force(true));

            try (BackgroundReconciler loop = new BackgroundReconciler(successor, d.plane)) {
                assertEquals(
                    "the first sighting must delete nothing",
                    java.util.Set.of(),
                    loop.sweepPublished(java.util.Set.of(onSuccessor))
                );
                assertEquals("nor the second", java.util.Set.of(), loop.sweepPublished(java.util.Set.of(onSuccessor)));

                final java.util.Set<String> swept = loop.sweepPublished(java.util.Set.of(onSuccessor));
                assertFalse("a blob unreferenced across the grace must eventually go: " + swept, swept.isEmpty());

                // And what the live commit names is still there, which is the whole safety rule.
                for (Map.Entry<String, String> file : liveCommit.files().entrySet()) {
                    assertTrue(
                        "the sweep deleted a file the live commit depends on: " + file.getValue() + "/" + file.getKey(),
                        d.store.blobContainer(d.plane.shardData("alpha", 0).add(file.getValue())).blobExists(file.getKey())
                    );
                }
                assertEquals(
                    "and the successor still serves its data",
                    2L,
                    ShardOps.hits(successor.searchService(), onSuccessor, "msg", "kept")
                );

                // Nothing is left watched, so the next pass costs nothing.
                assertEquals(java.util.Set.of(), loop.sweepPublished(java.util.Set.of()));
            }
        }
    }

    /**
     * And the full pass does it, not just the method.
     *
     * <p>The test above proves the sweep works when called; this proves something calls it. That gap is
     * the one a canary found — removing the call from {@code tick} broke nothing, because every test drove
     * the sweep directly. A correct method nothing runs is the same as no method.
     */
    public void testTheBackstopPassSweeps() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Deployment d = deploy(createTempDir(), clock, false);
        d.plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode zombie = new ServerlessNode(nodeSettings("p8-tick-z"));
            ServerlessNode successor = new ServerlessNode(nodeSettings("p8-tick-s"))
        ) {
            zombie.start();
            successor.start();
            final ShardId onZombie = zombie.activateWriter(d.plane, "alpha", 0).orElseThrow();
            final long term1 = d.plane.heads().read("alpha", 0).orElseThrow().term();
            ShardOps.indexDoc(zombie.reconciler().shard(onZombie), "1", "{\"msg\":\"kept\",\"n\":1}");
            zombie.reconciler().shard(onZombie).refresh("p8");
            zombie.publishShard(onZombie, term1);

            clock.set(1_000L + TTL);
            final ShardId onSuccessor = successor.activateWriter(d.plane, "alpha", 0).orElseThrow();
            final long term2 = d.plane.heads().read("alpha", 0).orElseThrow().term();
            ShardOps.indexDoc(successor.reconciler().shard(onSuccessor), "2", "{\"msg\":\"kept\",\"n\":2}");
            successor.reconciler().shard(onSuccessor).refresh("p8");
            final CommitManifest liveCommit = successor.publishShard(onSuccessor, term2);

            ShardOps.indexDoc(zombie.reconciler().shard(onZombie), "3", "{\"msg\":\"orphan\",\"n\":3}");
            zombie.reconciler().shard(onZombie).flush(new org.opensearch.action.admin.indices.flush.FlushRequest().force(true));
            final var deadTerm = d.store.blobContainer(d.plane.shardData("alpha", 0).add("t=" + term1));
            final int before = deadTerm.listBlobs().size();

            try (BackgroundReconciler loop = new BackgroundReconciler(successor, d.plane)) {
                loop.want("alpha", 0);
                // Three passes: one to see the orphans, one for the grace, one to delete them.
                for (int pass = 0; pass < 3; pass++) {
                    loop.tick(clock.get());
                }
            }

            assertTrue(
                "the backstop pass must have swept: " + before + " blobs before, " + deadTerm.listBlobs().size() + " after",
                deadTerm.listBlobs().size() < before
            );
            for (Map.Entry<String, String> file : liveCommit.files().entrySet()) {
                assertTrue(
                    "the pass deleted a file the live commit depends on: " + file.getValue() + "/" + file.getKey(),
                    d.store.blobContainer(d.plane.shardData("alpha", 0).add(file.getValue())).blobExists(file.getKey())
                );
            }
        }
    }

    /** With no grace configured, the first sweep is the sweep -- the knob is real and does something. */
    public void testAGraceOfZeroSweepsImmediately() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Deployment d = deploy(createTempDir(), clock, false);
        d.plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode zombie = new ServerlessNode(nodeSettings("p8-grace0-z"));
            ServerlessNode successor = new ServerlessNode(nodeSettings("p8-grace0-s"))
        ) {
            zombie.start();
            successor.start();
            final ShardId onZombie = zombie.activateWriter(d.plane, "alpha", 0).orElseThrow();
            final long term1 = d.plane.heads().read("alpha", 0).orElseThrow().term();
            ShardOps.indexDoc(zombie.reconciler().shard(onZombie), "1", "{\"msg\":\"kept\",\"n\":1}");
            zombie.reconciler().shard(onZombie).refresh("p8");
            zombie.publishShard(onZombie, term1);

            clock.set(1_000L + TTL);
            final ShardId onSuccessor = successor.activateWriter(d.plane, "alpha", 0).orElseThrow();
            final long term2 = d.plane.heads().read("alpha", 0).orElseThrow().term();
            ShardOps.indexDoc(successor.reconciler().shard(onSuccessor), "2", "{\"msg\":\"kept\",\"n\":2}");
            successor.reconciler().shard(onSuccessor).refresh("p8");
            successor.publishShard(onSuccessor, term2);
            ShardOps.indexDoc(zombie.reconciler().shard(onZombie), "3", "{\"msg\":\"orphan\",\"n\":3}");
            zombie.reconciler().shard(onZombie).flush(new org.opensearch.action.admin.indices.flush.FlushRequest().force(true));

            try (BackgroundReconciler loop = new BackgroundReconciler(successor, d.plane).setSweepGracePasses(0)) {
                assertFalse("with no grace the first sweep must delete", loop.sweepPublished(java.util.Set.of(onSuccessor)).isEmpty());
            }
        }
    }

    public void testGcLeavesTheLiveTermAloneAndNoOpsWithNothingPublished() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Deployment d = deploy(createTempDir(), clock, false);
        d.plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        final GarbageCollector gc = new GarbageCollector(d.store, d.base);

        // Nothing published: a writer may be mid-first-publish, so nothing is safe to judge.
        assertTrue(gc.collectShard(d.plane, "alpha", 0).isEmpty());

        try (ServerlessNode a = new ServerlessNode(nodeSettings("p8-gc-live"))) {
            a.start();
            final ShardId shardId = a.activateWriter(d.plane, "alpha", 0).orElseThrow();
            final long term = d.plane.heads().read("alpha", 0).orElseThrow().term();
            ShardOps.indexDoc(a.reconciler().shard(shardId), "1", "{\"msg\":\"live\",\"n\":1}");
            a.reconciler().shard(shardId).refresh("p8");
            final CommitManifest manifest = a.publishShard(shardId, term);

            // The reason the live term is excluded entirely, rather than trusted to the reference check:
            // publishing uploads files and THEN swaps the manifest, so there is a window in which a live
            // writer's files exist and are not yet referenced. Simulate exactly that.
            final String inFlight = "_inflight_not_yet_in_manifest.cfs";
            d.store.blobContainer(d.plane.shardData("alpha", 0).add("t=" + term))
                .writeBlob(inFlight, new java.io.ByteArrayInputStream(new byte[] { 1, 2, 3 }), 3L, false);

            assertTrue("the live term must never be collected", gc.collectShard(d.plane, "alpha", 0).isEmpty());
            assertTrue(
                "GC deleted a file a live writer was mid-publish on",
                d.store.blobContainer(d.plane.shardData("alpha", 0).add("t=" + term)).blobExists(inFlight)
            );
            for (Map.Entry<String, String> file : manifest.files().entrySet()) {
                assertTrue(d.store.blobContainer(d.plane.shardData("alpha", 0).add(file.getValue())).blobExists(file.getKey()));
            }
        }
    }

    // ---- the gossip gate (§10.3) -----------------------------------------------------------------

    /**
     * A pass does not go looking for points in time on every tick.
     *
     * <p><b>A listing is the most expensive shape of request the design makes</b>, and reaping expired
     * views needs one across the whole deployment. Doing it every pass would mean every node paying a
     * listing every thirty seconds to be told that a feature nobody in the deployment has used is still
     * not being used — a fleet of a thousand nodes turning that into millions of requests a day for
     * nothing. A keep-alive is measured in minutes, so a reap every tenth pass is soon enough.
     *
     * <p>The in-memory half — closing views this node is holding whose record has gone — still runs every
     * pass, because it costs nothing on a node holding none.
     *
     * <p>Measured rather than asserted about the constant, so the test fails if the gate is removed rather
     * than if the number is retuned.
     */
    public void testAPassDoesNotListPointsInTimeEveryTick() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Deployment d = deploy(createTempDir(), clock, true);
        final CountingBlobStore counter = (CountingBlobStore) d.store;
        d.plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode a = new ServerlessNode(nodeSettings("p8-pit-cost"))) {
            a.start();
            final BackgroundReconciler loop = new BackgroundReconciler(a, d.plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            counter.reset();
            final int passes = 10;
            for (int i = 0; i < passes; i++) {
                clock.set(clock.get() + 1_000L);
                loop.tick(clock.get());
            }

            assertTrue(
                "ten idle passes must not cost ten listings: " + counter.listings() + " of them, " + counter.breakdown(),
                counter.listings() <= 2
            );
        }
    }

    /**
     * A busy pass does not list either.
     *
     * <p>A pass that publishes used to list the shard's WAL container to decide what to truncate, list the
     * WAL root for older terms, list every dead term container to sweep, and list every point in time
     * and every snapshot to guard the sweep -- per dirty shard, per pass, on a shard whose publish had
     * only added segments. Now the writer knows its own ordinals, older terms are dropped once per term,
     * and a sweep runs only when a manifest lost a file. The one listing left is the first sweep after
     * the shard was opened, which is what a new owner owes its predecessor's leftovers.
     *
     * <p>And a reader opened on what was published lists nothing: the manifest carries every length.
     */
    public void testABusyPassDoesNotListEither() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Deployment d = deploy(createTempDir(), clock, true);
        final CountingBlobStore counter = (CountingBlobStore) d.store;
        d.plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("p8-busy"));
            ServerlessNode b = new ServerlessNode(nodeSettings("p8-busy-r"))
        ) {
            a.start();
            b.start();
            final BackgroundReconciler loop = new BackgroundReconciler(a, d.plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final var shardId = a.reconciler().openShards().iterator().next();
            // The first publish and the first sweep, paid once: the WAL root for older terms and the
            // shard's containers for a predecessor's orphans.
            a.index(shardId, "warm", "{\"msg\":\"warm\",\"n\":0}");
            clock.set(clock.get() + 1_000L);
            loop.tick(clock.get());

            counter.reset();
            // Five, not ten: few enough segments that no merge runs, since a merge is exactly what a
            // sweep may list for, and few enough passes that the tenth-pass reap does not fall inside.
            final int passes = 5;
            for (int i = 0; i < passes; i++) {
                a.index(shardId, "doc-" + i, "{\"msg\":\"busy\",\"n\":" + i + "}");
                clock.set(clock.get() + 1_000L);
                loop.tick(clock.get());
            }
            assertEquals("busy passes must not list: " + counter.listings() + " of them, " + counter.breakdown(), 0, counter.listings());
            // And a bounded number of register reads: the head once per shard per pass (the heartbeat's
            // read, reused by the hint rebuild and the publish), the descriptor once per index, the
            // scripts marker once. The publish swaps over the generation it remembers rather than
            // reading first.
            assertTrue(
                "a busy pass reads a handful of registers, not a dozen: " + counter.breakdown(),
                counter.registerReads() <= passes * 4
            );

            final var manifest = d.plane.segmentPublisher("alpha", 0).readManifest().orElseThrow();
            assertEquals("every published file's length is in the manifest", manifest.files().keySet(), manifest.lengths().keySet());

            counter.reset();
            b.serveAsReader(d.plane, "alpha", 0);
            assertEquals("opening a reader must not list: " + counter.breakdown(), 0, counter.listings());
        }
    }

    /**
     * §10.3 builds gossip only if polling turns out to be insufficient, measured rather than assumed.
     * This produces the number that decision needs: object-store operations per reconciliation pass.
     */
    public void testMeasureTheCostOfOneReconciliationPass() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Deployment d = deploy(createTempDir(), clock, true);
        final CountingBlobStore counter = (CountingBlobStore) d.store;
        d.plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode a = new ServerlessNode(nodeSettings("p8-measure"))) {
            a.start();
            final BackgroundReconciler loop = new BackgroundReconciler(a, d.plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());        // the acquiring tick; not what we are measuring

            counter.reset();
            final int passes = 10;
            for (int i = 0; i < passes; i++) {
                clock.set(clock.get() + 1_000L);
                loop.tick(clock.get());
            }

            final double opsPerTick = counter.total() / (double) passes;
            assertTrue("a steady-state tick must do some object-store work", opsPerTick > 0);
            assertTrue("a steady-state tick should not be doing dozens of operations per shard", opsPerTick < 20);

            // N nodes polling at interval T cost N * opsPerTick / T requests per second.
            final double atOneSecond = opsPerTick;
            logger.info(
                "phase 8 gossip gate: {} ops per tick per shard ({} reads, {} writes over {} passes). "
                    + "At a 1s interval that is {} req/s per node-shard; a 10k-node fleet polling 1 shard "
                    + "each every second would be {} req/s.",
                opsPerTick,
                counter.reads(),
                counter.writes(),
                passes,
                atOneSecond,
                atOneSecond * 10_000
            );
        }
    }
}
