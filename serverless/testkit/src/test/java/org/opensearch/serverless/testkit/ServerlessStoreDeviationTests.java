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
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicLong;

/**
 * What this system does when the object store is not what it was promised to be.
 *
 * <p><b>Why this exists.</b> R11 asks whether a provider's compare-and-swap is linearizable, and it cannot
 * be answered without an account to ask. The question underneath it never needed one and had never been
 * asked: <em>if the store did deviate, what would break, how far, and would anybody notice?</em> "Everything
 * is downstream of that assumption" is a sentence. These turn it into a measurement.
 *
 * <p><b>The finding, stated before the tests that establish it.</b> The blast radius is far narrower than
 * the sentence suggests, and it is narrow in a way that is worth knowing:
 *
 * <ul>
 *   <li>A <b>stale read</b> of a shard-head costs nothing. Ownership is decided by the swap, not by the
 *       read, so a node reading an out-of-date head simply swaps against a generation that has moved and
 *       loses — which is the ordinary, correct outcome.</li>
 *   <li>An <b>ambiguous outcome</b> — the write applied, the answer was lost — costs liveness and not
 *       safety. The node believes it failed to take the shard and does not serve it, while the head says it
 *       owns it. Nobody writes to the shard until the lease lapses.</li>
 *   <li><b>Two winners on one generation</b> is the one that costs data, and this is where it costs it: not
 *       at activation, where both nodes carry on happily, but at <em>publish</em>, where the manifest's own
 *       swap fences one of them — after it has already acknowledged writes that are now in a log no
 *       successor will ever replay.</li>
 * </ul>
 *
 * <p>So the manifest register is a second line of defence nobody designed as one, and it converts a
 * correctness failure into a bounded data-loss failure. That is much better than divergence and it is still
 * silent: the client that got its acknowledgement is never told.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} underneath, deviations injected on top.
 */
public class ServerlessStoreDeviationTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings readerSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-deviation")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "search")
            .build();
    }

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-deviation")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** A plane whose shard-head container misbehaves and whose everything else does not. */
    private record Deviating(MetadataPlane plane, MisbehavingBlobStore store) {
    }

    private Deviating planeWithBrokenHeads(AtomicLong clock) throws Exception {
        final var honest = new FsBlobStore(1024, createTempDir(), false);
        final BlobPath heads = RegisterMap.shards(BlobPath.cleanPath());
        final var store = new MisbehavingBlobStore(honest, path -> path.buildAsString().equals(heads.buildAsString()));
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        return new Deviating(plane, store);
    }

    /**
     * A stale read of the shard-head changes nothing, because the read is not what decides.
     *
     * <p>Worth establishing rather than assuming: it is the deviation a real provider is most likely to
     * have, and finding that the design does not depend on read freshness for <em>ownership</em> is what
     * narrows R11's blast radius from "everything" to one specific failure.
     */
    public void testAStaleHeadReadDoesNotProduceTwoOwners() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final var deviating = planeWithBrokenHeads(clock);

        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("dev-stale-a"));
            ServerlessNode b = new ServerlessNode(nodeSettings("dev-stale-b"))
        ) {
            a.start();
            b.start();
            assertTrue("A takes the shard", a.activateWriter(deviating.plane(), "alpha", 0).isPresent());

            // Now every read of the shard-head is one version out of date -- B is about to be told the
            // shard is still free.
            deviating.store().broken(RegisterMap.shards(BlobPath.cleanPath())).staleReadEvery(1);

            assertTrue(
                "B must still lose, because the swap decides and the read does not",
                b.activateWriter(deviating.plane(), "alpha", 0).isEmpty()
            );
            assertTrue("and B must not be serving anything", b.reconciler().openShards().isEmpty());
        }
    }

    /**
     * An ambiguous activation costs liveness, not safety.
     *
     * <p>The swap applied and the answer was lost — produced as often by a client's own retry after a
     * dropped connection as by any provider. The node concludes it did not get the shard. Nothing is
     * corrupted and nothing is written twice; the shard simply has an owner that is not serving it, until
     * the lease lapses and somebody takes it properly.
     *
     * <p>That is the right way round to fail, and it is worth knowing that it is what happens rather than
     * hoping so.
     */
    public void testAnAmbiguousActivationCostsLivenessAndNotSafety() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final var deviating = planeWithBrokenHeads(clock);
        deviating.store().broken(RegisterMap.shards(BlobPath.cleanPath())).ambiguousCreateEvery(1);

        try (ServerlessNode a = new ServerlessNode(nodeSettings("dev-ambiguous"))) {
            a.start();
            expectThrows(Exception.class, () -> a.activateWriter(deviating.plane(), "alpha", 0));
            assertTrue("the node must not be serving a shard it believes it failed to take", a.reconciler().openShards().isEmpty());

            // And the head says otherwise, which is exactly the asymmetry: the store took the write.
            deviating.store().broken(RegisterMap.shards(BlobPath.cleanPath())).ambiguousCreateEvery(0);
            assertEquals(
                "the write did apply, so the shard now has an owner that is not serving it",
                a.localNode().getId(),
                deviating.plane().heads().read("alpha", 0).orElseThrow().ownerNodeId()
            );
        }
    }

    /**
     * Two winners: both nodes serve the shard, and the published commit ends up being neither node's.
     *
     * <p>This is the measurement the file exists for, and the answer is worse than the one expected.
     *
     * <p><b>Setting it up faithfully needs two views, not one broken store.</b> Two winners is not a store
     * behaving badly in isolation — each side is atomic and honours its own preconditions — it is one
     * register having diverged, so a claim impossible against the true state is legal against the local
     * one. So each node gets its own view over shared bytes, and one of them is partitioned on exactly the
     * shard-head register: one key wide, as a replica that has missed one write is.
     *
     * <p><b>What happens.</b> Both nodes are told they own the shard at term 1, and nothing at activation
     * could have caught it. Both accept writes. Then both publish — and the manifest does <em>not</em>
     * fence the second, because its guard is a <em>strictly newer</em> term and these two share one. The
     * second publish inherits the first's file list by name, skipping the upload of any name already
     * published on the grounds that "a name already published has the same bytes" — which is true under
     * one writer per term and false here, where both shards started empty and independently produced
     * segments called {@code _0.cfs}.
     *
     * <p>So the surviving commit names the second writer's segments and points at the first writer's
     * bytes. Not divergence, and not the bounded loss of one node's unpublished writes: a commit neither
     * node ever had.
     *
     * <p>This is the sharpest statement available of why R11 matters, and it is a statement about this
     * design rather than about any provider. It also names the exact line that turns a control-plane
     * deviation into a data-plane one — the inherit-by-name optimisation in {@code SegmentPublisher} —
     * which is a thing worth knowing about even if every provider is perfect.
     */
    public void testTwoWinnersProduceACommitNeitherNodeWrote() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final var honest = new FsBlobStore(1024, createTempDir(), false);
        final BlobPath headsPath = RegisterMap.shards(BlobPath.cleanPath());
        final java.util.function.Predicate<BlobPath> isHeads = path -> path.buildAsString().equals(headsPath.buildAsString());

        // Two views over one set of bytes. A's is honest throughout.
        final var viewA = new MisbehavingBlobStore(honest, isHeads);
        final var viewB = new MisbehavingBlobStore(honest, isHeads);
        final MetadataPlane planeA = new MetadataPlane(viewA, BlobPath.cleanPath(), clock::get, TTL);
        final MetadataPlane planeB = new MetadataPlane(viewB, BlobPath.cleanPath(), clock::get, TTL);
        planeA.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        // B's view of the shard-head only. Everything else it reads and writes is the real thing --
        // including the manifest, which is what makes the second half of this test meaningful.
        viewB.broken(headsPath).partitionRegister(RegisterMap.shardHeadBlob("alpha", 0));

        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("dev-two-a"));
            ServerlessNode b = new ServerlessNode(nodeSettings("dev-two-b"))
        ) {
            a.start();
            b.start();

            final var onA = a.activateWriter(planeA, "alpha", 0);
            final var onB = b.activateWriter(planeB, "alpha", 0);
            assertTrue("A must have been told it owns the shard", onA.isPresent());
            assertTrue("B must have been told the same thing; that is the deviation", onB.isPresent());
            assertEquals(
                "and at the same term, which is what makes them two winners",
                1L,
                planeA.heads().read("alpha", 0).orElseThrow().term()
            );

            ShardOps.indexDoc(a.reconciler().shard(onA.get()), "a1", "{\"msg\":\"aaa\"}");
            a.reconciler().shard(onA.get()).refresh("deviation");
            ShardOps.indexDoc(b.reconciler().shard(onB.get()), "b1", "{\"msg\":\"bbb\"}");
            b.reconciler().shard(onB.get()).refresh("deviation");

            a.publishShard(onA.get(), 1L);
            final var afterA = planeA.segmentPublisher("alpha", 0).readManifest().orElseThrow();

            // The manifest's guard is a strictly newer term, so a second writer at the same term walks
            // straight through it.
            b.publishShard(onB.get(), 1L);
            final var afterB = planeA.segmentPublisher("alpha", 0).readManifest().orElseThrow();

            assertEquals("both writers published at one term", 1L, afterB.term());
            assertEquals(
                "and the second publish was not refused, because the manifest only fences a strictly newer term",
                afterA.files().keySet(),
                afterB.files().keySet()
            );

            // Why the file lists match: B produced segments with the same names A had already published --
            // both shards started empty and flushed once -- and the publisher skips uploading a name that
            // is already there, on the grounds that segment files are immutable and a published name has
            // the same bytes. True under one writer per term. Not true here.
            logger.info("two winners at one term: the surviving commit names {}", afterB.files().keySet());

            // So what survived is A's bytes, and B's acknowledged write is gone -- with no error anywhere,
            // no term bump, and nothing in the commit to say a second writer ever existed.
            try (ServerlessNode reader = new ServerlessNode(readerSettings("dev-two-reader"))) {
                reader.start();
                final ShardId served = reader.serveAsReader(planeA, "alpha", 0);
                assertEquals(
                    "the first writer's document must be in the published commit",
                    1L,
                    ShardOps.hits(reader.searchService(), served, "msg", "aaa")
                );
                assertEquals(
                    "the second writer's acknowledged document is silently gone",
                    0L,
                    ShardOps.hits(reader.searchService(), served, "msg", "bbb")
                );
            }
        }
    }

    /**
     * And when the two writers' segments do not happen to share names, the commit is not merely wrong —
     * it is not a commit.
     *
     * <p>The test above is the lucky case: both shards started empty, flushed once, and produced the same
     * file names, so the second publish inherited every one of them and uploaded nothing. The surviving
     * commit was at least <em>one node's</em>, whole.
     *
     * <p>Give the second writer a different number of segments and that stops being true. It inherits the
     * names it shares and uploads the ones it does not, so the manifest it publishes names its own segments
     * file — which refers to segments the first writer never wrote — alongside blobs holding the first
     * writer's bytes. The result is a manifest describing an index that has never existed anywhere.
     *
     * <p>This test does not assert that the commit is unreadable, because what Lucene does with an
     * incoherent segments file is Lucene's business and may change. It asserts the thing that is this
     * design's business: the surviving manifest names blobs from two different writers, which is a state
     * nothing downstream is built to survive.
     */
    public void testTwoWinnersWithDifferentSegmentsProduceAMixedCommit() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final var honest = new FsBlobStore(1024, createTempDir(), false);
        final BlobPath headsPath = RegisterMap.shards(BlobPath.cleanPath());
        final java.util.function.Predicate<BlobPath> isHeads = path -> path.buildAsString().equals(headsPath.buildAsString());

        final var viewA = new MisbehavingBlobStore(honest, isHeads);
        final var viewB = new MisbehavingBlobStore(honest, isHeads);
        final MetadataPlane planeA = new MetadataPlane(viewA, BlobPath.cleanPath(), clock::get, TTL);
        final MetadataPlane planeB = new MetadataPlane(viewB, BlobPath.cleanPath(), clock::get, TTL);
        planeA.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        viewB.broken(headsPath).partitionRegister(RegisterMap.shardHeadBlob("alpha", 0));

        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("dev-mixed-a"));
            ServerlessNode b = new ServerlessNode(nodeSettings("dev-mixed-b"))
        ) {
            a.start();
            b.start();
            final var onA = a.activateWriter(planeA, "alpha", 0).orElseThrow();
            final var onB = b.activateWriter(planeB, "alpha", 0).orElseThrow();

            ShardOps.indexDoc(a.reconciler().shard(onA), "a1", "{\"msg\":\"aaa\"}");
            a.reconciler().shard(onA).refresh("deviation");

            // Two commits on B, so it has segments the first writer never produced.
            ShardOps.indexDoc(b.reconciler().shard(onB), "b1", "{\"msg\":\"bbb\"}");
            b.reconciler().shard(onB).flush(new org.opensearch.action.admin.indices.flush.FlushRequest().force(true));
            ShardOps.indexDoc(b.reconciler().shard(onB), "b2", "{\"msg\":\"ccc\"}");
            b.reconciler().shard(onB).flush(new org.opensearch.action.admin.indices.flush.FlushRequest().force(true));

            final var fromA = a.publishShard(onA, 1L);
            final var fromB = b.publishShard(onB, 1L);

            final java.util.Set<String> onlyB = new java.util.HashSet<>(fromB.files().keySet());
            onlyB.removeAll(fromA.files().keySet());
            final java.util.Set<String> shared = new java.util.HashSet<>(fromB.files().keySet());
            shared.retainAll(fromA.files().keySet());

            assertFalse("this test needs the second writer to have segments the first did not: " + fromB.files().keySet(), onlyB.isEmpty());
            assertFalse("and to share at least one name, which is what makes the mixture: " + shared, shared.isEmpty());

            logger.info(
                "two winners with different segments: the surviving commit names {}, of which {} are the second "
                    + "writer's own uploads and {} are blobs the first writer put there",
                fromB.files().keySet(),
                onlyB,
                shared
            );

            final var surviving = planeA.segmentPublisher("alpha", 0).readManifest().orElseThrow();
            assertEquals("the second publish is the one that stands", fromB.files(), surviving.files());
        }
    }
}
