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
import org.opensearch.serverless.store.ForeignWriterException;
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
     * Two winners at one term are refused at publish, loudly.
     *
     * <p><b>What this measured before the guard existed.</b> Both nodes are told they own the shard at term
     * 1, and nothing at activation could have caught it. Both accept writes. Then both published — because
     * the manifest's fence refuses a <em>strictly newer</em> term and says nothing about two writers
     * sharing one. The second publish inherited the first's file names and skipped uploading them, on the
     * grounds that a published name has the same bytes: true under one writer per term, false here. The
     * second node's acknowledged write vanished, with no error anywhere and nothing in the commit to say a
     * second writer had ever existed.
     *
     * <p>A manifest now records which node wrote it, and a publish from a different node at the same term
     * is refused. The data is still lost — nothing can un-acknowledge a write — but the node is told, and
     * an operator gets a message naming the object store rather than a silent hole in an index.
     *
     * <p>Setting this up faithfully needs two views over one set of bytes rather than one broken store:
     * neither side misbehaves in isolation, the register has simply diverged, and a claim impossible
     * against the true state is legal against the local one.
     */
    public void testTwoWinnersAtOneTermAreRefusedAtPublish() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final var honest = new FsBlobStore(1024, createTempDir(), false);
        final BlobPath headsPath = RegisterMap.shards(BlobPath.cleanPath());
        final java.util.function.Predicate<BlobPath> isHeads = path -> path.buildAsString().equals(headsPath.buildAsString());

        final var viewA = new MisbehavingBlobStore(honest, isHeads);
        final var viewB = new MisbehavingBlobStore(honest, isHeads);
        final MetadataPlane planeA = new MetadataPlane(viewA, BlobPath.cleanPath(), clock::get, TTL);
        final MetadataPlane planeB = new MetadataPlane(viewB, BlobPath.cleanPath(), clock::get, TTL);
        planeA.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        // B's view of the shard-head only. Everything else it reads and writes is the real thing --
        // including the manifest, which is what makes the refusal below meaningful.
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
                "and at the same term, which is what makes them two winners rather than a takeover",
                1L,
                planeA.heads().read("alpha", 0).orElseThrow().term()
            );

            ShardOps.indexDoc(a.reconciler().shard(onA.get()), "a1", "{\"msg\":\"aaa\"}");
            a.reconciler().shard(onA.get()).refresh("deviation");
            ShardOps.indexDoc(b.reconciler().shard(onB.get()), "b1", "{\"msg\":\"bbb\"}");
            b.reconciler().shard(onB.get()).refresh("deviation");

            a.publishShard(onA.get(), 1L);

            final var refused = expectThrows(ForeignWriterException.class, () -> b.publishShard(onB.get(), 1L));
            assertTrue(
                "the refusal must name the other node and say what it implies: " + refused.getMessage(),
                refused.getMessage().contains(a.localNode().getId()) && refused.getMessage().contains("two winners")
            );

            // The surviving commit is the first writer's, whole. The second's write is gone -- that part is
            // not recoverable and never was -- but it is gone with an exception rather than in silence.
            try (ServerlessNode reader = new ServerlessNode(readerSettings("dev-two-reader"))) {
                reader.start();
                final ShardId served = reader.serveAsReader(planeA, "alpha", 0);
                assertEquals(
                    "the first writer's document must be in the published commit",
                    1L,
                    ShardOps.hits(reader.searchService(), served, "msg", "aaa")
                );
                assertEquals(
                    "and the second writer's must not have been spliced into it",
                    0L,
                    ShardOps.hits(reader.searchService(), served, "msg", "bbb")
                );
            }
        }
    }

    /**
     * And the refusal is what stops a manifest naming two writers' segments at once.
     *
     * <p>The case above is the lucky one: both shards flushed once and produced the same file names, so
     * without the guard the survivor was at least <em>one node's</em> commit, whole. Give the second writer
     * a different number of segments and that stops being true — it inherits the names it shares and
     * uploads the ones it does not, publishing a manifest whose segments file refers to segments the other
     * node wrote. Measured before the guard existed, the surviving commit named
     * {@code [_1.cfs, _1.cfe, _0.cfe, _1.si, _0.si, _0.cfs, segments_5]}, of which {@code segments_5} and
     * the {@code _1} files were the second node's uploads and the {@code _0} files were the first node's
     * blobs: a manifest describing an index that has never existed anywhere.
     *
     * <p>That is the outcome this test exists to keep impossible, and removing the guard is the canary that
     * proves it still would happen.
     */
    public void testAMixedCommitCannotBePublished() throws Exception {
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
            expectThrows(ForeignWriterException.class, () -> b.publishShard(onB, 1L));

            final var surviving = planeA.segmentPublisher("alpha", 0).readManifest().orElseThrow();
            assertEquals("the first writer's commit must be the one that stands", fromA.files(), surviving.files());
            assertEquals("and it must be recorded as its writer's", a.localNode().getId(), surviving.writer());
        }
    }

    /**
     * A manifest written before writers were recorded is treated as "cannot tell", not as a mismatch.
     *
     * <p>Refusing every publish onto an older manifest would be a worse failure than the one the guard
     * exists for: an upgrade would stop every shard in the deployment from publishing until somebody
     * rewrote its manifests by hand. The guard is a defence against something that should be impossible,
     * and a defence that breaks the ordinary case is not one.
     */
    public void testAManifestWithNoRecordedWriterDoesNotBlockPublishing() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final var store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode a = new ServerlessNode(nodeSettings("dev-legacy"))) {
            a.start();
            final var onA = a.activateWriter(plane, "alpha", 0).orElseThrow();
            ShardOps.indexDoc(a.reconciler().shard(onA), "a1", "{\"msg\":\"aaa\"}");
            a.reconciler().shard(onA).refresh("deviation");
            a.publishShard(onA, 1L);

            // Rewrite the manifest as an older build would have left it: a term and files, no writer.
            final var published = plane.segmentPublisher("alpha", 0).readManifest().orElseThrow();
            final var legacy = new org.opensearch.serverless.store.CommitManifest(published.term(), published.files());
            assertNull("the fixture must really have no writer", legacy.writer());
            final var container = store.blobContainer(plane.shardData("alpha", 0));
            final var existing = container.readRegister("manifest").orElseThrow();
            assertTrue(container.compareAndSwapRegister("manifest", existing.generation(), legacy.toBytes()).applied());

            // And publishing onto it still works.
            ShardOps.indexDoc(a.reconciler().shard(onA), "a2", "{\"msg\":\"ddd\"}");
            a.reconciler().shard(onA).refresh("deviation");
            final var again = a.publishShard(onA, 1L);
            assertEquals("the publish must have been allowed and recorded its writer", a.localNode().getId(), again.writer());
        }
    }
}
