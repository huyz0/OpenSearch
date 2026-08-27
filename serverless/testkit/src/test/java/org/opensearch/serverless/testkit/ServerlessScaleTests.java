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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Phase 9: the {@code plan-100m-index-implementation.md} targets, re-measured on this shell.
 *
 * <p>The claim the whole architecture rests on is from
 * {@code plan-area-h-metadata-off-cluster-state.md}: <em>index creation costs the same at 100 million
 * indices as at zero</em>. Classic OpenSearch does not manage that — the same document measured 7.4 ms
 * per create with 200 indices present, 10.3 at 1,000, 34.6 at 3,000 and 98.8 at 6,000, because
 * {@code Metadata.Builder.build()} rebuilds six name arrays and a sorted lookup across every index on
 * every create. Creating the millionth index does a million units of work that have nothing to do with it.
 *
 * <p>These tests measure the same shapes here. They assert on <b>trends and operation counts</b>, never
 * on absolute milliseconds: a wall-clock threshold would make the suite a flaky machine-speed detector
 * rather than an architecture check.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only. A local filesystem is not an object store, and these
 * numbers say nothing about what S3 does at the same shapes.
 */
public class ServerlessScaleTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-p9")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private static void createIndices(MetadataPlane plane, int from, int to) throws Exception {
        for (int i = from; i < to; i++) {
            plane.createIndex(new IndexDescriptor("idx-" + i, UUID.randomUUID().toString(), 1, MAPPING, null));
        }
    }

    /**
     * The headline. Per-create cost must not grow with the number of indices already present.
     */
    public void testCreationCostIsFlatInThePopulation() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final CountingBlobStore counter = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(counter, BlobPath.cleanPath(), clock::get, TTL);

        final int batch = 250;
        final List<Long> opsPerCreate = new ArrayList<>();
        final List<Long> nanosPerCreate = new ArrayList<>();

        for (int round = 0; round < 6; round++) {
            counter.reset();
            final long start = System.nanoTime();
            createIndices(plane, round * batch, (round + 1) * batch);
            final long elapsed = System.nanoTime() - start;
            opsPerCreate.add(counter.total() / batch);
            nanosPerCreate.add(elapsed / batch);
        }

        logger.info(
            "phase 9 creation: population 0->{} in {} batches of {}; ops/create={}; micros/create={}",
            6 * batch,
            6,
            batch,
            opsPerCreate,
            nanosPerCreate.stream().map(n -> n / 1000).toList()
        );

        // The architectural claim, stated as operations rather than as time: one create is one write,
        // whatever is already there. Classic OpenSearch cannot say this because build() sweeps.
        for (int round = 0; round < opsPerCreate.size(); round++) {
            assertEquals(
                "creation cost must not depend on the population; round " + round + " of " + opsPerCreate,
                opsPerCreate.get(0),
                opsPerCreate.get(round)
            );
        }
        assertEquals("a create should be a single object-store write", 1L, (long) opsPerCreate.get(0));
    }

    /**
     * §5's central claim: a node's residency tracks what it hosts, not how many indices exist.
     */
    public void testNodeResidencyTracksTheWorkingSetNotThePopulation() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        createIndices(plane, 0, 1_000);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("p9-residency"))) {
            node.start();
            node.activateWriter(plane, "idx-7", 0);

            // A thousand indices exist. This node holds one shard of one of them.
            assertEquals("the node must hold exactly the shard it owns", 1, node.reconciler().openShards().size());
            assertEquals(
                "a node's cluster state must contain only what it hosts, whatever else exists",
                1,
                node.clusterService().state().metadata().indices().size()
            );
            assertTrue(node.clusterService().state().metadata().hasIndex("idx-7"));
            assertFalse(
                "the node must not know about indices it does not host",
                node.clusterService().state().metadata().hasIndex("idx-8")
            );
        }
    }

    /**
     * And the cost of a node learning what it hosts must scale with what it hosts.
     *
     * <p>This is the one that matters most in the steady state: it runs on every reconciliation tick,
     * forever, on every node.
     */
    public void testTruthReadCostScalesWithWhatANodeHostsNotWithThePopulation() throws Exception {
        final long atSmall = truthOpsAtPopulation(200);
        final long atLarge = truthOpsAtPopulation(1_000);

        logger.info("phase 9 truthFor: {} ops at population 200, {} ops at population 1000 (node hosts 1 shard in both)", atSmall, atLarge);

        assertEquals(
            "reading one node's truth must not cost more because other indices exist: "
                + atSmall
                + " ops at 200 indices vs "
                + atLarge
                + " at 1000",
            atSmall,
            atLarge
        );
    }

    /**
     * The assignment listing is a hint, so a stale entry must cost a read and then be ignored — never
     * cause a node to open a shard it does not own.
     */
    public void testAStaleAssignmentClaimIsIgnoredRatherThanBelieved() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        createIndices(plane, 0, 3);

        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("p9-stale-a"));
            ServerlessNode b = new ServerlessNode(nodeSettings("p9-stale-b"))
        ) {
            a.start();
            b.start();
            a.activateWriter(plane, "idx-0", 0);
            assertEquals(1, plane.truthFor(a.localNode().getId()).assignments().size());

            // B takes the shard once A's lease lapses. A's claim entry is now stale and still present.
            clock.set(1_000L + TTL);
            assertTrue(b.activateWriter(plane, "idx-0", 0).isPresent());

            assertTrue(
                "a stale claim must not make a node believe it still owns a shard",
                plane.truthFor(a.localNode().getId()).assignments().isEmpty()
            );
            assertEquals("and the real owner must see it", 1, plane.truthFor(b.localNode().getId()).assignments().size());
        }
    }

    /** A claim whose index was deleted underneath it must not break the read. */
    public void testAClaimForADeletedIndexIsSkipped() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        createIndices(plane, 0, 2);

        try (ServerlessNode a = new ServerlessNode(nodeSettings("p9-deleted"))) {
            a.start();
            a.activateWriter(plane, "idx-0", 0);
            a.activateWriter(plane, "idx-1", 0);
            assertEquals(2, plane.truthFor(a.localNode().getId()).assignments().size());

            plane.deleteIndex("idx-0");
            final var truth = plane.truthFor(a.localNode().getId());
            assertEquals("the surviving index must still be reported", 1, truth.assignments().size());
            assertEquals("idx-1", truth.assignments().iterator().next().indexName());
        }
    }

    private long truthOpsAtPopulation(int population) throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final CountingBlobStore counter = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(counter, BlobPath.cleanPath(), clock::get, TTL);
        createIndices(plane, 0, population);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("p9-truth-" + population))) {
            node.start();
            node.activateWriter(plane, "idx-3", 0);
            counter.reset();
            plane.truthFor(node.localNode().getId());
            return counter.total();
        }
    }
}
