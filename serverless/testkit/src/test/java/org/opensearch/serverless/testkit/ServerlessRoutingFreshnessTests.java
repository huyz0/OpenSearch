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
 * Two costs were removed from the request path by allowing a bounded staleness, and these are the
 * properties that make that safe rather than merely cheap.
 *
 * <p>A descriptor read used to happen on every write, get and search; a head read used to happen for
 * every held shard on every pass. Both now answer from a recent read for a bounded window. The tests
 * below are about what that window can and cannot do — a cost test can only show the saving, and the
 * saving is the easy half.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessRoutingFreshnessTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-freshness")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /**
     * A name this node has not resolved before is read through, so an index is usable the moment it
     * exists.
     *
     * <p>The obvious way to get a routing cache wrong is to cache the absence too. Then create-then-write
     * — which is what every getting-started guide does, and what a test fixture does a thousand times —
     * fails for the length of the window against a node that happened to be asked about the name first.
     */
    public void testAnIndexIsWritableTheInstantItExists() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);

        // Asked before it exists, which is what puts the miss in front of the create.
        assertTrue("nothing should resolve yet", plane.writeTarget("alpha").isEmpty());

        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        assertTrue(
            "a name asked about before it existed must resolve as soon as it does: a cached miss would hide it",
            plane.writeTarget("alpha").isPresent()
        );
    }

    /**
     * A change this node makes is visible to this node's own next request, with no window at all.
     *
     * <p>Read-your-writes against the node you just wrote to is where a client actually notices staleness,
     * so the store announces every change and the plane drops the name. The window applies to changes made
     * <em>elsewhere</em>, which is the trade being bought.
     */
    public void testANodeSeesItsOwnChangeImmediately() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        // Resolved, and therefore cached.
        final var before = plane.writeTarget("alpha").orElseThrow();
        assertEquals("uuid-alpha-00000000", before.index().uuid());

        plane.deleteIndex("alpha");
        assertTrue("a delete on this node must be visible to this node at once, not a window later", plane.writeTarget("alpha").isEmpty());

        // And a recreation under a new uuid is the descriptor this node then routes by.
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-11111111", 1, MAPPING, null));
        assertEquals("uuid-alpha-11111111", plane.writeTarget("alpha").orElseThrow().index().uuid());
    }

    /**
     * A resolution left over from a previous incarnation cannot acknowledge a write into it.
     *
     * <p><b>This is the property the whole cache rests on.</b> Another node deletes and recreates the
     * index; this node's cached resolution still names the old uuid until its window passes. The write it
     * routes is refused rather than applied, because the uuid is checked again where the write lands — an
     * open shard is matched on uuid as well as name. A refusal is a retry; the alternative would have been
     * an acknowledged write into an incarnation the collector is about to sweep.
     */
    public void testAStaleResolutionIsRefusedRatherThanMisrouted() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        // Two planes over one store: one node's cache cannot be dropped by another node's write, which is
        // exactly the situation the window exists in.
        final MetadataPlane mine = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        final MetadataPlane theirs = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        theirs.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("freshness-stale"))) {
            node.start();
            node.setMetadataPlane(mine);
            final BackgroundReconciler loop = new BackgroundReconciler(node, mine);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();
            assertEquals("uuid-alpha-00000000", shardId.getIndex().getUUID());

            // Warm this node's cache on the old incarnation, then let the other node replace it.
            assertEquals("uuid-alpha-00000000", mine.writeTarget("alpha").orElseThrow().index().uuid());
            theirs.deleteIndex("alpha");
            theirs.createIndex(new IndexDescriptor("alpha", "uuid-alpha-11111111", 1, MAPPING, null));

            // This node still resolves the old uuid -- that is the staleness, and it is expected.
            assertEquals(
                "the window is real: this node has not learned of the change yet",
                "uuid-alpha-00000000",
                mine.writeTarget("alpha").orElseThrow().index().uuid()
            );

            // But the shard it would route to no longer answers for that name, so the write is refused.
            // place() matches an open shard on uuid, and the recreated index's shard is a different one.
            final var operations = new org.opensearch.serverless.shard.ShardOperations(node, mine);
            final var refused = expectThrows(
                Exception.class,
                "a write routed by a stale resolution must not be acknowledged into the previous incarnation",
                () -> operations.index("alpha", "doc", "{\"msg\":\"stale\"}", false)
            );
            logger.info("freshness: the stale route was refused with: {}", refused.toString());
        }
    }

    /**
     * A shard lost to another node is still released, once the head-verification interval comes round.
     *
     * <p>The saving is that the scan does not run on every pass. What must not change is that it runs, and
     * that when it runs it still lets go of what this node no longer owns. Driven by setting the interval
     * rather than by waiting one out.
     */
    public void testALostShardIsStillReleasedWhenTheScanComesRound() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane planeA = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        final MetadataPlane planeB = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        planeA.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("freshness-a"));
            ServerlessNode b = new ServerlessNode(nodeSettings("freshness-b"))
        ) {
            a.start();
            a.setMetadataPlane(planeA);
            // Never scans on its own, so the test says when.
            a.setHeadVerifyIntervalMillis(Long.MAX_VALUE);
            final BackgroundReconciler loopA = new BackgroundReconciler(a, planeA);
            loopA.want("alpha", 0);
            loopA.tick(clock.get());
            final ShardId held = a.reconciler().openShards().iterator().next();
            assertEquals(1, a.reconciler().openShards().size());

            // A's lease expires and B takes the shard.
            b.start();
            clock.set(1_000L + TTL);
            b.activateWriter(planeB, "alpha", 0).orElseThrow();

            // A scan that is not due changes nothing, which is the saving.
            assertTrue("a pass inside the interval must not scan", a.verifyHeadsIfDue(planeA).isEmpty());
            assertTrue("and must therefore still be holding the shard open", a.reconciler().openShards().contains(held));

            // The scan, when it comes, still finds the loss and lets go.
            a.setHeadVerifyIntervalMillis(0L);
            assertTrue("the periodic scan must still release a shard this node has lost", a.verifyHeadsIfDue(planeA).contains(held));
            assertFalse("and the shard must be closed here", a.reconciler().openShards().contains(held));
        }
    }

    /**
     * A pass that is not due still costs nothing per shard; a pass that is due costs one read each.
     *
     * <p>Counted rather than argued, on a filesystem, because the shape is what matters and it is the same
     * shape against a bucket. {@link ServerlessCostTests} takes the same measurement against MinIO.
     */
    public void testAScanThatIsNotDueReadsNoHeads() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        final int shards = 6;
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", shards, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("freshness-count"))) {
            node.start();
            node.setMetadataPlane(plane);
            node.setHeadVerifyIntervalMillis(Long.MAX_VALUE);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            for (int shard = 0; shard < shards; shard++) {
                loop.want("alpha", shard);
            }
            loop.tick(clock.get());
            assertEquals(shards, node.reconciler().openShards().size());

            store.reset();
            node.verifyHeadsIfDue(plane);
            final long skipped = store.registerReads();

            node.setHeadVerifyIntervalMillis(0L);
            store.reset();
            node.verifyHeadsIfDue(plane);
            final long scanned = store.registerReads();

            logger.info("freshness: a skipped pass read {} registers, a scanning pass read {} over {} shards", skipped, scanned, shards);
            // Not zero: a pass that skips the head scan still reads the descriptor of each open index, so
            // a deleted index's shards are let go on the pass that notices rather than on the next scan.
            // The point is that what remains is per *index* and the scan is per *shard*.
            assertTrue("a pass that is not due must not read a register per shard: " + skipped, skipped < shards);
            assertTrue("and a pass that is due must read at least one head per shard: " + scanned, scanned >= shards);
            assertTrue("so the scan is what costs: " + skipped + " against " + scanned, scanned > skipped);
        }
    }

}
