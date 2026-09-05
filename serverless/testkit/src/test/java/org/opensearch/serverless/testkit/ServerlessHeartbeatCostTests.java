/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.membership.BlobLeaseMembership;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.ShardHead;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * What a heartbeat costs, and what it must not cost: the lease is one write that never waits for a head
 * read, the head scan is one read per held writer shard and no more, the backstop does not renew again
 * behind a timer that already does, and the membership index does not grow with the fleet's history.
 *
 * <p>The lease renewal used to be scheduled only after a pass that read one head per held shard had
 * finished. At object-store latency, a node holding enough shards lapsed its own lease under no fault but
 * arithmetic, and every peer then stole its shards. The numbers here are the ones that were wrong.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessHeartbeatCostTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-heartbeat-cost")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private static MetadataPlane deploymentWith(CountingBlobStore store, AtomicLong clock, int shards) throws Exception {
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", shards, MAPPING, null));
        return plane;
    }

    private static BackgroundReconciler holdingAll(ServerlessNode node, MetadataPlane plane, AtomicLong clock, int shards)
        throws Exception {
        node.start();
        node.setMetadataPlane(plane);
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (int shard = 0; shard < shards; shard++) {
            loop.want("alpha", shard);
        }
        loop.tick(clock.get());
        assertEquals("every shard must be held before anything is measured", shards, node.reconciler().openShards().size());
        return loop;
    }

    private FsBlobContainer container(Path dir) throws Exception {
        return new FsBlobContainer(new FsBlobStore(1024, dir, false), BlobPath.cleanPath(), dir);
    }

    private static NodeLease lease(String id, String address) {
        return new NodeLease(id, id + "-eph", address, Set.of("ingest"), 0L);
    }

    // ---------------------------------------------------------------- the lease never waits

    /** Renewing the lease is one write and reads nothing; verifying the heads is one read per shard and writes nothing. */
    public void testTheLeaseIsRenewedWithoutReadingAHead() throws Exception {
        final int shards = 4;
        final AtomicLong clock = new AtomicLong(1_000L);
        final CountingBlobStore counter = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = deploymentWith(counter, clock, shards);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("hb-lease"))) {
            holdingAll(node, plane, clock, shards);

            counter.reset();
            assertTrue("a live node loses nothing on renewal", node.renewLease(plane).isEmpty());
            assertEquals("the lease is one register write: " + counter.breakdown(), 1L, counter.registerWrites());
            assertEquals("and it reads no head, whatever the node holds: " + counter.breakdown(), 0L, counter.registerReads());

            counter.reset();
            assertTrue("a live node loses nothing on verification", node.verifyHeads(plane).isEmpty());
            assertEquals(
                "one head per held shard and one descriptor per open index: " + counter.breakdown(),
                shards + 1L,
                counter.registerReads()
            );
            assertEquals("verification writes nothing: " + counter.breakdown(), 0L, counter.registerWrites());
        }
    }

    /** More shards than lanes: the scan runs in parallel, and still reads each head exactly once. */
    public void testVerifyingManyHeadsReadsEachExactlyOnce() throws Exception {
        final int shards = ServerlessNode.HEAD_READ_LANES + 4;
        final AtomicLong clock = new AtomicLong(1_000L);
        final CountingBlobStore counter = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = deploymentWith(counter, clock, shards);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("hb-lanes"))) {
            holdingAll(node, plane, clock, shards);
            counter.reset();
            assertTrue(node.verifyHeads(plane).isEmpty());
            assertEquals("each head read once, in lanes: " + counter.breakdown(), shards + 1L, counter.registerReads());
            assertEquals(shards, node.reconciler().openShards().size());
        }
    }

    /** With a renewal timer running, the backstop verifies but does not write the lease a fourth time per TTL. */
    public void testTheBackstopDoesNotRenewWhenATimerDoes() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final CountingBlobStore counter = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = deploymentWith(counter, clock, 2);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("hb-backstop"))) {
            final BackgroundReconciler loop = holdingAll(node, plane, clock, 2);
            loop.setRenewalDrivenByTimer(true);

            counter.reset();
            final BackgroundReconciler.TickResult quiet = loop.tick(clock.addAndGet(1_000L));
            assertTrue("a quiet backstop releases nothing", quiet.released().isEmpty());
            assertEquals("and writes no register when a timer renews the lease: " + counter.breakdown(), 0L, counter.registerWrites());
            assertTrue("but still verifies the heads: " + counter.breakdown(), counter.registerReads() >= 2);

            loop.setRenewalDrivenByTimer(false);
            counter.reset();
            loop.tick(clock.addAndGet(1_000L));
            assertEquals("without a timer the backstop is what renews: " + counter.breakdown(), 1L, counter.registerWrites());
        }
    }

    // ---------------------------------------------------------------- the members index

    /** A lease dead for many lifetimes is pruned from the index and its blob deleted; a recently expired one is kept. */
    public void testLongDeadLeasesArePrunedFromTheIndex() throws Exception {
        final AtomicLong now = new AtomicLong(1_000L);
        final FsBlobContainer c = container(createTempDir());
        final BlobLeaseMembership membership = new BlobLeaseMembership(c, now::get, TTL);

        membership.renew(lease("dead", "127.0.0.1:9301"));
        membership.renew(lease("live", "127.0.0.1:9302"));
        assertEquals(2, membership.refresh().size());

        // Expired, but not for long: kept, because a slow node renews the same register when it returns.
        now.set(1_000L + 2 * TTL);
        membership.renew(lease("live", "127.0.0.1:9302"));
        membership.refresh();
        assertTrue("a recently expired lease is not collected", membership.read("dead").isPresent());

        // Expired for ten lifetimes: gone from the index, and the blob with it.
        now.set(1_000L + (BlobLeaseMembership.DEAD_LEASE_PRUNE_TTLS + 2) * TTL);
        membership.renew(lease("live", "127.0.0.1:9302"));
        final Set<String> ids = membership.refresh().stream().map(NodeLease::nodeId).collect(Collectors.toSet());
        assertEquals(Set.of("live"), ids);
        assertTrue("the dead lease's blob must be deleted", membership.read("dead").isEmpty());
        final String index = c.readRegister(BlobLeaseMembership.MEMBERS).orElseThrow().value().utf8ToString();
        assertFalse("and the index must no longer name it, or every refresh reads it forever: " + index, index.contains("dead"));
    }

    /** A node whose lease blob was collected while it was away re-enrols when it renews. */
    public void testANodeWhoseLeaseWasCollectedReenrolsOnRenewal() throws Exception {
        final AtomicLong now = new AtomicLong(1_000L);
        final FsBlobContainer c = container(createTempDir());
        final BlobLeaseMembership membership = new BlobLeaseMembership(c, now::get, TTL);

        membership.renew(lease("node-a", "127.0.0.1:9301"));
        assertEquals(1, membership.refresh().size());

        // Somebody sweeps the blob; a refresh then finds the id listed with no lease and prunes it.
        c.deleteBlobsIgnoringIfNotExists(List.of(BlobLeaseMembership.LEASE_PREFIX + "node-a"));
        assertEquals(0, membership.refresh().size());
        assertFalse(c.readRegister(BlobLeaseMembership.MEMBERS).orElseThrow().value().utf8ToString().contains("node-a"));

        // The node comes back and renews: the register moved underneath it, so it must enrol again.
        now.addAndGet(1_000L);
        membership.renew(lease("node-a", "127.0.0.1:9301"));
        assertEquals("a live, renewing node must be visible again", 1, membership.refresh().size());
        assertTrue(membership.isEnrolled("node-a"));
    }

    /** A fast restart under the same node id and address renews at once; a live duplicate elsewhere is still refused. */
    public void testAFastRestartAtTheSameAddressRenewsAtOnce() throws Exception {
        final AtomicLong now = new AtomicLong(1_000L);
        final FsBlobContainer c = container(createTempDir());
        new BlobLeaseMembership(c, now::get, TTL).renew(new NodeLease("n", "eph-1", "127.0.0.1:9300", Set.of("ingest"), 0L));

        // Same id, same address, new process: the old lease is unexpired, and it cannot be alive.
        final BlobLeaseMembership restarted = new BlobLeaseMembership(c, now::get, TTL);
        final NodeLease renewed = restarted.renew(new NodeLease("n", "eph-2", "127.0.0.1:9300", Set.of("ingest"), 0L));
        assertEquals("eph-2", renewed.ephemeralId());
        assertEquals("eph-2", restarted.read("n").orElseThrow().ephemeralId());

        // Same id at a different address while the lease is live: a cloned data directory, refused.
        final BlobLeaseMembership clone = new BlobLeaseMembership(c, now::get, TTL);
        final IOException refused = expectThrows(
            IOException.class,
            () -> clone.renew(new NodeLease("n", "eph-3", "10.0.0.9:9300", Set.of("ingest"), 0L))
        );
        assertTrue(refused.getMessage(), refused.getMessage().contains("duplicate node id"));
    }

    // ---------------------------------------------------------------- hints have an age

    /** An owner hint older than a lease is not a hint: the head is re-read rather than trusted. */
    public void testAHintOlderThanALeaseIsNotAHint() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("hb-hint"))) {
            node.start();
            node.setMetadataPlane(plane);
            node.noteHead("alpha", 0, new ShardHead("alpha", 0, 1L, "somebody-else", "eph", 0L, "uuid-alpha-00000000"));
            assertEquals("somebody-else", node.ownerHint("alpha", 0).orElseThrow());

            clock.addAndGet(TTL);
            assertEquals("inside a lease the sighting is still a hint", "somebody-else", node.ownerHint("alpha", 0).orElseThrow());
            clock.incrementAndGet();
            assertTrue("a sighting older than a lease must be re-read, not routed on", node.ownerHint("alpha", 0).isEmpty());
        }
    }
}
