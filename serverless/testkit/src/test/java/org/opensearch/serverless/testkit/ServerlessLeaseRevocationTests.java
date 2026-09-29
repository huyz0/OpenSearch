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
import org.opensearch.serverless.membership.LeaseRevokedException;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A head is never taken from a node without that node being able to tell, from its own renewal.
 *
 * <p>The per-shard head scan existed to catch the case the lease alone misses: a taker whose clock says the
 * owner's lease has expired while the owner's clock says it has not. The taker now revokes the owner's lease
 * register before swapping the head, and the owner's next renewal fails on it and is treated as a lapse. That
 * is what lets the scan run every five minutes instead of on every pass, and these pin both halves and the
 * cost it buys.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessLeaseRevocationTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-revocation")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /**
     * The owner's clock is behind by more than the skew budget, so it believes its lease valid while the
     * taker, by its clock, finds it expired and takes the shard. The owner never scans heads on its own here;
     * its next renewal is the only thing that can tell it, and it must.
     */
    public void testAnOwnerBeyondTheSkewBudgetLearnsOfTheTakeoverFromItsRenewal() throws Exception {
        final AtomicLong ownerClock = new AtomicLong(1_000L);
        final AtomicLong takerClock = new AtomicLong(1_000L);
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane ownerPlane = new MetadataPlane(store, BlobPath.cleanPath(), ownerClock::get, TTL);
        final MetadataPlane takerPlane = new MetadataPlane(store, BlobPath.cleanPath(), takerClock::get, TTL);
        ownerPlane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode owner = new ServerlessNode(nodeSettings("revocation-owner"));
            ServerlessNode taker = new ServerlessNode(nodeSettings("revocation-taker"))
        ) {
            owner.start();
            owner.setMetadataPlane(ownerPlane);
            // Never scans on its own: only the renewal can tell it.
            owner.setHeadVerifyIntervalMillis(Long.MAX_VALUE);
            final BackgroundReconciler loop = new BackgroundReconciler(owner, ownerPlane);
            loop.want("alpha", 0);
            loop.tick(ownerClock.get());
            final ShardId held = owner.reconciler().openShards().iterator().next();

            // The taker's clock runs a whole lease ahead of the owner's.
            taker.start();
            takerClock.set(ownerClock.get() + TTL + 1);
            assertTrue("the taker judges the owner dead and takes the shard", taker.activateWriter(takerPlane, "alpha", 0).isPresent());
            assertTrue(
                "and the owner, by its own clock, still believes its lease valid",
                ownerPlane.membership().selfLeaseValidAt(ownerClock.get())
            );

            final Set<ShardId> released = owner.renewLease(ownerPlane);
            assertTrue("the renewal must reveal the takeover and release the shard: " + released, released.contains(held));
            assertFalse("and the shard must be closed here", owner.reconciler().openShards().contains(held));
        }
    }

    /** A live lease is not revoked; an expired one is, and its owner's next renewal fails on it. */
    public void testOnlyAnExpiredLeaseIsRevokedAndItsRenewalFails() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final NodeLease self = new NodeLease("node-a", "ephemeral-a", "127.0.0.1:9300", Set.of("ingest"), 0L);
        plane.membership().renew(self);

        assertFalse("a live lease must not be revoked", plane.membership().revoke("node-a", "ephemeral-a"));
        plane.membership().renew(self);

        clock.addAndGet(TTL + 1);
        assertTrue("an expired lease is revoked", plane.membership().revoke("node-a", "ephemeral-a"));
        assertTrue("and reads as revoked", plane.membership().read("node-a").orElseThrow().revoked());
        expectThrows(LeaseRevokedException.class, () -> plane.membership().renew(self));
        // The renewal after that starts afresh, over the revocation.
        assertFalse(plane.membership().renew(self).revoked());
        assertFalse(plane.membership().read("node-a").orElseThrow().revoked());
    }

    /**
     * With the production intervals, a node holding shards pays nothing per shard on a pass: no head read,
     * and no descriptor read for an index read within the refresh interval.
     */
    public void testASteadyStatePassCostsNothingPerShard() throws Exception {
        final long few = steadyStateRegisterReads(2);
        final long many = steadyStateRegisterReads(8);
        logger.info("revocation: a steady-state pass read {} registers holding 2 shards, {} holding 8", few, many);
        assertEquals("a pass must cost the same holding eight shards as two", few, many);
        assertEquals("and must read no register at all", 0L, many);
    }

    private long steadyStateRegisterReads(int shards) throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", shards, MAPPING, null));
        try (ServerlessNode node = new ServerlessNode(nodeSettings("revocation-cost-" + shards))) {
            node.start();
            node.setMetadataPlane(plane);
            node.setHeadVerifyIntervalMillis(ServerlessBootstrap.DEFAULT_HEAD_VERIFY_INTERVAL_MILLIS);
            node.setDescriptorRefreshIntervalMillis(ServerlessBootstrap.DEFAULT_DESCRIPTOR_REFRESH_INTERVAL_MILLIS);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            for (int shard = 0; shard < shards; shard++) {
                loop.want("alpha", shard);
            }
            loop.tick(clock.get());
            assertEquals(shards, node.reconciler().openShards().size());
            // The first pass scans and reads; the ones after it, inside both intervals, are what is measured.
            node.verifyHeadsIfDue(plane);
            store.reset();
            for (int pass = 0; pass < 4; pass++) {
                node.verifyHeadsIfDue(plane);
            }
            return store.registerReads();
        }
    }
}
