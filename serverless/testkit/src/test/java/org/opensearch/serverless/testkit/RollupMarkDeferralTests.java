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
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.DescriptorStore;
import org.opensearch.serverless.metadata.DigestRollups;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.RollupEntry;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.PruningDigest;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A writer's publishes skip the digest rollup while its mark stands, and the rollup still covers every published
 * document by the time the shard can be ruled out.
 *
 * <p>A shard a writer marks is never ruled out of a search, so widening its rollup entry on each publish bought
 * nothing and cost a register read and swap per publish. The entry now catches up when the mark is cleared: by the
 * writer letting the shard go, or by a survivor settling a dead writer's shard. Both are pinned here, because a clear
 * that left the entry narrower than the published commit would make wide searches skip documents.
 */
public class RollupMarkDeferralTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-rollup-deferral")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private static IndexDescriptor descriptor(String index) {
        return new IndexDescriptor(index, "uuid-" + index + "-000000000000".substring(index.length()), 1, MAPPING, null);
    }

    private static RollupEntry entryOf(MetadataPlane plane, String index) throws Exception {
        return plane.rollups().readGroup(DigestRollups.group(index), new DescriptorStore.Reads() {
            @Override
            public <T> List<T> runAll(List<Callable<T>> tasks) {
                final List<T> out = new ArrayList<>();
                for (Callable<T> task : tasks) {
                    try {
                        out.add(task.call());
                    } catch (Exception e) {
                        out.add(null);
                    }
                }
                return out;
            }
        }).get(index);
    }

    /** How many requests went to the rollup registers since the store's counters were last reset. */
    private static long rollupRequests(CountingBlobStore store) {
        long total = 0;
        for (String line : store.byPlace().split("\n")) {
            if (line.contains(" rollups/")) {
                total += Long.parseLong(line.substring(0, line.indexOf(' ')));
            }
        }
        return total;
    }

    private static void assertCoversThePublishedRange(RollupEntry entry, long nowMillis) {
        assertNotNull("the index keeps its entry", entry);
        assertEquals("the mark is cleared", 0L, entry.states().get(0).ownerTerm());
        assertNotNull("and the entry carries a digest", entry.states().get(0).digest());
        assertFalse(
            "a search that matches a published document must not be ruled out: " + entry,
            entry.rulesOut(QueryBuilders.rangeQuery("n").gte(0).lte(10), nowMillis)
        );
        assertFalse(
            "nor one that matches only the last document published: " + entry,
            entry.rulesOut(QueryBuilders.rangeQuery("n").gte(5).lte(5), nowMillis)
        );
        assertTrue(
            "a search outside every published value is ruled out: " + entry,
            entry.rulesOut(QueryBuilders.rangeQuery("n").gte(1_000_000), nowMillis)
        );
    }

    private static PruningDigest n(long min, long max) {
        return new PruningDigest(Map.of("n", new PruningDigest.FieldRange("long", min, max, null, null)));
    }

    /** Only the publishing writer's own mark lets it skip the rollup: a successor's higher mark does not. */
    public void testOnlyTheWritersOwnMarkSkipsTheRollup() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final DigestRollups rollups = new DigestRollups(store, BlobPath.cleanPath().add("rollups"));
            rollups.markOwned("logs-a", "uuid-a", 1, 0, 3L);

            store.reset();
            rollups.widenUnlessMarked("logs-a", "uuid-a", 1, 0, 3L, n(100, 200));
            assertEquals("under its own mark a writer's publish makes no request", 0L, store.total());

            // A successor marks the shard at a higher term. The writer at 3 is now stale, and its publish must widen.
            rollups.markOwned("logs-a", "uuid-a", 1, 0, 4L);
            store.reset();
            rollups.widenUnlessMarked("logs-a", "uuid-a", 1, 0, 3L, n(100, 200));
            assertTrue("a writer whose mark was superseded widens the entry", store.registerWrites() > 0L);

            // And a clear by the successor keeps what the stale writer published.
            rollups.clearOwned("logs-a", "uuid-a", 0, 4L, n(300, 400));
            final RollupEntry entry = rollups.readGroup(DigestRollups.group("logs-a"), new DescriptorStore.Reads() {
                @Override
                public <T> List<T> runAll(List<Callable<T>> tasks) throws InterruptedException {
                    final List<T> out = new ArrayList<>();
                    for (Callable<T> task : tasks) {
                        try {
                            out.add(task.call());
                        } catch (Exception e) {
                            out.add(null);
                        }
                    }
                    return out;
                }
            }).get("logs-a");
            assertFalse(entry.rulesOut(QueryBuilders.rangeQuery("n").gte(150).lte(150), 0L));
            assertFalse(entry.rulesOut(QueryBuilders.rangeQuery("n").gte(350).lte(350), 0L));
            assertTrue(entry.rulesOut(QueryBuilders.rangeQuery("n").gte(1_000_000), 0L));
        }
    }

    public void testPublishesUnderAWritersMarkSkipTheRollupUntilItLetsGo() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            plane.createIndex(descriptor("deferred"));
            try (ServerlessNode node = new ServerlessNode(nodeSettings("rollup-deferral-owner"))) {
                node.startup().start();
                node.setMetadataPlane(plane);
                node.leases().renewLease(plane);
                final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true);
                loop.activateOnDemand(List.of(Map.entry("deferred", 0)));
                final ShardId shardId = node.reconciler().openShards().iterator().next();
                assertTrue("marked before it opened", entryOf(plane, "deferred").states().get(0).ownerTerm() > 0L);

                store.reset();
                for (int n = 1; n <= 5; n++) {
                    // Each value widens the digest, so every one of these publishes used to swap the rollup.
                    node.writes().index(shardId, "doc-" + n, "{\"msg\":\"deferred\",\"n\":" + n + "}");
                    loop.markDirty(shardId);
                    assertEquals(1, loop.publishDirty().size());
                }
                assertEquals("publishes under the mark make no rollup request:\n" + store.byPlace(), 0L, rollupRequests(store));

                loop.setIdleAfterMillis(1);
                loop.releaseIdle(System.currentTimeMillis() + 60_000L);
                assertTrue("given back", node.reconciler().openShards().isEmpty());
                assertCoversThePublishedRange(entryOf(plane, "deferred"), clock.get());
            }
        }
    }

    public void testADeadWritersPublishesAreCoveredOnceASurvivorSettlesItsShard() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final FsBlobStore store = new FsBlobStore(8192, createTempDir(), false);
        final MetadataPlane setup = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        setup.createIndex(descriptor("orphaned"));
        try (
            ServerlessNode dying = new ServerlessNode(nodeSettings("rollup-deferral-dying"));
            ServerlessNode survivor = new ServerlessNode(nodeSettings("rollup-deferral-survivor"))
        ) {
            final MetadataPlane dyingPlane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            dying.startup().start();
            dying.setMetadataPlane(dyingPlane);
            dying.leases().renewLease(dyingPlane);
            final BackgroundReconciler dyingLoop = new BackgroundReconciler(dying, dyingPlane).setDemandDrivenActivation(true);
            dyingLoop.activateOnDemand(List.of(Map.entry("orphaned", 0)));
            final ShardId shardId = dying.reconciler().openShards().iterator().next();
            for (int n = 1; n <= 5; n++) {
                dying.writes().index(shardId, "doc-" + n, "{\"msg\":\"orphaned\",\"n\":" + n + "}");
                dyingLoop.markDirty(shardId);
                assertEquals(1, dyingLoop.publishDirty().size());
            }
            final String dead = dying.localNode().getId();
            final RollupEntry marked = entryOf(setup, "orphaned");
            assertTrue("still marked by the writer that died", marked.states().get(0).ownerTerm() > 0L);
            assertNull("and its publishes never widened the entry", marked.states().get(0).digest());

            // The writer never lets go; its lease runs out on the shared clock.
            clock.addAndGet(2 * TTL);
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            survivor.startup().start();
            survivor.setMetadataPlane(plane);
            survivor.leases().renewLease(plane);
            plane.membership().refresh();
            assertEquals(MetadataPlane.Settled.RELEASED_CLEAN, plane.settleAbandoned("orphaned", 0, dead));
            assertCoversThePublishedRange(entryOf(plane, "orphaned"), clock.get());
        }
    }
}
