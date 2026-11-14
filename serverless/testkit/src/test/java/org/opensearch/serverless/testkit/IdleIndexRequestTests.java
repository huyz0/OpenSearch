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
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

// ExtrasFS may plant a stray directory among the shard containers of one run's store and not the other's: the orphan
// sweep then visits it in place of an index, and the two hours differ by the descriptor read it did not make.
/**
 * An index nobody uses costs nothing: once its shards are released, an hour of a node's periodic work makes the same
 * store requests whether the deployment holds a few idle indices or many.
 *
 * <p>At tens of millions of mostly idle indices, one request per index per few minutes is a large bill on its own, so
 * every periodic pass (renewal, head checks, the janitor, the orphan, reclaim and tombstone sweeps, view reaping) must
 * scale with what is held or what changed, never with how many indices exist.
 */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class IdleIndexRequestTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final long IDLE_AFTER = 300_000L;
    private static final long TICK = 10_000L;
    private static final long SWEEP_EVERY = 60_000L;
    private static final long HOUR = 3_600_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-idle-requests")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private static String name(int i) {
        return String.format(Locale.ROOT, "idle-%04d", i);
    }

    /** Requests one simulated hour of periodic work makes once {@code indices} written indices have gone idle. */
    private long idleHour(int indices, int orphanSliceBudget, StringBuilder report) throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            for (int i = 0; i < indices; i++) {
                plane.createIndex(new IndexDescriptor(name(i), "uuid-" + name(i), 1, MAPPING, null));
            }
            try (ServerlessNode node = new ServerlessNode(nodeSettings("idle-" + indices))) {
                // Realistic housekeeping intervals: a test-built node otherwise verifies heads and refreshes
                // descriptors on every renewal, which a bootstrapped node does every five minutes.
                node.setHeadVerifyIntervalMillis(300_000L).setDescriptorRefreshIntervalMillis(300_000L);
                node.startup().start();
                node.setMetadataPlane(plane);
                node.leases().renewLease(plane);
                plane.membership().refresh();
                final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setIdleAfterMillis(IDLE_AFTER)
                    .setMaxShardsHeld(indices * 2);
                loop.setOrphanSliceBudget(orphanSliceBudget);
                for (int i = 0; i < indices; i++) {
                    loop.want(name(i), 0);
                }
                for (int pass = 0; pass < 50 && node.reconciler().openShards().size() < indices; pass++) {
                    loop.tick(clock.get());
                }
                assertEquals(indices, node.reconciler().openShards().size());
                for (var shardId : node.reconciler().openShards()) {
                    node.writes().index(shardId, "1", "{\"msg\":\"once\"}");
                }
                loop.publishAll();
                for (int i = 0; i < indices; i++) {
                    loop.stopWanting(name(i), 0);
                }
                // Let every index go idle, be released, and let the sweeps catch up with what was published.
                for (long t = 0; t < IDLE_AFTER + 4 * SWEEP_EVERY; t += TICK) {
                    clock.addAndGet(TICK);
                    loop.tick(clock.get());
                    if (t % SWEEP_EVERY == 0) {
                        sweep(loop);
                    }
                }
                assertEquals("every idle index should have been released", 0, node.reconciler().openShards().size());

                store.reset();
                store.traceCallers(6);
                // Only the reconciler's own cadence from here: each sweep runs as often as its pass schedules it.
                for (long t = 0; t < HOUR; t += TICK) {
                    clock.addAndGet(TICK);
                    loop.tick(clock.get());
                    settle(loop);
                }
                report.append(
                    String.format(
                        Locale.ROOT,
                        "%n%d idle indices: %d requests (%s)%n%s",
                        indices,
                        store.total(),
                        store.breakdown(),
                        store.byCaller()
                    )
                );
                return store.total();
            }
        }
    }

    private static void settle(BackgroundReconciler loop) throws Exception {
        loop.orphanSlice().get();
        loop.reclaimSweep().get();
        loop.tombstoneSweep().get();
    }

    private static void sweep(BackgroundReconciler loop) throws Exception {
        loop.sweepAbandoned(BackgroundReconciler.DEFAULT_JANITOR_BUDGET, BackgroundReconciler.DEFAULT_JANITOR_REPLAYS);
        loop.orphanSlice().get();
        loop.reclaimSweep().get();
        loop.tombstoneSweep().get();
        loop.reapExpiredViews();
    }

    /**
     * The hour costs the same with ten times the idle indices. The orphan sweep is left out here and bounded below: it
     * is the one pass that visits shard containers, and it visits a fixed number per node per hour.
     */
    public void testIdleIndicesMakeNoPeriodicRequests() throws Exception {
        final StringBuilder report = new StringBuilder();
        final long few = idleHour(4, 0, report);
        final long many = idleHour(40, 0, report);
        logger.info("IDLE|{}", report);
        assertEquals("an idle index must cost no periodic store requests; per-hour totals differ:" + report, few, many);
    }

    /** The orphan backstop's hour costs its budget, not the number of indices: past the budget, more indices add nothing. */
    public void testTheOrphanSweepCostsItsBudgetNotTheIndexCount() throws Exception {
        final StringBuilder report = new StringBuilder();
        final int budget = 5;
        final long few = idleHour(20, budget, report);
        final long many = idleHour(40, budget, report);
        logger.info("IDLE-ORPHAN|{}", report);
        assertEquals("the orphan sweep's hour should cost its budget whatever the index count:" + report, few, many);
    }
}
