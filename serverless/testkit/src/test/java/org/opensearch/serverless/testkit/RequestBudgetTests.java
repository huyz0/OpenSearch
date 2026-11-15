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
import org.opensearch.serverless.store.IndexUsage;
import org.opensearch.serverless.store.MeteredBlobStore;
import org.opensearch.serverless.store.StoreUsage;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The request and round-trip budgets of {@code serverless/request-budgets.md}, measured through one index's life on a
 * filesystem store and a simulated clock: first take, a write, a publish, an idle hour held, release, a take of what
 * was released, then a cold search on a reader.
 *
 * <p>Every count is held to the table by {@link RequestBudgets#check}: a change that adds a request to one of these
 * paths fails here and names where the request went, and a change that removes one fails until the table records the
 * saving, so it cannot be lost again without a diff to the table.
 *
 * <p>Requests are counted as S3 bills them ({@link CountingBlobStore#impliedS3Requests()}: a compare-and-swap is a GET
 * and a conditional PUT). Round trips are the sequential depth of what a caller waits on
 * ({@link RoundTripDepthBlobStore}), measured with nothing else talking to the store.
 */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class RequestBudgetTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final long TICK = 10_000L;
    private static final long HOUR = 3_600_000L;
    private static final long IDLE_AFTER = 300_000L;
    private static final long ROUND_TRIP_MILLIS = 50L;
    private static final int INDICES = 4;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name, String roles) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-request-budgets")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .build();
    }

    private static String name(int i) {
        return String.format(Locale.ROOT, "budget-%c", (char) ('a' + i));
    }

    /** What a line of per-index usage would cost in S3 requests, counted as {@link CountingBlobStore} counts them. */
    private static long implied(IndexUsage usage) {
        return usage.get(IndexUsage.Kind.REGISTER_READ) + 2 * usage.get(IndexUsage.Kind.REGISTER_WRITE) + usage.get(
            IndexUsage.Kind.BLOB_READ
        ) + usage.get(IndexUsage.Kind.BLOB_WRITE) + usage.get(IndexUsage.Kind.LISTING) + usage.get(IndexUsage.Kind.DELETE);
    }

    /** The test's three views of one store: what S3 would bill, how deep the chains are, and who each request was for. */
    private static final class Meters {
        final RoundTripDepthBlobStore depth;
        final CountingBlobStore counts;
        final MeteredBlobStore metered;

        Meters(FsBlobStore fs) {
            depth = new RoundTripDepthBlobStore(fs);
            counts = new CountingBlobStore(depth);
            metered = new MeteredBlobStore(counts, new StoreUsage(StoreUsage.DEFAULT_MAX_INDICES));
        }

        /** Starts counting afresh; with a round-trip time, also starts measuring depth. */
        void reset(long roundTripMillis) {
            counts.reset();
            counts.traceCallers(5);
            depth.reset(roundTripMillis);
            usageAtReset = snapshot();
        }

        private Map<String, Long> usageAtReset = Map.of();

        private Map<String, Long> snapshot() {
            final Map<String, Long> now = new java.util.HashMap<>();
            for (Map.Entry<String, IndexUsage> line : metered.usage().lines().entrySet()) {
                now.put(line.getKey(), implied(line.getValue()));
            }
            return now;
        }

        /** Requests charged to the budget indices since the last reset, rounded: a shared upload is charged in parts. */
        long indexRequests() {
            return Math.round(indexMilliRequests() / (double) IndexUsage.ONE_REQUEST);
        }

        private long indexMilliRequests() {
            long sum = 0;
            for (Map.Entry<String, Long> line : snapshot().entrySet()) {
                if (line.getKey().startsWith("budget-")) {
                    sum += line.getValue() - usageAtReset.getOrDefault(line.getKey(), 0L);
                }
            }
            return sum;
        }

        /** Requests charged to the node itself since the last reset. */
        long nodeRequests() {
            final long milli = snapshot().get(StoreUsage.NODE) - usageAtReset.getOrDefault(StoreUsage.NODE, 0L);
            return Math.round(milli / (double) IndexUsage.ONE_REQUEST);
        }

        String detail() {
            return counts.breakdown() + "\nby caller:\n" + counts.byCaller();
        }
    }

    public void testIndexLifecycleBudgets() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        final Meters meters = new Meters(new FsBlobStore(8192, createTempDir(), false));
        final StringBuilder report = new StringBuilder();
        try (meters.metered) {
            final MetadataPlane plane = new MetadataPlane(meters.metered, BlobPath.cleanPath(), clock::get, TTL);
            for (int i = 0; i < INDICES; i++) {
                plane.createIndex(new IndexDescriptor(name(i), "uuid-" + name(i), 1, MAPPING, null));
            }
            try (
                ServerlessNode writer = new ServerlessNode(nodeSettings("budget-writer", "ingest"));
                ServerlessNode reader = new ServerlessNode(nodeSettings("budget-reader", "search"))
            ) {
                // A bootstrapped node's housekeeping intervals; a test-built one would otherwise verify heads and
                // refresh descriptors on every renewal.
                writer.setHeadVerifyIntervalMillis(300_000L).setDescriptorRefreshIntervalMillis(300_000L);
                writer.startup().start();
                // The write fence re-reads a descriptor older than a second by the wall clock, which a fast run and a
                // slow one cross at different steps. A window longer than the test makes every run cross none: the
                // writes below are counted as a writer inside its window pays for them.
                writer.fences().setIncarnationFenceMillis(java.util.concurrent.TimeUnit.MINUTES.toMillis(10));
                writer.setMetadataPlane(plane);
                writer.leases().renewLease(plane);
                plane.membership().refresh();
                final BackgroundReconciler loop = new BackgroundReconciler(writer, plane).setIdleAfterMillis(IDLE_AFTER)
                    .setMaxShardsHeld(INDICES * 2);

                meters.reset(0);
                for (int i = 0; i < INDICES; i++) {
                    loop.want(name(i), 0);
                }
                for (int pass = 0; pass < 10 && writer.reconciler().openShards().size() < INDICES; pass++) {
                    loop.tick(clock.get());
                }
                assertEquals(INDICES, writer.reconciler().openShards().size());
                measure(report, "take.fresh.requests", meters.counts.impliedS3Requests(), meters);
                // Held from here as a demand-driven node holds what it was asked for.
                for (int i = 0; i < INDICES; i++) {
                    loop.stopWanting(name(i), 0);
                }
                final List<ShardId> held = new ArrayList<>(writer.reconciler().openShards());
                // In name order, so every run writes first to the same index.
                held.sort(java.util.Comparator.comparing(ShardId::getIndexName));

                // The node's first write opens its log epoch; the next is what every write after it pays.
                meters.reset(ROUND_TRIP_MILLIS);
                writer.writes().index(held.get(0), "first", "{\"msg\":\"budget\"}");
                measure(report, "write.first.requests", meters.counts.impliedS3Requests(), meters);
                measure(report, "write.first.round_trips", meters.depth.depth(), meters);
                meters.reset(ROUND_TRIP_MILLIS);
                writer.writes().index(held.get(0), "next", "{\"msg\":\"budget\"}");
                measure(report, "write.next.requests", meters.counts.impliedS3Requests(), meters);
                measure(report, "write.next.round_trips", meters.depth.depth(), meters);

                meters.reset(0);
                for (ShardId shardId : held) {
                    writer.writes().index(shardId, "1", "{\"msg\":\"budget\"}");
                }
                loop.publishAll();
                measure(report, "publish.requests", meters.counts.impliedS3Requests(), meters);

                // An hour with every shard held and nothing written or searched.
                loop.setIdleAfterMillis(2 * HOUR);
                meters.reset(0);
                for (long t = 0; t < HOUR; t += TICK) {
                    clock.addAndGet(TICK);
                    loop.tick(clock.get());
                }
                measure(report, "held_idle.index_requests_per_hour", meters.indexRequests(), meters);
                measure(report, "held_idle.node_requests_per_hour", meters.nodeRequests(), meters);
                loop.setIdleAfterMillis(IDLE_AFTER);

                meters.reset(0);
                clock.addAndGet(IDLE_AFTER + 1);
                loop.tick(clock.get());
                assertEquals("every index should be released", 0, writer.reconciler().openShards().size());
                measure(report, "release.requests", meters.counts.impliedS3Requests(), meters);

                meters.reset(ROUND_TRIP_MILLIS);
                loop.want(name(0), 0);
                loop.tick(clock.get());
                assertEquals(1, writer.reconciler().openShards().size());
                measure(report, "take.released.requests", meters.counts.impliedS3Requests(), meters);
                measure(report, "take.released.round_trips", meters.depth.depth(), meters);
                loop.stopWanting(name(0), 0);

                reader.startup().start();
                reader.setMetadataPlane(plane);
                reader.leases().renewLease(plane);
                meters.reset(ROUND_TRIP_MILLIS);
                final ShardId onReader = reader.readers().serveAsReader(plane, name(1), 0);
                assertTrue(ShardOps.hits(reader.searchService(), onReader, "msg", "budget") > 0);
                measure(report, "search.cold.requests", meters.counts.impliedS3Requests(), meters);
                measure(report, "search.cold.round_trips", meters.depth.depth(), meters);

                meters.reset(ROUND_TRIP_MILLIS);
                assertTrue(ShardOps.hits(reader.searchService(), onReader, "msg", "budget") > 0);
                measure(report, "search.warm.requests", meters.counts.impliedS3Requests(), meters);
            }
        } finally {
            logger.info("BUDGETS|{}", report);
        }
    }

    private static void measure(StringBuilder report, String key, long measured, Meters meters) {
        report.append(String.format(Locale.ROOT, "%n%s=%d", key, measured));
        if (RequestBudgets.recording()) {
            report.append('\n').append(meters.detail());
        }
        RequestBudgets.check(key, measured, meters.detail());
    }
}
