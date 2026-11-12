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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What one small index costs in store requests over its life on a node: taken, written and published, held idle,
 * released, opened cold by a search node. These are the per-index costs a deployment of many small indices pays once
 * per index, and the inputs of the request model for grouping small indices.
 *
 * <p>Off unless {@code -Dtests.meter.small=true}; {@code -Dtests.meter.small.indices} sets the count (50).
 */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*") // many shards need more handles than its file system allows
public class SmallIndexMeterTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final long TICK = 10_000L;
    private static final long HOUR = 3_600_000L;
    private static final long IDLE_AFTER = 300_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name, String roles, boolean packs, boolean logCommits, boolean buckets) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-small-meter")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.commits", logCommits)
            .put("serverless.segments.node_pack.enabled", packs)
            .put("serverless.buckets.enabled", buckets)
            // Staged commits age by the real clock, and the meter's minutes pass in about a second of it: a one-second
            // checkpoint age, waited out once per simulated minute, checkpoints as a minute's age does in service.
            .put(org.opensearch.serverless.shell.ServerlessBootstrap.NODE_LOG_PUBLISH_MAX_AGE, 0L)
            .put(org.opensearch.serverless.shell.ServerlessBootstrap.PUBLISH_MIN_INTERVAL, 0L)
            .put(
                org.opensearch.serverless.shell.LogCommitCheckpoint.SETTING,
                org.opensearch.serverless.store.CommitStaging.MIN_STAGED_MILLIS
            )
            .build();
    }

    private static String name(int i) {
        return String.format(Locale.ROOT, "small-%04d", i);
    }

    /** PUT, LIST and GET per index since the last reset. */
    private static String perIndex(CountingBlobStore store, int indices) {
        final double puts = (store.blobWrites() + store.registerWrites()) / (double) indices;
        final double lists = store.listings() / (double) indices;
        final double gets = (store.blobReads() + store.registerReads()) / (double) indices;
        return String.format(Locale.ROOT, "PUT=%.2f LIST=%.2f GET=%.2f", puts, lists, gets);
    }

    /** Conditional writes of commit registers per index since the last reset: shards' own and buckets'. */
    private static String manifestSwaps(CountingBlobStore store, int indices) {
        return String.format(Locale.ROOT, " (commit register CAS=%.2f)", store.registerWritesMatching("manifest") / (double) indices);
    }

    private void meter(boolean packs, boolean logCommits, boolean buckets, int indices) throws Exception {
        final String mode = (packs ? "node_pack" : "segment_blob") + (logCommits ? " + log commits" : "") + (buckets ? " + buckets" : "");
        final AtomicLong clock = new AtomicLong(1_000_000L);
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            final java.util.Map<String, org.opensearch.serverless.bucket.BucketManifest> manifests = new java.util.TreeMap<>();
            for (int i = 0; i < indices; i++) {
                final IndexDescriptor descriptor = new IndexDescriptor(name(i), "uuid-" + name(i), 1, MAPPING, null);
                final String bucketId = String.format(Locale.ROOT, "b-%04d", i / 256);
                if (buckets) {
                    manifests.merge(
                        bucketId,
                        org.opensearch.serverless.bucket.BucketManifest.empty(bucketId)
                            .withMember(descriptor.uuid(), new org.opensearch.serverless.bucket.BucketManifest.Member(name(i), null)),
                        (a, b) -> a.withMember(descriptor.uuid(), b.member(descriptor.uuid()).orElseThrow())
                    );
                }
                plane.createIndex(buckets ? descriptor.withBucket(bucketId) : descriptor);
            }
            for (org.opensearch.serverless.bucket.BucketManifest manifest : manifests.values()) {
                plane.buckets().swap(manifest, org.opensearch.common.blobstore.BlobRegister.ABSENT_GENERATION).orElseThrow();
            }
            try (
                ServerlessNode writer = new ServerlessNode(nodeSettings("small-writer", "ingest", packs, logCommits, buckets));
                ServerlessNode reader = new ServerlessNode(nodeSettings("small-reader", "search", packs, logCommits, buckets))
            ) {
                writer.setHeadVerifyIntervalMillis(300_000L).setDescriptorRefreshIntervalMillis(300_000L);
                writer.startup().start();
                writer.setMetadataPlane(plane);
                writer.leases().renewLease(plane);
                plane.membership().refresh();
                final BackgroundReconciler loop = new BackgroundReconciler(writer, plane).setIdleAfterMillis(IDLE_AFTER)
                    .setMaxShardsHeld(indices * 2);
                final StringBuilder out = new StringBuilder();

                store.reset();
                for (int i = 0; i < indices; i++) {
                    loop.want(name(i), 0);
                }
                for (int pass = 0; pass < 50 && writer.reconciler().openShards().size() < indices; pass++) {
                    loop.tick(clock.get());
                }
                assertEquals(indices, writer.reconciler().openShards().size());
                out.append("|first take: ").append(perIndex(store, indices));
                // Held from here on as a demand-driven node holds what it was asked for, not as wanted shards, whose
                // routing hints and descriptors a reconciler re-reads on every pass.
                for (int i = 0; i < indices; i++) {
                    loop.stopWanting(name(i), 0);
                }

                store.reset();
                final List<ShardId> held = new ArrayList<>(writer.reconciler().openShards());
                for (ShardId shardId : held) {
                    writer.writes().index(shardId, "1", "{\"msg\":\"small\"}");
                }
                loop.publishAll();
                loop.checkpointDue();
                out.append("|one write + publish: ").append(perIndex(store, indices));

                // An hour of steady writes: a tenth of the indices written each minute, published as a routine publish
                // is, which stages the commit, and checkpointed as it comes due.
                store.reset();
                store.traceCallers(6);
                loop.setIdleAfterMillis(2 * HOUR);
                final java.util.Random random = new java.util.Random(42);
                for (int minute = 0; minute < 60; minute++) {
                    for (int n = 0; n < Math.max(1, held.size() / 10); n++) {
                        writer.writes().index(held.get(random.nextInt(held.size())), "m" + minute, "{\"msg\":\"small\"}");
                    }
                    loop.publishAll();
                    final long publishedAt = System.nanoTime();
                    for (int tick = 0; tick < 6; tick++) {
                        clock.addAndGet(TICK);
                        loop.tick(clock.get());
                    }
                    assertBusy(() -> assertTrue(System.nanoTime() - publishedAt > 1_100_000_000L));
                    loop.checkpointDue();
                }
                loop.setIdleAfterMillis(IDLE_AFTER);
                if (org.opensearch.common.Booleans.parseBoolean(System.getProperty("tests.meter.small.trace", "false"))) {
                    logger.info("SMALL-STEADY|{}", store.byCaller());
                }
                out.append("|10% written per minute, 1 h: ").append(perIndex(store, indices)).append(manifestSwaps(store, indices));

                store.reset();
                store.traceCallers(5);
                loop.setIdleAfterMillis(2 * HOUR);
                for (long t = 0; t < HOUR; t += TICK) {
                    clock.addAndGet(TICK);
                    loop.tick(clock.get());
                }
                loop.setIdleAfterMillis(IDLE_AFTER);
                out.append("|held idle 1 h: ").append(perIndex(store, indices));
                if (org.opensearch.common.Booleans.parseBoolean(System.getProperty("tests.meter.small.trace", "false"))) {
                    logger.info("SMALL-HELD|{}", store.byCaller());
                }

                store.reset();
                clock.addAndGet(IDLE_AFTER + 1);
                loop.tick(clock.get());
                assertEquals("every index should be released", 0, writer.reconciler().openShards().size());
                out.append("|release: ").append(perIndex(store, indices));

                store.reset();
                for (int i = 0; i < indices; i++) {
                    loop.want(name(i), 0);
                }
                for (int pass = 0; pass < 50 && writer.reconciler().openShards().size() < indices; pass++) {
                    loop.tick(clock.get());
                }
                assertEquals(indices, writer.reconciler().openShards().size());
                out.append("|retake after release: ").append(perIndex(store, indices));

                reader.startup().start();
                reader.setMetadataPlane(plane);
                reader.leases().renewLease(plane);
                store.reset();
                for (int i = 0; i < indices; i++) {
                    final ShardId onReader = reader.readers().serveAsReader(plane, name(i), 0);
                    assertTrue(ShardOps.hits(reader.searchService(), onReader, "msg", "small") > 0);
                }
                out.append("|cold reader open + first search: ").append(perIndex(store, indices));
                logger.info("SMALL|{}|{} indices{}", mode, indices, out);
            }
        }
    }

    public void testSmallIndexLifecycleCosts() throws Exception {
        assumeTrue(
            "a meter runs only when -Dtests.meter.small=true",
            org.opensearch.common.Booleans.parseBoolean(System.getProperty("tests.meter.small", "false"))
        );
        final int indices = Integer.parseInt(System.getProperty("tests.meter.small.indices", "50"));
        meter(false, false, false, indices);
        meter(true, true, false, indices);
        meter(true, true, true, indices);
    }
}
