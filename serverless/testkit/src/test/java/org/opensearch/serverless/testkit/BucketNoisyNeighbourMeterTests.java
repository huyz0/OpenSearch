/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.bucket.BucketManifest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Whether a hot member slows a quiet one beside it in a full bucket more than it does when both are indices of their
 * own on the same node: the quiet index's write and search latency while the hot one takes writes as fast as it can,
 * and the bucket checkpoints every second.
 *
 * <p>Off unless {@code -Dtests.meter.bucket_noisy=true}; {@code -Dtests.meter.bucket_noisy.members} sets the bucket's
 * size (1024).
 */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*")
public class BucketNoisyNeighbourMeterTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int ROUNDS = 400;
    private static final int HOT_WRITERS = 64;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-bucket-noisy")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.commits", true)
            .put("serverless.segments.node_pack.enabled", true)
            .put("serverless.buckets.enabled", true)
            .put(org.opensearch.serverless.shell.ServerlessBootstrap.NODE_LOG_PUBLISH_MAX_AGE, 0L)
            .put(org.opensearch.serverless.shell.ServerlessBootstrap.PUBLISH_MIN_INTERVAL, 0L)
            .put(
                org.opensearch.serverless.shell.LogCommitCheckpoint.SETTING,
                org.opensearch.serverless.store.CommitStaging.MIN_STAGED_MILLIS
            )
            .build();
    }

    private static double percentile(List<Double> sorted, double p) {
        return sorted.get(Math.min(sorted.size() - 1, (int) Math.ceil(p * sorted.size()) - 1));
    }

    private String measure(String mode, int bucketMembers) throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            final String bucketId = "s04-1";
            final String[] names = { "hot", "quiet" };
            if (bucketMembers > 0) {
                BucketManifest padded = BucketManifestCodecMeterTests.bucket(bucketMembers - names.length, 8, 3);
                for (String name : names) {
                    padded = padded.withMember("uuid-" + name, new BucketManifest.Member(name, null));
                }
                plane.buckets().swap(new BucketManifest(bucketId, 0, padded.members()), BlobRegister.ABSENT_GENERATION).orElseThrow();
            }
            for (String name : names) {
                final IndexDescriptor descriptor = new IndexDescriptor(name, "uuid-" + name, 1, MAPPING, null);
                plane.createIndex(bucketMembers > 0 ? descriptor.withBucket(bucketId) : descriptor);
            }
            try (ServerlessNode writer = new ServerlessNode(nodeSettings("noisy-writer"))) {
                writer.startup().start();
                writer.setMetadataPlane(plane);
                writer.leases().renewLease(plane);
                plane.membership().refresh();
                final BackgroundReconciler loop = new BackgroundReconciler(writer, plane);
                for (String name : names) {
                    loop.want(name, 0);
                }
                for (int pass = 0; pass < 20 && writer.reconciler().openShards().size() < names.length; pass++) {
                    loop.tick(clock.get());
                }
                ShardId hot = null;
                ShardId quiet = null;
                for (ShardId shardId : writer.reconciler().openShards()) {
                    if (shardId.getIndexName().equals("hot")) {
                        hot = shardId;
                    } else {
                        quiet = shardId;
                    }
                }
                // A first publish each, so later ones stage and checkpoint as routine publishes do.
                writer.writes().index(hot, "seed", "{\"msg\":\"seed\"}");
                writer.writes().index(quiet, "seed", "{\"msg\":\"seed\"}");
                loop.publishAll();

                final AtomicBoolean running = new AtomicBoolean(true);
                final ShardId hotShard = hot;
                final AtomicLong hotWrites = new AtomicLong();
                // Many writers on the hot member: each write waits out the node log's window, so one would barely load it.
                final List<Thread> noise = new ArrayList<>();
                for (int t = 0; t < HOT_WRITERS; t++) {
                    final int thread = t;
                    noise.add(new Thread(() -> {
                        long n = 0;
                        while (running.get()) {
                            try {
                                writer.writes().index(hotShard, "h" + thread + "-" + n++, "{\"msg\":\"hot hot hot " + n + "\"}");
                                hotWrites.incrementAndGet();
                            } catch (Exception e) {
                                return;
                            }
                        }
                    }, "hot-member-" + t));
                }
                noise.forEach(Thread::start);
                final List<Double> writes = new ArrayList<>();
                final List<Double> searches = new ArrayList<>();
                long lastCheckpoint = System.nanoTime();
                try {
                    for (int round = 0; round < ROUNDS; round++) {
                        long start = System.nanoTime();
                        writer.writes().index(quiet, "q" + round, "{\"msg\":\"quiet " + round + "\"}");
                        writes.add((System.nanoTime() - start) / 1e6);
                        start = System.nanoTime();
                        ShardOps.hits(writer.searchService(), quiet, "msg", "quiet");
                        searches.add((System.nanoTime() - start) / 1e6);
                        if (round % 20 == 19) {
                            loop.publishAll();
                        }
                        if (System.nanoTime() - lastCheckpoint > 1_100_000_000L) {
                            loop.checkpointDue();
                            lastCheckpoint = System.nanoTime();
                        }
                    }
                } finally {
                    running.set(false);
                    for (Thread thread : noise) {
                        thread.join();
                    }
                }
                writes.sort(Double::compare);
                searches.sort(Double::compare);
                return String.format(
                    Locale.ROOT,
                    "|%s: quiet write p50 %.1f p99 %.1f ms, search p50 %.1f p99 %.1f ms, hot writes %d",
                    mode,
                    percentile(writes, 0.5),
                    percentile(writes, 0.99),
                    percentile(searches, 0.5),
                    percentile(searches, 0.99),
                    hotWrites.get()
                );
            }
        }
    }

    public void testAHotMemberAndAQuietOne() throws Exception {
        assumeTrue(
            "a meter runs only when -Dtests.meter.bucket_noisy=true",
            org.opensearch.common.Booleans.parseBoolean(System.getProperty("tests.meter.bucket_noisy", "false"))
        );
        final int members = Integer.parseInt(System.getProperty("tests.meter.bucket_noisy.members", "1024"));
        final StringBuilder out = new StringBuilder();
        // Twice each, alternating, so a JIT or disk-cache warm-up does not land on one mode only.
        out.append(measure("own indices", 0));
        out.append(measure(members + "-member bucket", members));
        out.append(measure("own indices", 0));
        out.append(measure(members + "-member bucket", members));
        logger.info("BUCKET-NOISY{}", out);
    }
}
