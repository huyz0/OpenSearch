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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What recording commits with the node log costs and saves, against writing each shard's manifest on every publish, for
 * both segment placements: write acknowledgement latency, requests per second while every shard publishes once a second,
 * and what a cold reader then reads and waits. Every store request takes {@value #REQUEST_MILLIS} ms, and one in
 * {@value #TAIL_EVERY} takes {@value #TAIL_MILLIS} ms.
 */
public class NodeLogCommitsMeterTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final long REQUEST_MILLIS = 20;
    private static final int TAIL_EVERY = 50;
    private static final long TAIL_MILLIS = 200;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";
    private static final int SHARDS = Integer.parseInt(System.getProperty("meter.shards", "100"));
    private static final int SECONDS = Integer.parseInt(System.getProperty("meter.seconds", "10"));
    private static final int WRITERS = 16;
    private static final int COLD_SHARDS = 20;

    private Settings nodeSettings(String name, String roles, boolean packs, boolean logCommits) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-commits-meter")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.window_millis", 250)
            .put("serverless.wal.node_log.commits", logCommits)
            .put("serverless.segments.node_pack.enabled", packs)
            .put("serverless.segments.node_pack.linger_millis", 250)
            .build();
    }

    private static String name(int i) {
        return String.format(Locale.ROOT, "tenant-%04d", i);
    }

    private static double percentile(List<Long> sortedNanos, double p) {
        if (sortedNanos.isEmpty()) {
            return 0;
        }
        final int at = (int) Math.min(sortedNanos.size() - 1, Math.ceil(p * sortedNanos.size()) - 1);
        return sortedNanos.get(Math.max(0, at)) / 1e6;
    }

    private void measure(boolean packs, boolean logCommits) throws Exception {
        final String mode = (packs ? "node_pack" : "segment_blob") + (logCommits ? " + log commits" : " + manifest per publish");
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final LatencyInjectingBlobStore slow = new LatencyInjectingBlobStore(new FsBlobStore(8192, createTempDir(), false), 0);
        try (CountingBlobStore store = new CountingBlobStore(slow)) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            for (int i = 0; i < SHARDS; i++) {
                plane.createIndex(new IndexDescriptor(name(i), "uuid-" + name(i), 1, MAPPING, null));
            }
            final ExecutorService pool = Executors.newFixedThreadPool(WRITERS + 1);
            try (
                ServerlessNode writer = new ServerlessNode(nodeSettings("meter-writer", "ingest", packs, logCommits));
                ServerlessNode reader = new ServerlessNode(nodeSettings("meter-reader", "search", packs, logCommits))
            ) {
                writer.startup().start();
                writer.setMetadataPlane(plane);
                writer.leases().renewLease(plane);
                reader.startup().start();
                reader.setMetadataPlane(plane);
                reader.leases().renewLease(plane);
                final BackgroundReconciler loop = new BackgroundReconciler(writer, plane).setMaxShardsHeld(SHARDS * 2);
                for (int i = 0; i < SHARDS; i++) {
                    loop.want(name(i), 0);
                }
                loop.tick(clock.get());
                final List<ShardId> held = new ArrayList<>(writer.reconciler().openShards());
                assertEquals(SHARDS, held.size());
                for (ShardId shardId : held) {
                    writer.writes().index(shardId, "seed", "{\"msg\":\"meter\",\"n\":0}");
                }
                loop.publishAll();

                slow.setDelayMillis(REQUEST_MILLIS);
                slow.setTail(TAIL_EVERY, TAIL_MILLIS);
                store.reset();
                final AtomicBoolean running = new AtomicBoolean(true);
                final ConcurrentLinkedQueue<Long> acks = new ConcurrentLinkedQueue<>();
                final List<Future<?>> writing = new ArrayList<>();
                for (int w = 0; w < WRITERS; w++) {
                    final int offset = w;
                    writing.add(pool.submit(() -> {
                        long n = 0;
                        while (running.get()) {
                            final ShardId shardId = held.get((int) ((offset + n * WRITERS) % held.size()));
                            final long started = System.nanoTime();
                            writer.writes().index(shardId, "w" + offset + "-" + n, "{\"msg\":\"meter\",\"n\":" + n + "}");
                            acks.add(System.nanoTime() - started);
                            n++;
                        }
                        return null;
                    }));
                }
                final List<Long> passes = new ArrayList<>();
                final long started = System.nanoTime();
                int published = 0;
                for (int second = 0; second < SECONDS; second++) {
                    final long passStarted = System.nanoTime();
                    published += loop.publishAll().size();
                    loop.checkpointDue();
                    final long took = System.nanoTime() - passStarted;
                    passes.add(took);
                    final long rest = TimeUnit.SECONDS.toNanos(1) - took;
                    if (rest > 0) {
                        TimeUnit.NANOSECONDS.sleep(rest);
                    }
                }
                running.set(false);
                for (Future<?> each : writing) {
                    each.get();
                }
                final double elapsed = (System.nanoTime() - started) / 1e9;
                final long puts = store.blobWrites() + store.registerWrites();
                final long lists = store.listings();
                final long gets = store.blobReads() + store.registerReads();
                final List<Long> sortedAcks = new ArrayList<>(acks);
                Collections.sort(sortedAcks);
                Collections.sort(passes);

                // A cold reader opens some of the shards and runs its first search on each.
                store.reset();
                long firstNanos = 0;
                for (ShardId shardId : held.subList(0, COLD_SHARDS)) {
                    final long openStarted = System.nanoTime();
                    final ShardId onReader = reader.readers().serveAsReader(plane, shardId.getIndexName(), 0);
                    assertTrue(ShardOps.hits(reader.searchService(), onReader, "msg", "meter") > 0);
                    firstNanos += System.nanoTime() - openStarted;
                }
                final double coldGets = (store.blobReads() + store.registerReads()) / (double) COLD_SHARDS;

                logger.info(
                    "COMMITSMETER|{}|{} shards, {} s, {} ms per request, 1 in {} at {} ms|writes acked={} p50={} ms p99={} ms"
                        + "|shard publishes={} ({}/s)|publish pass p50={} ms max={} ms"
                        + "|PUT/s={} LIST/s={} GET/s={} PUT+LIST per shard publish={}"
                        + "|cold open: {} GET per shard, first search {} ms",
                    mode,
                    SHARDS,
                    SECONDS,
                    REQUEST_MILLIS,
                    TAIL_EVERY,
                    TAIL_MILLIS,
                    sortedAcks.size(),
                    String.format(Locale.ROOT, "%.1f", percentile(sortedAcks, 0.50)),
                    String.format(Locale.ROOT, "%.1f", percentile(sortedAcks, 0.99)),
                    published,
                    String.format(Locale.ROOT, "%.0f", published / elapsed),
                    String.format(Locale.ROOT, "%.0f", percentile(passes, 0.50)),
                    String.format(Locale.ROOT, "%.0f", percentile(passes, 1.0)),
                    String.format(Locale.ROOT, "%.1f", puts / elapsed),
                    String.format(Locale.ROOT, "%.1f", lists / elapsed),
                    String.format(Locale.ROOT, "%.1f", gets / elapsed),
                    String.format(Locale.ROOT, "%.3f", published == 0 ? 0 : (puts + lists) / (double) published),
                    String.format(Locale.ROOT, "%.1f", coldGets),
                    String.format(Locale.ROOT, "%.1f", firstNanos / 1e6 / COLD_SHARDS)
                );
                slow.setDelayMillis(0);
                slow.setTail(0, 0);
            } finally {
                pool.shutdownNow();
            }
        }
    }

    public void testLogCommitsAgainstAManifestPerPublish() throws Exception {
        measure(false, false);
        measure(false, true);
        measure(true, false);
        measure(true, true);
    }
}
