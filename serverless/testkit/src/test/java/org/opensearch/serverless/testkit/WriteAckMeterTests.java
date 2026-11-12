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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Measures how long a write takes to be acknowledged against a slow store, and where that time goes: each step of the
 * write path, and the node log's window, the blob ahead of it, and its PUT.
 *
 * <p>Two loads. <b>Closed</b>: a fixed number of writers, each writing again as soon as its last write is acknowledged,
 * as the soak does; their writes arrive together just after a window lands, so each waits out a whole window. <b>Open</b>:
 * writes arrive at random times at a fixed rate whatever is acknowledged, as many independent clients would, so a write
 * arrives at a random point of the window.
 *
 * <p>Off unless {@code -Dtests.ack.seconds} is set. {@code -Dtests.ack.shards} sets the shard count (200),
 * {@code -Dtests.ack.rate} the open load's writes per second (100), {@code -Dtests.ack.modes} a subset of {@code closed,open},
 * and {@code -Dtests.ack.settings} extra node settings as {@code key=value,key=value}.
 */
@com.carrotsearch.randomizedtesting.annotations.TimeoutSuite(millis = 1_800_000)
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*") // hundreds of shards need more handles than its file system allows
public class WriteAckMeterTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final long REQUEST_MILLIS = 20;
    private static final int TAIL_EVERY = 50;
    private static final long TAIL_MILLIS = 200;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";
    private static final int CLOSED_WRITERS = 16;

    private Settings nodeSettings(String extra) {
        final Settings.Builder builder = Settings.builder()
            .put("node.name", "ack-writer")
            .put("cluster.name", "serverless-ack")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.window_millis", 250);
        for (String pair : extra.split(",")) {
            if (pair.isBlank() == false) {
                final String[] kv = pair.split("=", 2);
                builder.put(kv[0].trim(), kv[1].trim());
            }
        }
        return builder.build();
    }

    private static String name(int i) {
        return String.format(Locale.ROOT, "ack-%04d", i);
    }

    private static double percentile(List<Long> sortedNanos, double p) {
        if (sortedNanos.isEmpty()) {
            return 0;
        }
        final int at = (int) Math.min(sortedNanos.size() - 1, Math.ceil(p * sortedNanos.size()) - 1);
        return sortedNanos.get(Math.max(0, at)) / 1e6;
    }

    private void measure(String mode, int shards, long seconds, double rate, String extra) throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final Path root = createTempDir();
        final LatencyInjectingBlobStore slow = new LatencyInjectingBlobStore(new FsBlobStore(8192, root, false), 0);
        try (CountingBlobStore store = new CountingBlobStore(slow)) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            for (int i = 0; i < shards; i++) {
                plane.createIndex(new IndexDescriptor(name(i), "uuid-" + name(i), 1, MAPPING, null));
            }
            try (ServerlessNode node = new ServerlessNode(nodeSettings(extra))) {
                node.startup().start();
                node.setMetadataPlane(plane);
                node.leases().renewLease(plane);
                plane.membership().refresh();
                final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setMaxShardsHeld(shards * 2);
                for (int i = 0; i < shards; i++) {
                    loop.want(name(i), 0);
                }
                for (int pass = 0; pass < 50 && node.reconciler().openShards().size() < shards; pass++) {
                    loop.tick(clock.get());
                }
                final List<ShardId> held = new ArrayList<>(node.reconciler().openShards());
                assertEquals(shards, held.size());
                // One write to each shard first, so every shard is admitted to the log's epoch before timing starts.
                for (ShardId shardId : held) {
                    node.writes().index(shardId, "warm", "{\"msg\":\"warm\",\"n\":0}");
                }
                slow.setDelayMillis(REQUEST_MILLIS);
                slow.setTail(TAIL_EVERY, TAIL_MILLIS);
                store.reset();
                node.writes().ackTimings().reset();
                node.nodeLogWriter().committer().timings().reset();

                final ConcurrentLinkedQueue<Long> acks = new ConcurrentLinkedQueue<>();
                final AtomicReference<Throwable> failed = new AtomicReference<>();
                final AtomicBoolean running = new AtomicBoolean(true);
                final ExecutorService pool = Executors.newFixedThreadPool(mode.equals("closed") ? CLOSED_WRITERS : 256);
                final List<Future<?>> writing = new ArrayList<>();
                final long started = System.nanoTime();
                final long end = started + TimeUnit.SECONDS.toNanos(seconds);
                try {
                    if (mode.equals("closed")) {
                        for (int w = 0; w < CLOSED_WRITERS; w++) {
                            final int offset = w;
                            writing.add(pool.submit(() -> {
                                long n = 0;
                                try {
                                    while (running.get()) {
                                        final ShardId shardId = held.get((int) ((offset + n * CLOSED_WRITERS) % held.size()));
                                        final long at = System.nanoTime();
                                        node.writes().index(shardId, "c" + offset + "-" + n, "{\"msg\":\"ack\",\"n\":" + n + "}");
                                        acks.add(System.nanoTime() - at);
                                        n++;
                                    }
                                } catch (Throwable t) {
                                    failed.compareAndSet(null, t);
                                }
                                return null;
                            }));
                        }
                    } else {
                        // Poisson arrivals at the given rate, each its own task, so a slow write never delays the next.
                        final Random arrivals = new Random(random().nextLong());
                        long next = started;
                        long n = 0;
                        while (next < end) {
                            final long wait = next - System.nanoTime();
                            if (wait > 0) {
                                TimeUnit.NANOSECONDS.sleep(wait);
                            }
                            final ShardId shardId = held.get(arrivals.nextInt(held.size()));
                            final long id = n++;
                            writing.add(pool.submit(() -> {
                                try {
                                    final long at = System.nanoTime();
                                    node.writes().index(shardId, "o" + id, "{\"msg\":\"ack\",\"n\":" + id + "}");
                                    acks.add(System.nanoTime() - at);
                                } catch (Throwable t) {
                                    failed.compareAndSet(null, t);
                                }
                                return null;
                            }));
                            next += (long) (-Math.log(1 - arrivals.nextDouble()) / rate * 1e9);
                        }
                    }
                    long published = 0;
                    while (System.nanoTime() < end) {
                        final long passStarted = System.nanoTime();
                        published += loop.publishAll().size();
                        final long rest = TimeUnit.SECONDS.toNanos(1) - (System.nanoTime() - passStarted);
                        if (rest > 0) {
                            TimeUnit.NANOSECONDS.sleep(Math.min(rest, end - System.nanoTime()));
                        }
                    }
                    running.set(false);
                    for (Future<?> each : writing) {
                        each.get();
                    }
                    if (failed.get() != null) {
                        throw new AssertionError("a write failed", failed.get());
                    }
                    final double secondsRun = (System.nanoTime() - started) / 1e9;
                    final List<Long> sorted = new ArrayList<>(acks);
                    Collections.sort(sorted);
                    final long logPuts = node.nodeLogWriter().committer().timings().puts();
                    logger.info(
                        "ACK|{}|{} shards, {} s{}, {} ms per request, 1 in {} at {} ms{}|writes={} ({}/s) p50={} p90={} p99={} max={} ms"
                            + "|log PUTs={} ({}/s) store PUT={} GET={} LIST={} per write: PUT+LIST={} GET={}|publishes={}"
                            + "|steps {}|log {}",
                        mode,
                        shards,
                        seconds,
                        mode.equals("open") ? ", " + rate + " writes/s offered" : ", " + CLOSED_WRITERS + " writers",
                        REQUEST_MILLIS,
                        TAIL_EVERY,
                        TAIL_MILLIS,
                        extra.isBlank() ? "" : ", " + extra,
                        sorted.size(),
                        String.format(Locale.ROOT, "%.1f", sorted.size() / secondsRun),
                        String.format(Locale.ROOT, "%.1f", percentile(sorted, 0.50)),
                        String.format(Locale.ROOT, "%.1f", percentile(sorted, 0.90)),
                        String.format(Locale.ROOT, "%.1f", percentile(sorted, 0.99)),
                        String.format(Locale.ROOT, "%.1f", percentile(sorted, 1.0)),
                        logPuts,
                        String.format(Locale.ROOT, "%.1f", logPuts / secondsRun),
                        store.blobWrites() + store.registerWrites(),
                        store.blobReads() + store.registerReads(),
                        store.listings(),
                        String.format(
                            Locale.ROOT,
                            "%.3f",
                            (store.blobWrites() + store.registerWrites() + store.listings()) / (double) sorted.size()
                        ),
                        String.format(Locale.ROOT, "%.3f", (store.blobReads() + store.registerReads()) / (double) sorted.size()),
                        published,
                        node.writes().ackTimings().describe(),
                        node.nodeLogWriter().committer().timings().describe()
                    );
                    logger.info("ACKPLACES|{}\n{}", mode, store.byPlace());
                } finally {
                    running.set(false);
                    pool.shutdownNow();
                }
            }
        }
    }

    public void testWhereAWriteWaitsBeforeItsAcknowledgement() throws Exception {
        final String secondsProperty = System.getProperty("tests.ack.seconds", "");
        assumeTrue("the meter runs only when -Dtests.ack.seconds is set", secondsProperty.isEmpty() == false);
        final long seconds = Long.parseLong(secondsProperty);
        final int shards = Integer.parseInt(System.getProperty("tests.ack.shards", "200"));
        final double rate = Double.parseDouble(System.getProperty("tests.ack.rate", "100"));
        final String extra = System.getProperty("tests.ack.settings", "");
        for (String mode : System.getProperty("tests.ack.modes", "closed,open").split(",")) {
            measure(mode.trim(), shards, seconds, rate, extra);
        }
    }
}
