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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.reconcile.ReconcileScheduler;
import org.opensearch.serverless.shard.ShardReconciler;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.transport.CommitPush;
import org.opensearch.serverless.transport.TailOverlays;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * How long an acknowledged write takes to become searchable, by the path a search takes: the writer's own engine, a
 * search node that already has the shard open, and a search node opening the shard for the first time. Nodes run their
 * schedulers the way {@link ServerlessBootstrap} wires them, on the wall clock, and every store request takes
 * {@value #REQUEST_MILLIS} ms with one in {@value #TAIL_EVERY} taking {@value #TAIL_MILLIS} ms by default; {@link MeterStores} can put the meter on a live S3 endpoint or change the latency.
 *
 * <p>A measurement, not a pin: it logs one {@code FRESHMETER} line per configuration and asserts only that every write
 * became visible on every path. Run with {@code -Dtests.meter.freshness=all}.
 */
// On a live endpoint (MeterStores) the S3 client keeps its threads past the test; they are the client's, not leaks.
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters(filters = {
    org.opensearch.test.BouncyCastleThreadFilter.class,
    S3ClientThreads.class })
public class FreshnessMeterTests extends OpenSearchTestCase {

    private static final long TTL = ServerlessBootstrap.DEFAULT_LEASE_TTL_MILLIS;
    private static final long WINDOW = 250;
    private static final long REQUEST_MILLIS = 20;
    private static final int TAIL_EVERY = 50;
    private static final long TAIL_MILLIS = 200;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"keyword\"},\"n\":{\"type\":\"long\"}}}";
    private static final int SAMPLES = Integer.parseInt(System.getProperty("meter.samples", "40"));
    private static final int WARM_INDICES = 8;
    private static final long SPACING_MILLIS = 700;
    private static final long POLL_MILLIS = 25;
    private static final long GIVE_UP_MILLIS = 90_000;

    private Settings nodeSettings(String name, String roles, Settings extra) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-freshness-meter")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.window_millis", WINDOW)
            .put(extra)
            .build();
    }

    /** Starts a node's background loop and scheduler with the intervals and spacing a bootstrapped node uses. */
    private static ReconcileScheduler scheduled(ServerlessNode node, MetadataPlane plane, BackgroundReconciler loop) {
        loop.setLazyPublish(
            ServerlessBootstrap.DEFAULT_NODE_LOG_PUBLISH_MAX_AGE_MILLIS,
            ReconcileScheduler.DEFAULT_PUBLISH_MAX_UNPUBLISHED_OPERATIONS,
            WINDOW
        );
        final ReconcileScheduler scheduler = new ReconcileScheduler(
            loop,
            node.threadPool(),
            plane.clock(),
            TimeValue.timeValueMillis(TTL / ReconcileScheduler.RENEWALS_PER_TTL),
            TimeValue.timeValueMillis(WINDOW),
            ReconcileScheduler.DEFAULT_BACKSTOP_INTERVAL
        );
        scheduler.setPublishSpacing(
            ReconcileScheduler.DEFAULT_PUBLISH_MIN_INTERVAL,
            ReconcileScheduler.DEFAULT_PUBLISH_MAX_UNPUBLISHED_OPERATIONS
        );
        node.setSignals(scheduler);
        return scheduler.start();
    }

    private static ServerlessNode joined(ServerlessNode node, MetadataPlane plane) throws Exception {
        node.startup().start();
        node.setMetadataPlane(plane);
        node.leases().renewLease(plane);
        return node;
    }

    private static double percentile(List<Long> sortedMillis, double p) {
        if (sortedMillis.isEmpty()) {
            return Double.NaN;
        }
        final int at = (int) Math.min(sortedMillis.size() - 1, Math.ceil(p * sortedMillis.size()) - 1);
        return sortedMillis.get(Math.max(0, at));
    }

    private static String summary(List<Long> millis) {
        final List<Long> sorted = new ArrayList<>(millis);
        Collections.sort(sorted);
        return String.format(
            Locale.ROOT,
            "p50=%.0f p90=%.0f p99=%.0f max=%.0f ms (n=%d)",
            percentile(sorted, 0.5),
            percentile(sorted, 0.9),
            percentile(sorted, 0.99),
            percentile(sorted, 1.0),
            sorted.size()
        );
    }

    /** One write being watched: when it was acknowledged, and when each path first saw it. */
    private static final class Sample {
        final String index;
        final String coldIndex;
        final String token;
        volatile long ackedAtNanos;
        final Map<String, Long> seenAfterMillis = new ConcurrentHashMap<>();

        Sample(String index, String coldIndex, String token) {
            this.index = index;
            this.coldIndex = coldIndex;
            this.token = token;
        }
    }

    /** Whether a path sees a sample yet. */
    private interface Probe {
        boolean visible(Sample sample) throws Exception;
    }

    /** Polls one path for every sample it has not seen yet, until all are seen or the deadline passes. */
    private Thread watch(String path, List<Sample> samples, Probe visible, long deadlineNanos) {
        final Thread thread = new Thread(() -> {
            while (System.nanoTime() < deadlineNanos) {
                boolean pending = false;
                for (Sample sample : samples) {
                    if (sample.ackedAtNanos == 0 || sample.seenAfterMillis.containsKey(path)) {
                        pending |= sample.ackedAtNanos == 0;
                        continue;
                    }
                    pending = true;
                    try {
                        if (visible.visible(sample)) {
                            sample.seenAfterMillis.put(path, (System.nanoTime() - sample.ackedAtNanos) / 1_000_000L);
                        }
                    } catch (Exception e) {
                        // A refusal or a reader let go under the query: this poll did not see it, the next one tries again.
                    }
                }
                if (pending == false && samples.size() == SAMPLES) {
                    return;
                }
                try {
                    Thread.sleep(POLL_MILLIS);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }, "freshness-" + path);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private void measure(String label, Settings extra) throws Exception {
        final LatencyInjectingBlobStore slow = new LatencyInjectingBlobStore(MeterStores.create(createTempDir(), createTempDir()), 0);
        try (CountingBlobStore store = new CountingBlobStore(slow)) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), System::currentTimeMillis, TTL);
            final List<String> warmIndices = new ArrayList<>();
            for (int i = 0; i < WARM_INDICES; i++) {
                final String name = String.format(Locale.ROOT, "warm-%02d", i);
                plane.createIndex(new IndexDescriptor(name, "uuid-" + name, 1, MAPPING, null));
                warmIndices.add(name);
            }
            for (int s = 0; s < SAMPLES; s++) {
                final String name = String.format(Locale.ROOT, "cold-%03d", s);
                plane.createIndex(new IndexDescriptor(name, "uuid-" + name, 1, MAPPING, null));
            }
            try (
                ServerlessNode writer = joined(new ServerlessNode(nodeSettings("fresh-writer", "ingest", extra)), plane);
                ServerlessNode warm = joined(new ServerlessNode(nodeSettings("fresh-warm", "search", extra)), plane);
                ServerlessNode cold = joined(new ServerlessNode(nodeSettings("fresh-cold", "search", extra)), plane)
            ) {
                final BackgroundReconciler writerLoop = new BackgroundReconciler(writer, plane).setMaxShardsHeld(4 * SAMPLES);
                for (String name : warmIndices) {
                    writerLoop.want(name, 0);
                }
                for (int s = 0; s < SAMPLES; s++) {
                    writerLoop.want(String.format(Locale.ROOT, "cold-%03d", s), 0);
                }
                writerLoop.tick(System.currentTimeMillis());
                final Map<String, ShardId> held = new ConcurrentHashMap<>();
                for (ShardId shardId : writer.reconciler().openShards()) {
                    held.put(shardId.getIndexName(), shardId);
                    writer.writes().index(shardId, "seed", "{\"msg\":\"seed\",\"n\":0}");
                }
                assertEquals(WARM_INDICES + SAMPLES, held.size());
                writerLoop.publishAll();
                for (String name : warmIndices) {
                    final ShardId onWarm = warm.readers().serveAsReader(plane, name, 0);
                    assertEquals(1L, ShardOps.hits(warm.searchService(), onWarm, "msg", "seed"));
                }
                // Every node knows the others, as it would a renewal after they joined.
                plane.membership().refresh();
                final List<ReconcileScheduler> schedulers = new ArrayList<>();
                schedulers.add(scheduled(writer, plane, writerLoop));
                schedulers.add(scheduled(warm, plane, new BackgroundReconciler(warm, plane)));
                schedulers.add(scheduled(cold, plane, new BackgroundReconciler(cold, plane)));
                try {
                    MeterStores.slowDown(slow);
                    store.reset();
                    final List<Sample> samples = new java.util.concurrent.CopyOnWriteArrayList<>();
                    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(SAMPLES * SPACING_MILLIS + GIVE_UP_MILLIS);
                    final List<Thread> watchers = List.of(
                        watch(
                            "writer",
                            samples,
                            s -> ShardOps.hits(writer.searchService(), held.get(s.index), "msg", s.token) > 0,
                            deadline
                        ),
                        watch("warm-reader", samples, s -> {
                            final ShardId id = warm.readers().serveAsReader(plane, s.index, 0);
                            return ShardOps.hits(warm.searchService(), id, "msg", s.token) > 0;
                        }, deadline),
                        watch("cold-reader", samples, s -> {
                            // Each sample's index is one this node has never been asked for: its first search opens it. A miss
                            // lets the copy go, so the next poll is a first open again, as on a node that had never served it.
                            final ShardId id = cold.readers().serveAsReader(plane, s.coldIndex, 0);
                            final boolean seen = ShardOps.hits(cold.searchService(), id, "msg", s.token) > 0;
                            if (seen == false) {
                                final var commit = cold.reconciler().readerCommit(id);
                                if (commit.isPresent()
                                    && cold.reconciler()
                                        .releaseReader(
                                            id,
                                            commit.get(),
                                            "freshness meter: open again"
                                        ) == ShardReconciler.ReaderRelease.RELEASED) {
                                    cold.readers().forgetReader(id);
                                }
                            }
                            return seen;
                        }, deadline)
                    );
                    final List<Long> acks = new ArrayList<>();
                    final long writerBytesBefore = writer.router().transportBytesSent();
                    final long started = System.nanoTime();
                    for (int s = 0; s < SAMPLES; s++) {
                        final Sample sample = new Sample(
                            warmIndices.get(s % WARM_INDICES),
                            String.format(Locale.ROOT, "cold-%03d", s),
                            "tok-" + s
                        );
                        samples.add(sample);
                        final long writeStarted = System.nanoTime();
                        writer.writes().index(held.get(sample.index), "d" + s, "{\"msg\":\"" + sample.token + "\",\"n\":" + s + "}");
                        final long ackedAt = System.nanoTime();
                        writer.writes().index(held.get(sample.coldIndex), "d" + s, "{\"msg\":\"" + sample.token + "\",\"n\":" + s + "}");
                        sample.ackedAtNanos = ackedAt;
                        acks.add((ackedAt - writeStarted) / 1_000_000L);
                        // Spread across the publish and refresh cycles, so the delays sample every phase of them.
                        Thread.sleep(SPACING_MILLIS / 2 + randomLongBetween(0, SPACING_MILLIS));
                    }
                    for (Thread watcher : watchers) {
                        watcher.join(TimeUnit.NANOSECONDS.toMillis(Math.max(1, deadline - System.nanoTime())) + 1_000);
                    }
                    final double elapsed = (System.nanoTime() - started) / 1e9;
                    final long writerBytes = writer.router().transportBytesSent() - writerBytesBefore;
                    final Map<String, List<Long>> byPath = new java.util.TreeMap<>();
                    int missing = 0;
                    for (Sample sample : samples) {
                        for (String path : List.of("writer", "warm-reader", "cold-reader")) {
                            final Long seen = sample.seenAfterMillis.get(path);
                            if (seen == null) {
                                missing++;
                            } else {
                                byPath.computeIfAbsent(path, k -> new ArrayList<>()).add(seen);
                            }
                        }
                    }
                    final StringBuilder line = new StringBuilder();
                    line.append("FRESHMETER|")
                        .append(label)
                        .append("|")
                        .append(MeterStores.describe())
                        .append("|write ack ")
                        .append(summary(acks));
                    byPath.forEach((path, millis) -> line.append("|").append(path).append(" visible after ack ").append(summary(millis)));
                    line.append(
                        String.format(
                            Locale.ROOT,
                            "|writer sent %.1f KB/s over transport|requests over %.0f s incl. polling: PUT/s=%.1f LIST/s=%.1f GET/s=%.1f",
                            writerBytes / 1024.0 / elapsed,
                            elapsed,
                            (store.blobWrites() + store.registerWrites()) / elapsed,
                            store.listings() / elapsed,
                            (store.blobReads() + store.registerReads()) / elapsed
                        )
                    );
                    logger.info(line.toString());
                    assertEquals("every write must become visible on every path: " + line, 0, missing);
                } finally {
                    MeterStores.speedUp(slow);
                    for (ReconcileScheduler scheduler : schedulers) {
                        scheduler.close();
                    }
                }
            }
        }
    }

    /**
     * Runs only with {@code -Dtests.meter.freshness=all} (every configuration) or {@code =push} (only with pushes on),
     * since each configuration watches writes for about a minute.
     */
    public void testWriteToSearchableDelayByPath() throws Exception {
        final String which = System.getProperty("tests.meter.freshness", "");
        assumeTrue("a freshness meter runs only when -Dtests.meter.freshness is set", which.isEmpty() == false);
        final Settings commitOnly = Settings.builder().put(TailOverlays.ENABLED, false).put(CommitPush.ENABLED, false).build();
        if ("push".equals(which) == false) {
            measure("commit only (no push, no overlay)", commitOnly);
            measure("commit push, no overlay", Settings.builder().put(commitOnly).put(CommitPush.ENABLED, true).build());
        }
        measure("overlay + commit push", Settings.builder().put(TailOverlays.ENABLED, true).put(CommitPush.ENABLED, true).build());
        measure(
            "overlay + commit push + log commits",
            Settings.builder()
                .put(TailOverlays.ENABLED, true)
                .put(CommitPush.ENABLED, true)
                .put("serverless.wal.node_log.commits", true)
                .build()
        );
    }

    /**
     * What the longer publish age saves on busy shards: {@value #BUSY_SHARDS} shards each written {@value #BUSY_RATE}
     * times a second for {@value #BUSY_SECONDS} seconds, with schedulers as a bootstrapped node runs them, counting PUTs
     * per acknowledged write with overlays off (publish after a second) and on (publish after ten).
     */
    public void testPublishPutsOnBusyShards() throws Exception {
        assumeTrue(
            "a freshness meter runs only when -Dtests.meter.freshness is set",
            System.getProperty("tests.meter.freshness", "").isEmpty() == false
        );
        busy("overlay off", Settings.builder().put(TailOverlays.ENABLED, false).build());
        busy("overlay on", Settings.builder().put(TailOverlays.ENABLED, true).build());
    }

    private static final int BUSY_SHARDS = 8;
    private static final int BUSY_RATE = 4;
    private static final int BUSY_SECONDS = 30;

    private void busy(String label, Settings extra) throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), System::currentTimeMillis, TTL);
            for (int i = 0; i < BUSY_SHARDS; i++) {
                final String name = String.format(Locale.ROOT, "busy-%02d", i);
                plane.createIndex(new IndexDescriptor(name, "uuid-" + name, 1, MAPPING, null));
            }
            try (ServerlessNode writer = joined(new ServerlessNode(nodeSettings("busy-writer", "ingest", extra)), plane)) {
                final BackgroundReconciler loop = new BackgroundReconciler(writer, plane).setMaxShardsHeld(4 * BUSY_SHARDS);
                for (int i = 0; i < BUSY_SHARDS; i++) {
                    loop.want(String.format(Locale.ROOT, "busy-%02d", i), 0);
                }
                loop.tick(System.currentTimeMillis());
                final List<ShardId> held = new ArrayList<>(writer.reconciler().openShards());
                for (ShardId shardId : held) {
                    writer.writes().index(shardId, "seed", "{\"msg\":\"seed\",\"n\":0}");
                }
                loop.publishAll();
                final ReconcileScheduler scheduler = scheduled(writer, plane, loop);
                try {
                    store.reset();
                    long writes = 0;
                    final long started = System.nanoTime();
                    for (int tick = 0; tick < BUSY_SECONDS * BUSY_RATE; tick++) {
                        for (ShardId shardId : held) {
                            writer.writes().index(shardId, "d" + tick, "{\"msg\":\"busy\",\"n\":" + tick + "}");
                            writes++;
                        }
                        final long next = started + TimeUnit.MILLISECONDS.toNanos((tick + 1) * 1000L / BUSY_RATE);
                        final long wait = next - System.nanoTime();
                        if (wait > 0) {
                            TimeUnit.NANOSECONDS.sleep(wait);
                        }
                    }
                    final long puts = store.blobWrites() + store.registerWrites();
                    logger.info(
                        "FRESHMETER|busy shards, {}|{} shards x {} writes/s x {} s|{} writes, {} PUT ({} per write), {} LIST",
                        label,
                        BUSY_SHARDS,
                        BUSY_RATE,
                        BUSY_SECONDS,
                        writes,
                        puts,
                        String.format(Locale.ROOT, "%.3f", puts / (double) writes),
                        store.listings()
                    );
                } finally {
                    scheduler.close();
                }
            }
        }
    }
}
