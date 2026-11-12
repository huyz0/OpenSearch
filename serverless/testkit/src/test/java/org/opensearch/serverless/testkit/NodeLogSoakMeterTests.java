/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.apache.logging.log4j.LogManager;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.reconcile.GarbageCollector;
import org.opensearch.serverless.reconcile.NodePackCollector;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.NodePackLedger;
import org.opensearch.serverless.store.NodePackStore;
import org.opensearch.serverless.store.NodePackWriter;
import org.opensearch.serverless.store.SegmentPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * A soak of each segment placement, with and without log commits: many shards written for minutes against a slow
 * store, with forced merges, and a writer crash and takeover halfway. Off unless {@code -Dtests.soak.seconds} is set, since it
 * runs for minutes; {@code -Dtests.soak.shards} sets the shard count and {@code -Dtests.soak.modes} a comma-separated subset of
 * {@code blob,blob+log,pack,pack+log}.
 */
@com.carrotsearch.randomizedtesting.annotations.TimeoutSuite(millis = 3_600_000) // four soaks of minutes each
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*") // a thousand shards need more handles than its file system allows
public class NodeLogSoakMeterTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final long REQUEST_MILLIS = 20;
    private static final int TAIL_EVERY = 50;
    private static final long TAIL_MILLIS = 200;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";
    private static final int WRITERS = 16;
    private static final int COLD_SHARDS = 20;
    private static final int VERIFIED = 2_000;

    private Settings nodeSettings(String name, String roles, boolean packs, boolean logCommits) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-soak")
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
        return String.format(Locale.ROOT, "soak-%04d", i);
    }

    private static double percentile(List<Long> sortedNanos, double p) {
        if (sortedNanos.isEmpty()) {
            return 0;
        }
        final int at = (int) Math.min(sortedNanos.size() - 1, Math.ceil(p * sortedNanos.size()) - 1);
        return sortedNanos.get(Math.max(0, at)) / 1e6;
    }

    private static BackgroundReconciler takeAll(ServerlessNode node, MetadataPlane plane, AtomicLong clock, int shards) throws Exception {
        node.leases().renewLease(plane);
        plane.membership().refresh();
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setMaxShardsHeld(shards * 2);
        for (int i = 0; i < shards; i++) {
            loop.want(name(i), 0);
        }
        // Activations are paced, so taking a thousand shards takes several passes.
        for (int pass = 0; pass < 50 && node.reconciler().openShards().size() < shards; pass++) {
            final long started = System.nanoTime();
            loop.tick(clock.get());
            LogManager.getLogger(NodeLogSoakMeterTests.class)
                .info(
                    "SOAKTAKE|pass {} took {} ms, {} shards open",
                    pass,
                    (System.nanoTime() - started) / 1_000_000,
                    node.reconciler().openShards().size()
                );
        }
        assertEquals(shards, node.reconciler().openShards().size());
        return loop;
    }

    /** Segment data the store holds: node packs, and blobs under any shard's segments. */
    private static long[] segmentBytesAndBlobs(Path root) throws Exception {
        final long[] out = new long[2];
        try (Stream<Path> files = Files.walk(root)) {
            files.filter(Files::isRegularFile).forEach(file -> {
                final String rel = root.relativize(file).toString().replace('\\', '/');
                final String base = file.getFileName().toString();
                final boolean shardBlob = rel.startsWith("segments/") && base.startsWith(SegmentPublisher.MANIFEST) == false;
                if (rel.startsWith(NodePackStore.ROOT + "/") || shardBlob) {
                    try {
                        out[0] += Files.size(file);
                        out[1]++;
                    } catch (Exception e) {
                        // gone meanwhile
                    }
                }
            });
        }
        return out;
    }

    private record Phase(List<Long> acks, long published, long puts, long lists, long gets, double seconds, int merges) {
    }

    private Phase run(
        ServerlessNode node,
        BackgroundReconciler loop,
        CountingBlobStore store,
        List<ShardId> held,
        long seconds,
        Map<String, ShardId> acked,
        String tag,
        boolean publishAtEnd
    ) throws Exception {
        final ExecutorService pool = Executors.newFixedThreadPool(WRITERS);
        try {
            store.reset();
            final AtomicBoolean running = new AtomicBoolean(true);
            final ConcurrentLinkedQueue<Long> acks = new ConcurrentLinkedQueue<>();
            final AtomicReference<Throwable> failed = new AtomicReference<>();
            final List<Future<?>> writing = new ArrayList<>();
            for (int w = 0; w < WRITERS; w++) {
                final int offset = w;
                writing.add(pool.submit(() -> {
                    long n = 0;
                    try {
                        while (running.get()) {
                            final ShardId shardId = held.get((int) ((offset + n * WRITERS) % held.size()));
                            final String id = tag + offset + "-" + n;
                            final long started = System.nanoTime();
                            node.writes().index(shardId, id, "{\"msg\":\"soak\",\"n\":" + n + "}");
                            acks.add(System.nanoTime() - started);
                            acked.put(id, shardId);
                            n++;
                        }
                    } catch (Throwable t) {
                        failed.compareAndSet(null, t);
                    }
                    return null;
                }));
            }
            final long started = System.nanoTime();
            final long end = started + TimeUnit.SECONDS.toNanos(seconds);
            long published = 0;
            int merges = 0;
            long nextMerge = started + TimeUnit.SECONDS.toNanos(20);
            while (System.nanoTime() < end) {
                final long passStarted = System.nanoTime();
                if (passStarted >= nextMerge) {
                    // A tenth of the shards merge down to one segment, as a busy shard's merges would.
                    for (int i = merges % 10; i < held.size(); i += 10) {
                        node.reconciler().shard(held.get(i)).forceMerge(new ForceMergeRequest().maxNumSegments(1));
                    }
                    merges++;
                    nextMerge = passStarted + TimeUnit.SECONDS.toNanos(20);
                }
                published += loop.publishAll().size();
                loop.checkpointDue();
                final long rest = TimeUnit.SECONDS.toNanos(1) - (System.nanoTime() - passStarted);
                if (rest > 0) {
                    TimeUnit.NANOSECONDS.sleep(rest);
                }
            }
            running.set(false);
            for (Future<?> each : writing) {
                each.get();
            }
            if (failed.get() != null) {
                throw new AssertionError("a write failed", failed.get());
            }
            if (publishAtEnd) {
                published += loop.publishAll().size();
            }
            final List<Long> sorted = new ArrayList<>(acks);
            Collections.sort(sorted);
            return new Phase(
                sorted,
                published,
                store.blobWrites() + store.registerWrites(),
                store.listings(),
                store.blobReads() + store.registerReads(),
                (System.nanoTime() - started) / 1e9,
                merges
            );
        } finally {
            pool.shutdownNow();
        }
    }

    private void soak(boolean packs, boolean logCommits, int shards, long seconds) throws Exception {
        final String mode = (packs ? "node_pack" : "segment_blob") + (logCommits ? " + log commits" : "");
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final Path root = createTempDir();
        final LatencyInjectingBlobStore slow = new LatencyInjectingBlobStore(new FsBlobStore(8192, root, false), 0);
        final Map<String, ShardId> acked = new ConcurrentHashMap<>();
        try (CountingBlobStore store = new CountingBlobStore(slow)) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            for (int i = 0; i < shards; i++) {
                plane.createIndex(new IndexDescriptor(name(i), "uuid-" + name(i), 1, MAPPING, null));
            }
            final ServerlessNode first = new ServerlessNode(nodeSettings("soak-first", "ingest", packs, logCommits));
            try (
                ServerlessNode second = new ServerlessNode(nodeSettings("soak-second", "ingest", packs, logCommits));
                ServerlessNode reader = new ServerlessNode(nodeSettings("soak-reader", "search", packs, logCommits))
            ) {
                first.startup().start();
                first.setMetadataPlane(plane);
                final BackgroundReconciler firstLoop = takeAll(first, plane, clock, shards);
                final List<ShardId> firstHeld = new ArrayList<>(first.reconciler().openShards());
                slow.setDelayMillis(REQUEST_MILLIS);
                slow.setTail(TAIL_EVERY, TAIL_MILLIS);
                final Phase before = run(first, firstLoop, store, firstHeld, seconds / 2, acked, "a", false);
                final String firstId = first.localNode().getId();

                // The writer crashes as its writes stop, with whatever it had not published or had staged; a successor
                // takes every shard and replays its log.
                clock.addAndGet(2 * TTL);
                second.startup().start();
                second.setMetadataPlane(plane);
                store.reset();
                final long takeStarted = System.nanoTime();
                final BackgroundReconciler loop = takeAll(second, plane, clock, shards);
                final double takeoverSeconds = (System.nanoTime() - takeStarted) / 1e9;
                final double takeoverRequestsPerShard = (store.total()) / (double) shards;
                logger.info(
                    "SOAKTAKE|{}|takeover {} s: {}|open phases ms {}|log records read {}|opened from logged commits {}",
                    mode,
                    takeoverSeconds,
                    store.breakdown(),
                    java.util.Arrays.toString(second.reconciler().writerOpenPhaseMillis()),
                    second.reconciler().logRecordsRead(),
                    second.reconciler().loggedCommitOpens()
                );
                final List<ShardId> held = new ArrayList<>(second.reconciler().openShards());
                final Map<String, ShardId> byName = new ConcurrentHashMap<>();
                for (ShardId shardId : held) {
                    byName.put(shardId.getIndexName(), shardId);
                }
                final List<Map.Entry<String, ShardId>> sample = new ArrayList<>(acked.entrySet());
                Collections.shuffle(sample, random());
                int lost = 0;
                for (Map.Entry<String, ShardId> write : sample.subList(0, Math.min(VERIFIED, sample.size()))) {
                    if (second.reads().get(byName.get(write.getValue().getIndexName()), write.getKey()).found() == false) {
                        lost++;
                    }
                }
                assertEquals("acknowledged writes lost in the takeover", 0, lost);
                final Phase after = run(second, loop, store, held, seconds - seconds / 2, acked, "b", true);

                // What a manifest names, and what the store holds, before and after the first node's packs are collected.
                long liveBytes = 0;
                long namedBlobs = 0;
                for (ShardId shardId : held) {
                    final CommitManifest newest = second.reconciler().newestCommit(shardId);
                    final Set<String> blobs = new HashSet<>();
                    for (String file : newest.files().keySet()) {
                        final Long length = newest.lengthOf(file);
                        liveBytes += length == null ? 0 : length;
                        blobs.add(newest.location(file).referencedBlob());
                    }
                    namedBlobs += blobs.size();
                }
                final long[] stored = segmentBytesAndBlobs(root);
                slow.setDelayMillis(0);
                slow.setTail(0, 0);
                if (packs) {
                    final NodePackCollector collector = new NodePackCollector(
                        new NodePackStore(plane.blobStore(), plane.basePath(), firstId),
                        new NodePackLedger(plane.blobStore(), plane.basePath(), firstId),
                        plane,
                        clock::get,
                        GarbageCollector.DEFAULT_MINIMUM_UNREFERENCED_MILLIS
                    );
                    for (int pass = 0; pass < 4; pass++) {
                        collector.run();
                        clock.addAndGet(NodePackWriter.COLLECTABLE_AFTER_MILLIS + NodePackCollector.MIN_RECHECK_MILLIS);
                    }
                }
                final long[] collected = segmentBytesAndBlobs(root);

                // A cold reader opens some shards and runs a first search on each.
                slow.setDelayMillis(REQUEST_MILLIS);
                slow.setTail(TAIL_EVERY, TAIL_MILLIS);
                reader.startup().start();
                reader.setMetadataPlane(plane);
                reader.leases().renewLease(plane);
                second.leases().renewLease(plane);
                store.reset();
                slow.drain();
                long firstNanos = 0;
                long openNanos = 0;
                for (ShardId shardId : held.subList(0, COLD_SHARDS)) {
                    final long openStarted = System.nanoTime();
                    final ShardId onReader = reader.readers().serveAsReader(plane, shardId.getIndexName(), 0);
                    final long opened = System.nanoTime();
                    assertTrue(ShardOps.hits(reader.searchService(), onReader, "msg", "soak") > 0);
                    openNanos += opened - openStarted;
                    firstNanos += System.nanoTime() - openStarted;
                }
                // Which requests a cold open waits on, by the thread that made them and the kind of blob.
                final Map<String, Integer> coldRequests = new java.util.TreeMap<>();
                final Map<String, Integer> serialKeys = new java.util.TreeMap<>();
                for (LatencyInjectingBlobStore.Request request : slow.drain()) {
                    if (request.thread().contains("TEST-")) {
                        serialKeys.merge(
                            request.op() + " " + request.key().replaceAll("[0-9a-f]{8,}|_[0-9]+|[0-9]{3,}", "*"),
                            1,
                            Integer::sum
                        );
                    }
                    final String key = request.key();
                    final String kind = key.contains("manifest") ? "manifest"
                        : key.contains("/packs/") ? "packs"
                        : key.contains("/segments/") ? "segments"
                        : key.contains("node_log") || key.contains("nodelog") ? "node_log"
                        : key.contains("lease") ? "lease"
                        : "other";
                    final String thread = request.thread().contains("TEST-")
                        ? "test"
                        : request.thread().replaceAll("\\[|\\]|#\\d+|-\\d+", "");
                    coldRequests.merge(thread + ":" + request.op() + ":" + kind, 1, Integer::sum);
                }
                logger.info(
                    "SOAKCOLD|{}|open {} ms, search {} ms per shard|requests {}|waited on {}",
                    mode,
                    String.format(Locale.ROOT, "%.1f", openNanos / 1e6 / COLD_SHARDS),
                    String.format(Locale.ROOT, "%.1f", (firstNanos - openNanos) / 1e6 / COLD_SHARDS),
                    coldRequests,
                    serialKeys
                );
                final double coldGets = (store.blobReads() + store.registerReads()) / (double) COLD_SHARDS;
                slow.setDelayMillis(0);
                slow.setTail(0, 0);

                final List<Long> acks = new ArrayList<>(before.acks());
                acks.addAll(after.acks());
                Collections.sort(acks);
                final long published = before.published() + after.published();
                final double secondsRun = before.seconds() + after.seconds();
                final long putsAndLists = before.puts() + before.lists() + after.puts() + after.lists();
                logger.info(
                    "SOAK|{}|{} shards, {} s, {} ms per request, 1 in {} at {} ms, {} merge rounds"
                        + "|writes acked={} p50={} ms p99={} ms|shard publishes={} ({}/s)"
                        + "|PUT/s={} LIST/s={} GET/s={} PUT+LIST per shard publish={} GET per shard publish={}"
                        + "|takeover {} s, {} requests per shard, {} sampled writes all found"
                        + "|blobs named per shard={} segment bytes stored={} MB live={} MB dead share={}%"
                        + " after pack collection {}% ({} blobs stored)"
                        + "|cold open: {} GET per shard, first search {} ms",
                    mode,
                    shards,
                    seconds,
                    REQUEST_MILLIS,
                    TAIL_EVERY,
                    TAIL_MILLIS,
                    before.merges() + after.merges(),
                    acks.size(),
                    String.format(Locale.ROOT, "%.1f", percentile(acks, 0.50)),
                    String.format(Locale.ROOT, "%.1f", percentile(acks, 0.99)),
                    published,
                    String.format(Locale.ROOT, "%.0f", published / secondsRun),
                    String.format(Locale.ROOT, "%.1f", (before.puts() + after.puts()) / secondsRun),
                    String.format(Locale.ROOT, "%.1f", (before.lists() + after.lists()) / secondsRun),
                    String.format(Locale.ROOT, "%.1f", (before.gets() + after.gets()) / secondsRun),
                    String.format(Locale.ROOT, "%.3f", published == 0 ? 0 : putsAndLists / (double) published),
                    String.format(Locale.ROOT, "%.3f", published == 0 ? 0 : (before.gets() + after.gets()) / (double) published),
                    String.format(Locale.ROOT, "%.1f", takeoverSeconds),
                    String.format(Locale.ROOT, "%.1f", takeoverRequestsPerShard),
                    Math.min(VERIFIED, sample.size()),
                    String.format(Locale.ROOT, "%.1f", namedBlobs / (double) shards),
                    String.format(Locale.ROOT, "%.1f", stored[0] / 1e6),
                    String.format(Locale.ROOT, "%.1f", liveBytes / 1e6),
                    String.format(Locale.ROOT, "%.0f", stored[0] == 0 ? 0 : 100.0 * (stored[0] - liveBytes) / stored[0]),
                    String.format(
                        Locale.ROOT,
                        "%.0f",
                        collected[0] == 0 ? 0 : 100.0 * Math.max(0, collected[0] - liveBytes) / collected[0]
                    ),
                    collected[1],
                    String.format(Locale.ROOT, "%.1f", coldGets),
                    String.format(Locale.ROOT, "%.1f", firstNanos / 1e6 / COLD_SHARDS)
                );
            } finally {
                try {
                    first.close();
                } catch (Exception e) {
                    // It lost every shard it held.
                }
            }
        }
    }

    public void testSoakEachPlacementWithAndWithoutLogCommits() throws Exception {
        final String secondsProperty = System.getProperty("tests.soak.seconds", "");
        assumeTrue("a soak runs only when -Dtests.soak.seconds is set", secondsProperty.isEmpty() == false);
        final long seconds = Long.parseLong(secondsProperty);
        final int shards = Integer.parseInt(System.getProperty("tests.soak.shards", "1000"));
        final Set<String> modes = Set.of(System.getProperty("tests.soak.modes", "blob,blob+log,pack,pack+log").split(","));
        for (String each : List.of("blob", "blob+log", "pack", "pack+log")) {
            if (modes.contains(each)) {
                soak(each.startsWith("pack"), each.endsWith("+log"), shards, seconds);
            }
        }
    }
}
