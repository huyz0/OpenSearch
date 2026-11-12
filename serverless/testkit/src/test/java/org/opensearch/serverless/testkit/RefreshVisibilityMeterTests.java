/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
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
import org.opensearch.serverless.transport.RefreshVisibility;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * What waiting for search nodes costs a refreshing write, and what it buys: write acknowledgement times with and without
 * {@code refresh}, how often a search node that serves the shard misses a write right after its refreshing answer, how
 * long a first open of a shard takes, and the store requests and transport messages of each, in {@code owner} and
 * {@code serving} mode, with pushes leaving at once and with each push held {@value #PUSH_DELAY_MILLIS} ms. Every store request takes {@value #REQUEST_MILLIS} ms, one in {@value #TAIL_EVERY} takes
 * {@value #TAIL_MILLIS} ms, and nodes run their schedulers as a bootstrapped node does. That is the default; {@link MeterStores} can put the meter on a live S3 endpoint or change the latency.
 *
 * <p>A measurement, not a pin: it logs one {@code REFRESHMETER} line per mode. Run with
 * {@code -Dtests.meter.refresh=true}.
 */
// On a live endpoint (MeterStores) the S3 client keeps its threads past the test; they are the client's, not leaks.
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters(filters = {
    org.opensearch.test.BouncyCastleThreadFilter.class,
    S3ClientThreads.class })
public class RefreshVisibilityMeterTests extends OpenSearchTestCase {

    private static final long TTL = ServerlessBootstrap.DEFAULT_LEASE_TTL_MILLIS;
    private static final long WINDOW = 250;
    private static final long REQUEST_MILLIS = 20;
    private static final int TAIL_EVERY = 50;
    private static final long TAIL_MILLIS = 200;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"keyword\"},\"n\":{\"type\":\"long\"}}}";
    private static final int THREADS = 4;
    private static final int WRITES_PER_THREAD = Integer.parseInt(System.getProperty("meter.writes", "60"));
    private static final int OPENS = Integer.parseInt(System.getProperty("meter.opens", "20"));
    private static final int ROUNDS = 2;
    private static final long PUSH_DELAY_MILLIS = 30;

    private Settings nodeSettings(String name, String roles) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-refresh-meter")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.window_millis", WINDOW)
            .put(CommitPush.ENABLED, true)
            .build();
    }

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

    private static double percentile(List<Long> sorted, double p) {
        if (sorted.isEmpty()) {
            return Double.NaN;
        }
        final int at = (int) Math.min(sorted.size() - 1, Math.ceil(p * sorted.size()) - 1);
        return sorted.get(Math.max(0, at)) / 1000.0;
    }

    /** Microseconds in, milliseconds out. */
    private static String summary(List<Long> micros) {
        final List<Long> sorted = new ArrayList<>(micros);
        Collections.sort(sorted);
        return String.format(
            Locale.ROOT,
            "p50=%.1f p99=%.1f max=%.1f ms (n=%d)",
            percentile(sorted, 0.5),
            percentile(sorted, 0.99),
            percentile(sorted, 1.0),
            sorted.size()
        );
    }

    private static void setMode(RefreshVisibility.Mode mode, ServerlessNode... nodes) {
        final Settings cluster = Settings.builder().put(RefreshVisibility.CLUSTER_SETTING, mode.settingValue()).build();
        for (ServerlessNode node : nodes) {
            node.router().refreshVisibility().applyCluster(cluster);
        }
    }

    /** One mode's numbers, summed over its rounds. */
    private static final class Tally {
        final List<Long> plainAcks = new CopyOnWriteArrayList<>();
        final List<Long> refreshAcks = new CopyOnWriteArrayList<>();
        final List<Long> opens = new CopyOnWriteArrayList<>();
        final AtomicInteger refreshes = new AtomicInteger();
        final AtomicInteger missed = new AtomicInteger();
        final AtomicInteger missedClaimed = new AtomicInteger();
        final AtomicInteger notClaimed = new AtomicInteger();
        long puts;
        long gets;
        long lists;
        long pushMessages;
        long fetches;
        int writes;
    }

    /** Hits on a search node's copy, found the way a search finds it: open, or opening it, or waiting for its open. */
    private static long hits(ServerlessNode reader, MetadataPlane plane, String token) throws Exception {
        for (int attempt = 0;; attempt++) {
            try {
                final ShardId on = reader.readers().serveAsReader(plane, "hot", 0);
                return ShardOps.hits(reader.searchService(), on, "msg", token);
            } catch (Exception e) {
                // Let go between the open and the query, as a reader moving onto a pushed commit is: open it again.
                if (attempt >= 3) {
                    throw e;
                }
            }
        }
    }

    public void testWhatWaitingForSearchNodesCosts() throws Exception {
        assumeTrue(
            "a refresh meter runs only when -Dtests.meter.refresh is set",
            System.getProperty("tests.meter.refresh", "").isEmpty() == false
        );
        final LatencyInjectingBlobStore slow = new LatencyInjectingBlobStore(MeterStores.create(createTempDir(), createTempDir()), 0);
        try (CountingBlobStore store = new CountingBlobStore(slow)) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), System::currentTimeMillis, TTL);
            plane.createIndex(new IndexDescriptor("hot", "uuid-hot-00000000000", 1, MAPPING, null));
            for (int i = 0; i < OPENS * ROUNDS * 4; i++) {
                final String name = String.format(Locale.ROOT, "open-%03d", i);
                plane.createIndex(new IndexDescriptor(name, "uuid-" + name, 1, MAPPING, null));
            }
            try (
                ServerlessNode writer = joined(new ServerlessNode(nodeSettings("meter-writer", "ingest")), plane);
                ServerlessNode first = joined(new ServerlessNode(nodeSettings("meter-search-a", "search")), plane);
                ServerlessNode second = joined(new ServerlessNode(nodeSettings("meter-search-b", "search")), plane)
            ) {
                final BackgroundReconciler loop = new BackgroundReconciler(writer, plane).setMaxShardsHeld(OPENS * ROUNDS * 8 + 4);
                loop.want("hot", 0);
                for (int i = 0; i < OPENS * ROUNDS * 4; i++) {
                    loop.want(String.format(Locale.ROOT, "open-%03d", i), 0);
                }
                loop.tick(System.currentTimeMillis());
                final java.util.Map<String, ShardId> held = new java.util.HashMap<>();
                int seeded = 0;
                for (ShardId shardId : writer.reconciler().openShards()) {
                    held.put(shardId.getIndexName(), shardId);
                    writer.writes().index(shardId, "seed", "{\"msg\":\"seed\",\"n\":0}");
                    if (++seeded % 20 == 0) {
                        writer.leases().renewLease(plane);
                    }
                }
                writer.leases().renewLease(plane);
                loop.publishAll();
                plane.membership().refresh();
                final ShardId hot = held.get("hot");
                first.readers().serveAsReader(plane, "hot", 0);
                second.readers().serveAsReader(plane, "hot", 0);
                // Setting up took a while: renewed before the schedulers take over renewing.
                for (ServerlessNode node : List.of(writer, first, second)) {
                    node.leases().renewLease(plane);
                }
                final List<ReconcileScheduler> schedulers = new ArrayList<>();
                schedulers.add(scheduled(writer, plane, loop));
                schedulers.add(scheduled(first, plane, new BackgroundReconciler(first, plane)));
                schedulers.add(scheduled(second, plane, new BackgroundReconciler(second, plane)));
                final java.util.Map<String, Tally> tallies = new java.util.TreeMap<>();
                int opened = 0;
                try {
                    MeterStores.slowDown(slow);
                    for (int round = 0; round < ROUNDS; round++) {
                        for (String variant : List.of("owner", "serving", "owner+push delay", "serving+push delay")) {
                            final RefreshVisibility.Mode mode = variant.startsWith("serving")
                                ? RefreshVisibility.Mode.SERVING
                                : RefreshVisibility.Mode.OWNER;
                            setMode(mode, writer, first, second);
                            // A push that takes PUSH_DELAY_MILLIS to leave, as one to a busy or distant search node might.
                            writer.router()
                                .injectSendFault(
                                    variant.endsWith("push delay")
                                        ? (action, target) -> org.opensearch.serverless.transport.TailPushRequest.ACTION.equals(action)
                                            ? PUSH_DELAY_MILLIS
                                            : 0L
                                        : null
                                );
                            final Tally tally = tallies.computeIfAbsent(variant, m -> new Tally());
                            store.reset();
                            final long[] pushBefore = writer.router().tailPush().counts();
                            final String tag = variant.replace("+push delay", "-delayed") + "-" + round;
                            final List<Thread> threads = new ArrayList<>();
                            for (int t = 0; t < THREADS; t++) {
                                final int thread = t;
                                threads.add(new Thread(() -> {
                                    for (int w = 0; w < WRITES_PER_THREAD; w++) {
                                        final String token = tag + "-" + thread + "-" + w;
                                        final boolean refresh = w % 4 == 0;
                                        try {
                                            final long started = System.nanoTime();
                                            final var outcome = writer.writes()
                                                .index(hot, token, "{\"msg\":\"" + token + "\",\"n\":" + w + "}");
                                            if (refresh == false) {
                                                tally.plainAcks.add((System.nanoTime() - started) / 1_000L);
                                                continue;
                                            }
                                            final boolean claimed = writer.writes().refreshWritten(hot, outcome, "meter");
                                            tally.refreshAcks.add((System.nanoTime() - started) / 1_000L);
                                            tally.refreshes.incrementAndGet();
                                            if (claimed == false) {
                                                tally.notClaimed.incrementAndGet();
                                            }
                                            for (var reader : List.of(first, second)) {
                                                if (hits(reader, plane, token) == 0) {
                                                    tally.missed.incrementAndGet();
                                                    if (claimed) {
                                                        tally.missedClaimed.incrementAndGet();
                                                        logger.info(
                                                            "REFRESHMISS|{}|seqNo {}|on {}",
                                                            token,
                                                            outcome.seqNo(),
                                                            reader.localNode().getName()
                                                        );
                                                    }
                                                }
                                            }
                                        } catch (Exception e) {
                                            throw new AssertionError(e);
                                        }
                                    }
                                }, "meter-writer-" + t));
                            }
                            for (Thread thread : threads) {
                                thread.start();
                            }
                            for (Thread thread : threads) {
                                thread.join();
                            }
                            // First opens of shards this node never served, on the search node that opens them.
                            for (int i = 0; i < OPENS; i++) {
                                final String name = String.format(Locale.ROOT, "open-%03d", opened++);
                                final long started = System.nanoTime();
                                final ShardId id = first.readers().serveAsReader(plane, name, 0);
                                tally.opens.add((System.nanoTime() - started) / 1_000L);
                                final var commit = first.reconciler().readerCommit(id);
                                if (commit.isPresent()
                                    && first.reconciler()
                                        .releaseReader(id, commit.get(), "meter") == ShardReconciler.ReaderRelease.RELEASED) {
                                    first.readers().forgetReader(id);
                                }
                            }
                            final long[] pushAfter = writer.router().tailPush().counts();
                            tally.puts += store.blobWrites() + store.registerWrites();
                            tally.gets += store.blobReads() + store.registerReads();
                            tally.lists += store.listings();
                            tally.pushMessages += pushAfter[1] - pushBefore[1];
                            tally.fetches += pushAfter[3] - pushBefore[3];
                            tally.writes += THREADS * WRITES_PER_THREAD;
                        }
                    }
                } finally {
                    MeterStores.speedUp(slow);
                    for (ReconcileScheduler scheduler : schedulers) {
                        scheduler.close();
                    }
                }
                writer.router().injectSendFault(null);
                tallies.forEach((variant, tally) -> {
                    final String line = String.format(
                        Locale.ROOT,
                        "REFRESHMETER|%s|%s|refresh=false ack %s|refresh=true ack %s"
                            + "|missed on a search node right after a refreshing answer %d of %d (%d answered refreshed)|answered not refreshed %d"
                            + "|first open %s|per write: PUT %.3f GET %.3f LIST %.3f, push messages %.3f|fetches %d",
                        variant,
                        MeterStores.describe(),
                        summary(tally.plainAcks),
                        summary(tally.refreshAcks),
                        tally.missed.get(),
                        tally.refreshes.get() * 2,
                        tally.missedClaimed.get(),
                        tally.notClaimed.get(),
                        summary(tally.opens),
                        (double) tally.puts / tally.writes,
                        (double) tally.gets / tally.writes,
                        (double) tally.lists / tally.writes,
                        (double) tally.pushMessages / tally.writes,
                        tally.fetches
                    );
                    logger.info(line);
                });
                assertEquals(
                    "no write answered refreshed in serving mode is missed on a search node serving the shard",
                    0,
                    tallies.get("serving").missedClaimed.get() + tallies.get("serving+push delay").missedClaimed.get()
                );
            }
        }
    }
}
