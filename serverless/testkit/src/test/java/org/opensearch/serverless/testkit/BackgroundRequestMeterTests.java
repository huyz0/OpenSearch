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
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * The store requests a deployment makes in the background, by source, per node-hour and per index-hour: what a writer
 * node and a search node cost with nothing to do, and what each lightly written, lightly searched index adds on top.
 *
 * <p>Nodes run their schedulers on the wall clock with the intervals {@link ServerlessBootstrap} gives them (lease
 * renewal every ttl/3, the backstop pass every 30 s, the janitor every minute, head and descriptor checks every five
 * minutes). Each index is written once every {@value #WRITE_EVERY_SECONDS} s and searched once every
 * {@value #SEARCH_EVERY_SECONDS} s on the search node, a tenant that is in use but far from busy. The writes' own
 * requests (the log and the publishes they cause) are counted too, under their callers, so the report shows the
 * background against them.
 *
 * <p>A measurement, not a pin: it logs one {@code BGMETER} block per configuration and asserts nothing about the
 * numbers. Off unless {@code -Dtests.meter.background.seconds} is set; {@code -Dtests.meter.background.indices} sets the
 * index count (default 20) and {@code -Dtests.meter.background.extra} adds node settings as {@code key=value,...}.
 */
@com.carrotsearch.randomizedtesting.annotations.TimeoutSuite(millis = 7_200_000)
public class BackgroundRequestMeterTests extends OpenSearchTestCase {

    private static final long TTL = ServerlessBootstrap.DEFAULT_LEASE_TTL_MILLIS;
    private static final long WINDOW = 250;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"keyword\"},\"n\":{\"type\":\"long\"}}}";
    private static final int WRITE_EVERY_SECONDS = 10;
    private static final int SEARCH_EVERY_SECONDS = 10;

    private Settings nodeSettings(String name, String roles, Settings extra) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-background-meter")
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

    /** Starts a node's loop and scheduler the way {@link ServerlessBootstrap} does. */
    private static ReconcileScheduler scheduled(ServerlessNode node, MetadataPlane plane, BackgroundReconciler loop) {
        node.setHeadVerifyIntervalMillis(ServerlessBootstrap.DEFAULT_HEAD_VERIFY_INTERVAL_MILLIS);
        node.setDescriptorRefreshIntervalMillis(300_000L);
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
        scheduler.setJanitorInterval(TimeValue.timeValueMillis(ServerlessBootstrap.DEFAULT_JANITOR_INTERVAL_MILLIS));
        node.setSignals(scheduler);
        return scheduler.start();
    }

    private static ServerlessNode joined(ServerlessNode node, MetadataPlane plane) throws Exception {
        node.startup().start();
        node.setMetadataPlane(plane);
        node.leases().renewLease(plane);
        return node;
    }

    private static ShardId openReader(ServerlessNode node, String indexName) {
        for (ShardId shardId : node.reconciler().readerShards()) {
            if (shardId.getIndexName().equals(indexName) && shardId.id() == 0) {
                return shardId;
            }
        }
        return null;
    }

    private static Settings extraFromProperty() {
        final Settings.Builder extra = Settings.builder();
        final String spec = System.getProperty("tests.meter.background.extra", "");
        for (String pair : spec.split(",")) {
            if (pair.isBlank()) {
                continue;
            }
            final int eq = pair.indexOf('=');
            extra.put(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
        }
        return extra.build();
    }

    /**
     * Runs one writer and one search node for {@code seconds} with {@code indices} indices in use, and logs the requests.
     *
     * @return requests per hour, by kind: puts, lists, gets
     */
    private long[] measure(String label, int indices, long seconds, Settings extra) throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            // Each node has its own plane over the one store, as separate processes would: a plane shared by two nodes
            // shares their lease bookkeeping, and every renewal then misses its compare-and-swap and retries.
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), System::currentTimeMillis, TTL);
            final MetadataPlane searchPlane = new MetadataPlane(store, BlobPath.cleanPath(), System::currentTimeMillis, TTL);
            final List<String> names = new ArrayList<>();
            for (int i = 0; i < indices; i++) {
                final String name = String.format(Locale.ROOT, "bg-%03d", i);
                plane.createIndex(new IndexDescriptor(name, "uuid-" + name, 1, MAPPING, null));
                names.add(name);
            }
            try (
                ServerlessNode writer = joined(new ServerlessNode(nodeSettings("bg-writer", "ingest", extra)), plane);
                ServerlessNode search = joined(new ServerlessNode(nodeSettings("bg-search", "search", extra)), searchPlane)
            ) {
                final BackgroundReconciler writerLoop = new BackgroundReconciler(writer, plane).setMaxShardsHeld(4 * indices + 4);
                for (String name : names) {
                    writerLoop.want(name, 0);
                }
                for (int pass = 0; pass < 20 && writer.reconciler().openShards().size() < indices; pass++) {
                    writerLoop.tick(System.currentTimeMillis());
                }
                final Map<String, ShardId> held = new ConcurrentHashMap<>();
                for (ShardId shardId : writer.reconciler().openShards()) {
                    held.put(shardId.getIndexName(), shardId);
                    writer.writes().index(shardId, "seed", "{\"msg\":\"seed\",\"n\":0}");
                }
                assertEquals(indices, held.size());
                writerLoop.publishAll();
                for (String name : names) {
                    final ShardId onSearch = search.readers().serveAsReader(searchPlane, name, 0);
                    assertEquals(1L, ShardOps.hits(search.searchService(), onSearch, "msg", "seed"));
                }
                plane.membership().refresh();
                searchPlane.membership().refresh();
                final List<ReconcileScheduler> schedulers = new ArrayList<>();
                schedulers.add(scheduled(writer, plane, writerLoop));
                schedulers.add(scheduled(search, searchPlane, new BackgroundReconciler(search, searchPlane)));
                try {
                    // A minute to settle: the first passes read what a node reads once.
                    TimeUnit.SECONDS.sleep(Math.min(60, seconds / 4));
                    final long[] looksBefore = search.router().commitPush().registerLooks().counts();
                    final long[] pushesBefore = search.router().commitPush().counts();
                    store.reset();
                    store.traceCallers(4);
                    final long started = System.nanoTime();
                    final long end = started + TimeUnit.SECONDS.toNanos(seconds);
                    long second = 0;
                    long writes = 0;
                    long searches = 0;
                    while (System.nanoTime() < end) {
                        // Spread over the interval: index i is written and searched in second i mod the interval.
                        for (int i = 0; i < names.size(); i++) {
                            final String name = names.get(i);
                            if ((second + i) % WRITE_EVERY_SECONDS == 0) {
                                writer.writes().index(held.get(name), "d" + second, "{\"msg\":\"w\",\"n\":" + second + "}");
                                writes++;
                            }
                            if ((second + i) % SEARCH_EVERY_SECONDS == SEARCH_EVERY_SECONDS / 2) {
                                try {
                                    // As the search path asks: a reader open here is checked, one that is not is opened.
                                    ShardId onSearch = openReader(search, name);
                                    if (onSearch != null) {
                                        search.readers().ensureReaderCurrent(searchPlane, name, 0, onSearch);
                                    } else {
                                        onSearch = search.readers().serveAsReader(searchPlane, name, 0);
                                    }
                                    ShardOps.hits(search.searchService(), onSearch, "msg", "w");
                                    searches++;
                                } catch (Exception e) {
                                    // A reader let go under the search: retried in a later second, as a client would.
                                }
                            }
                        }
                        second++;
                        final long wait = started + TimeUnit.SECONDS.toNanos(second) - System.nanoTime();
                        if (wait > 0) {
                            TimeUnit.NANOSECONDS.sleep(wait);
                        }
                    }
                    final double hours = (System.nanoTime() - started) / 3.6e12;
                    final long puts = store.blobWrites() + store.registerWrites();
                    final long lists = store.listings();
                    final long gets = store.blobReads() + store.registerReads();
                    final StringBuilder report = new StringBuilder();
                    report.append(
                        String.format(
                            Locale.ROOT,
                            "BGMETER|%s|%d indices, %.0f s, %d writes, %d searches|per hour: PUT=%.0f LIST=%.0f GET=%.0f DELETE=%.0f%n",
                            label,
                            indices,
                            hours * 3600,
                            writes,
                            searches,
                            puts / hours,
                            lists / hours,
                            gets / hours,
                            store.deletes() / hours
                        )
                    );
                    final long[] looks = search.router().commitPush().registerLooks().counts();
                    final long[] pushes = search.router().commitPush().counts();
                    report.append(
                        String.format(
                            Locale.ROOT,
                            "search node: register looks skipped=%d made=%d, pushes received=%d reopened=%d, pushed blocks taken=%d%n",
                            looks[0] - looksBefore[0],
                            looks[1] - looksBefore[1],
                            pushes[2] - pushesBefore[2],
                            pushes[3] - pushesBefore[3],
                            pushes[5] - pushesBefore[5]
                        )
                    );
                    report.append("search node block cache: ").append(search.blockCache().stats()).append('\n');
                    report.append(store.breakdown()).append('\n');
                    report.append(store.byCaller());
                    logger.info(report.toString());
                    return new long[] { Math.round(puts / hours), Math.round(lists / hours), Math.round(gets / hours) };
                } finally {
                    for (ReconcileScheduler scheduler : schedulers) {
                        scheduler.close();
                    }
                }
            }
        }
    }

    /** Two nodes with nothing to do, then the same two nodes with indices in use; the difference is per index. */
    public void testBackgroundRequestsPerNodeAndIndexHour() throws Exception {
        final String secondsProperty = System.getProperty("tests.meter.background.seconds", "");
        assumeTrue("a background meter runs only when -Dtests.meter.background.seconds is set", secondsProperty.isEmpty() == false);
        final long seconds = Long.parseLong(secondsProperty);
        final int indices = Integer.parseInt(System.getProperty("tests.meter.background.indices", "20"));
        final Settings extra = extraFromProperty();
        final long[] empty = measure("two nodes, no indices", 0, seconds, extra);
        final long[] used = measure("two nodes, indices in use", indices, seconds, extra);
        logger.info(
            "BGMETER|summary|{}|node pair per hour: PUT={} LIST={} GET={}|per index-hour on top: PUT={} LIST={} GET={}",
            extra,
            empty[0],
            empty[1],
            empty[2],
            String.format(Locale.ROOT, "%.1f", (used[0] - empty[0]) / (double) indices),
            String.format(Locale.ROOT, "%.1f", (used[1] - empty[1]) / (double) indices),
            String.format(Locale.ROOT, "%.1f", (used[2] - empty[2]) / (double) indices)
        );
    }
}
