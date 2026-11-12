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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What an idle writer costs open, hibernated and released, and what waking it costs.
 *
 * <p>Many single-shard indices, the shape of an index per tenant: each open writer carries its own index service and
 * mapper as well as its engine, which is what a node holding a few thousand of them pays for. Four numbers, logged
 * for the record:
 * <ul>
 *   <li>heap per writer, open and after hibernating every one of them;</li>
 *   <li>store requests in an idle hour per writer, open, hibernated and released, over what the node makes with
 *       nothing held at all;</li>
 *   <li>the first write's latency and store requests at a fixed store latency, waking from hibernation and taking a
 *       released shard back;</li>
 *   <li>and whether every acknowledged write is still there afterwards, which is asserted.</li>
 * </ul>
 * What is asserted beyond that is the shape the switch's default rests on: hibernated costs no more idle requests
 * than released, and waking costs fewer requests than a takeover.
 *
 * <p>{@code -Dtests.hibernate.indices} (default 120), {@code -Dtests.hibernate.wakes} (default 12) and
 * {@code -Dtests.hibernate.delay_millis} (default 20) size it.
 */
// Lucene's test filesystem refuses past 2,048 open handles, which a thousand open writers reach: about three each.
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems({ "ExtrasFS", "HandleLimitFS" })
public class WriterHibernationMeterTests extends OpenSearchTestCase {

    /** Longer than a bootstrapped node's, so that a pass every {@link #TICK} renews it as the node's own timer would. */
    private static final long TTL = 90_000L;
    /** The backstop interval a bootstrapped node runs at: the resync runs every tenth, as there. */
    private static final long TICK = 30_000L;
    private static final long HOUR = 3_600_000L;
    private static final long HIBERNATE_AFTER = 30_000L;

    /** A tenant-sized mapping: what a real index's mapper costs, not the one-field minimum. */
    private static final String MAPPING;
    static {
        final StringBuilder fields = new StringBuilder("{\"properties\":{");
        final String[] types = { "text", "keyword", "long", "date", "boolean", "double" };
        for (int i = 0; i < 24; i++) {
            if (i > 0) {
                fields.append(',');
            }
            fields.append("\"f").append(i).append("\":{\"type\":\"").append(types[i % types.length]).append("\"}");
        }
        MAPPING = fields.append("}}").toString();
    }

    private static int intProperty(String name, int fallback) {
        final String value = System.getProperty(name);
        return value == null || value.isEmpty() ? fallback : Integer.parseInt(value);
    }

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-hibernation-meter")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private static String name(int i) {
        return String.format(Locale.ROOT, "tenant-%04d", i);
    }

    private static String doc(int n) {
        return "{\"f0\":\"hello " + n + "\",\"f1\":\"k" + n + "\",\"f2\":" + n + ",\"f4\":true}";
    }

    private static long usedHeapBytes() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            System.gc();
            Thread.sleep(50);
        }
        final Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static long openFileDescriptors() throws java.io.IOException {
        final java.nio.file.Path fds = org.opensearch.common.io.PathUtils.get("/proc/self/fd");
        if (java.nio.file.Files.isDirectory(fds) == false) {
            return -1;
        }
        try (var listing = java.nio.file.Files.list(fds)) {
            return listing.count();
        }
    }

    private static long percentile(List<Long> values, int p) {
        final List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        return sorted.get(Math.min(sorted.size() - 1, (int) Math.ceil(p / 100.0 * sorted.size()) - 1));
    }

    /** One simulated hour of the node's own cadence, counted. */
    private static long idleHour(BackgroundReconciler loop, AtomicLong clock, CountingBlobStore counting) throws Exception {
        counting.reset();
        for (long t = 0; t < HOUR; t += TICK) {
            clock.addAndGet(TICK);
            loop.tick(clock.get());
            loop.orphanSlice().get();
            loop.reclaimSweep().get();
            loop.tombstoneSweep().get();
        }
        return counting.total();
    }

    public void testWhatAnIdleWriterCostsOpenHibernatedAndReleasedAndWhatWakingCosts() throws Exception {
        final int indices = intProperty("tests.hibernate.indices", 120);
        final int wakes = Math.min(indices / 2, intProperty("tests.hibernate.wakes", 12));
        final long delayMillis = intProperty("tests.hibernate.delay_millis", 20);
        final AtomicLong clock = new AtomicLong(1_000_000L);
        final StringBuilder report = new StringBuilder();
        final LatencyInjectingBlobStore latency = new LatencyInjectingBlobStore(new FsBlobStore(8192, createTempDir(), false), 0);
        try (CountingBlobStore counting = new CountingBlobStore(latency)) {
            final MetadataPlane plane = new MetadataPlane(counting, BlobPath.cleanPath(), clock::get, TTL);
            for (int i = 0; i < indices; i++) {
                plane.createIndex(new IndexDescriptor(name(i), "uuid-" + name(i), 1, MAPPING, null));
            }
            try (ServerlessNode node = new ServerlessNode(nodeSettings("hibernation-meter"))) {
                // A bootstrapped node's housekeeping intervals.
                node.setHeadVerifyIntervalMillis(300_000L).setDescriptorRefreshIntervalMillis(300_000L);
                node.startup().start();
                node.setMetadataPlane(plane);
                node.leases().renewLease(plane);
                plane.membership().refresh();
                final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true)
                    .setMaxShardsHeld(indices * 2)
                    .setIdleAfterMillis(0)
                    .setHibernateAfterMillis(0);
                // The orphan backstop visits a fixed number of shard containers an hour whatever is held; see IdleIndexRequestTests.
                loop.setOrphanSliceBudget(0);
                counting.traceCallers(8);

                // Nothing held: the node's own floor.
                final long floor = idleHour(loop, clock, counting);
                final long heapEmpty = usedHeapBytes();
                final long fdsEmpty = openFileDescriptors();

                final List<ShardId> writers = new ArrayList<>();
                final AtomicLong acknowledged = new AtomicLong();
                final long openingAt = System.nanoTime();
                for (int i = 0; i < indices; i++) {
                    writers.add(node.activation().activateWriter(plane, name(i), 0).orElseThrow());
                }
                final long openMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - openingAt);
                // In parallel: each write waits for the node log's window, and a thousand indices one after another is minutes.
                final java.util.concurrent.ExecutorService writing = java.util.concurrent.Executors.newFixedThreadPool(64);
                try {
                    final List<java.util.concurrent.Future<?>> seeded = new ArrayList<>();
                    for (ShardId shardId : writers) {
                        seeded.add(writing.submit(() -> {
                            for (int d = 0; d < 3; d++) {
                                node.writes().index(shardId, "seed-" + d, doc(d));
                                acknowledged.incrementAndGet();
                            }
                            return null;
                        }));
                    }
                    for (java.util.concurrent.Future<?> f : seeded) {
                        f.get();
                    }
                } finally {
                    writing.shutdown();
                }
                loop.publishAll();
                final long heapOpen = usedHeapBytes();
                final long fdsOpen = openFileDescriptors();
                final long openHour = idleHour(loop, clock, counting);
                final String openCallers = counting.byCaller();
                assertEquals("nothing released while both thresholds are off", indices, node.reconciler().openShards().size());

                // Hibernate every one of them, and keep the heads for longer than the hour measured.
                loop.setHibernateAfterMillis(HIBERNATE_AFTER).setIdleAfterMillis(4 * HOUR);
                clock.addAndGet(HIBERNATE_AFTER + TICK);
                counting.reset();
                loop.tick(clock.get());
                final long hibernateRequests = counting.total();
                final String hibernateCallers = counting.byCaller();
                assertEquals("every idle writer hibernated", 0, node.reconciler().openShards().size());
                assertEquals(indices, node.hibernation().size());
                final long heapHibernated = usedHeapBytes();
                final long fdsHibernated = openFileDescriptors();
                final long hibernatedHour = idleHour(loop, clock, counting);
                final String hibernatedCallers = counting.byCaller();
                assertEquals("still hibernated", indices, node.hibernation().size());
                for (int i = 0; i < indices; i++) {
                    assertEquals(
                        "a hibernated writer keeps its head",
                        node.localNode().getId(),
                        plane.heads().read(name(i), 0).orElseThrow().ownerNodeId()
                    );
                }

                // Wake half the sample by a write, at a real store's latency.
                latency.setDelayMillis(delayMillis);
                final List<Long> wakeMillis = new ArrayList<>();
                final List<Long> wakeRequests = new ArrayList<>();
                final List<Long> wakeOpenMillis = new ArrayList<>();
                for (int i = 0; i < wakes; i++) {
                    counting.reset();
                    final long startedAt = System.nanoTime();
                    final ShardId woken = node.activation().activateWriter(plane, name(i), 0).orElseThrow();
                    wakeOpenMillis.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
                    node.writes().index(woken, "woken", doc(100));
                    wakeMillis.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
                    wakeRequests.add(counting.total());
                    acknowledged.incrementAndGet();
                    assertEquals("woken at the term it hibernated at", writers.get(i).getIndex().getUUID(), woken.getIndex().getUUID());
                }
                final String wakeSequence = counting.breakdown();
                final String wakeCallers = counting.byCaller();
                latency.setDelayMillis(0);
                assertEquals(wakes, node.hibernation().wakes());

                // Now the rest: give the heads back, as idle release does, and take them back the ordinary way.
                loop.publishAll();
                loop.setHibernateAfterMillis(0).setIdleAfterMillis(HIBERNATE_AFTER);
                clock.addAndGet(4 * HOUR);
                counting.reset();
                loop.tick(clock.get());
                // The woken writers were open: what an idle release of an open writer costs, against hibernating one.
                final long releaseRequests = counting.total();
                final String releaseCallers = counting.byCaller();
                assertEquals("every writer given back", 0, node.reconciler().openShards().size());
                assertEquals(0, node.hibernation().size());
                final long heapReleased = usedHeapBytes();
                loop.setIdleAfterMillis(0);
                final long releasedHour = idleHour(loop, clock, counting);

                latency.setDelayMillis(delayMillis);
                final List<Long> takeMillis = new ArrayList<>();
                final List<Long> takeRequests = new ArrayList<>();
                final List<Long> takeOpenMillis = new ArrayList<>();
                for (int i = wakes; i < 2 * wakes; i++) {
                    counting.reset();
                    final long startedAt = System.nanoTime();
                    final ShardId taken = node.activation().activateWriter(plane, name(i), 0).orElseThrow();
                    takeOpenMillis.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
                    node.writes().index(taken, "taken", doc(200));
                    takeMillis.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
                    takeRequests.add(counting.total());
                    acknowledged.incrementAndGet();
                }
                final String takeSequence = counting.breakdown();
                final String takeCallers = counting.byCaller();
                latency.setDelayMillis(0);

                // Nothing acknowledged is missing.
                long found = 0;
                for (int i = 0; i < indices; i++) {
                    final ShardId open = node.activation().activateWriter(plane, name(i), 0).orElseThrow();
                    node.reconciler().shard(open).refresh("meter");
                    try (var searcher = node.reconciler().shard(open).acquireSearcher("meter")) {
                        found += searcher.getIndexReader().numDocs();
                    }
                }
                assertEquals("every acknowledged write survives hibernating, waking and being given back", acknowledged.get(), found);

                final double perWriterHour = 1.0 / indices;
                report.append(
                    String.format(
                        Locale.ROOT,
                        "%n%d single-shard indices, %d-field mapping, opened one after another in %d ms",
                        indices,
                        24,
                        openMillis
                    )
                );
                report.append(
                    String.format(
                        Locale.ROOT,
                        "%nheap per writer: open %d B, hibernated %d B, released %d B",
                        (heapOpen - heapEmpty) / indices,
                        (heapHibernated - heapEmpty) / indices,
                        (heapReleased - heapEmpty) / indices
                    )
                );
                report.append(
                    String.format(
                        Locale.ROOT,
                        "%nfile descriptors per writer: open %.2f, hibernated %.2f",
                        (fdsOpen - fdsEmpty) / (double) indices,
                        (fdsHibernated - fdsEmpty) / (double) indices
                    )
                );
                report.append(
                    String.format(
                        Locale.ROOT,
                        "%nidle hour: node floor %d requests; per writer-hour over it: open %.1f, hibernated %.1f, released %.1f",
                        floor,
                        (openHour - floor) * perWriterHour,
                        (hibernatedHour - floor) * perWriterHour,
                        (releasedHour - floor) * perWriterHour
                    )
                );
                report.append(String.format(Locale.ROOT, "%nhibernating all: %.1f requests per writer", hibernateRequests * perWriterHour));
                report.append(
                    String.format(
                        Locale.ROOT,
                        "%ngiving back %d open and %d hibernated: %d requests",
                        wakes,
                        indices - wakes,
                        releaseRequests
                    )
                );
                report.append(
                    String.format(
                        Locale.ROOT,
                        "%nfirst write at %d ms store latency: wake p50 %d ms (open %d ms) p99 %d ms, %d requests p50 (%s); "
                            + "take back p50 %d ms (open %d ms) p99 %d ms, %d requests p50 (%s)",
                        delayMillis,
                        percentile(wakeMillis, 50),
                        percentile(wakeOpenMillis, 50),
                        percentile(wakeMillis, 99),
                        percentile(wakeRequests, 50),
                        wakeSequence,
                        percentile(takeMillis, 50),
                        percentile(takeOpenMillis, 50),
                        percentile(takeMillis, 99),
                        percentile(takeRequests, 50),
                        takeSequence
                    )
                );
                logger.info("HIBERNATION|{}", report);
                logger.info("HIBERNATED-HOUR-CALLERS|{}", hibernatedCallers);
                logger.info("WAKE-CALLERS|{}", wakeCallers);
                logger.info("OPEN-HOUR-CALLERS|{}", openCallers);
                logger.info("HIBERNATE-CALLERS|{}", hibernateCallers);
                logger.info("RELEASE-CALLERS|{}", releaseCallers);
                logger.info("TAKE-CALLERS|{}", takeCallers);

                assertTrue(
                    "a hibernated writer must cost no more idle requests than a released one:" + report,
                    hibernatedHour <= releasedHour + indices / 10
                );
                assertTrue(
                    "waking must cost fewer requests than taking a released shard back:" + report,
                    percentile(wakeRequests, 50) < percentile(takeRequests, 50)
                );
            }
        }
    }
}
