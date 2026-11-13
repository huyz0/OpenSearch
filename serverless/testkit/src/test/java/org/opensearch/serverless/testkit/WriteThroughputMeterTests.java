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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * How many writes per second one writer node sustains while it holds thousands of shards, and what limits it.
 *
 * <p>One ingest node takes every shard (timed, with the file descriptors, heap and threads each open writer costs), then an
 * open load climbs a ladder of rates: writes arrive at random times whatever is acknowledged, spread over every shard, as
 * many independent clients would. Each write's latency is counted from when it arrived, so time spent waiting for a free
 * client is part of it. For each rate the meter reports what was acknowledged per second, p50/p99, CPU (per write, and by
 * thread group, which names the bottleneck), heap, and store requests. The ladder stops at the first rate the node cannot keep
 * up with. Optionally a second node then takes every shard over, which is how long a node holding that many takes to replace.
 *
 * <p>Off unless {@code -Dtests.tput.step_seconds} is set. {@code -Dtests.tput.shards} (3000), {@code -Dtests.tput.rates} a
 * comma-separated ladder of writes per second (500,1000,2000,4000,8000), {@code -Dtests.tput.batch} documents per request
 * (1: each write a single index request; more sends them as one bulk to one shard), {@code -Dtests.tput.active} how many
 * of the shards the writes go to (all), {@code -Dtests.tput.clients} the most
 * requests in flight (4096), {@code -Dtests.tput.cadence} {@code node} (default: publishes and renewals driven by the node's own
 * scheduler with its defaults, as a running node) or {@code meter} (every shard with anything new published once a second, as
 * the soak does), {@code -Dtests.tput.takeover} true to time a takeover at the end, {@code -Dtests.tput.activation} how many shards the take opens at once (8; pair it with {@code serverless.activation.concurrency} in the settings so the pool is sized to match), and {@code -Dtests.tput.settings}
 * extra node settings as {@code key=value,key=value}. {@link MeterStores} picks the store and the latency.
 */
@com.carrotsearch.randomizedtesting.annotations.TimeoutSuite(millis = 7_200_000)
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*") // thousands of shards need more handles than its file system allows
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters(filters = {
    org.opensearch.test.BouncyCastleThreadFilter.class,
    S3ClientThreads.class })
public class WriteThroughputMeterTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING =
        "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"},\"tag\":{\"type\":\"keyword\"}}}";
    /** A ladder step that acknowledges less than this share of what was offered is past what the node sustains. */
    private static final double KEEPS_UP = 0.95;
    /** Nor does one whose p99 is past this. */
    private static final double P99_LIMIT_MILLIS = 5_000;
    /** Arrivals waiting for a free client beyond this many are dropped and counted, so an overloaded step cannot run out of heap. */
    private static final int MAX_WAITING = 200_000;

    private Settings nodeSettings(String name, String extra) {
        final Settings.Builder builder = Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-tput")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest");
        for (String pair : extra.split(",")) {
            if (pair.isBlank() == false) {
                final String[] kv = pair.split("=", 2);
                builder.put(kv[0].trim(), kv[1].trim());
            }
        }
        return builder.build();
    }

    private static String name(int i) {
        return String.format(Locale.ROOT, "tput-%05d", i);
    }

    private static String doc(long n) {
        return "{\"msg\":\"order "
            + n
            + " shipped to customer "
            + (n % 9973)
            + " from warehouse "
            + (n % 17)
            + "\",\"n\":"
            + n
            + ",\"tag\":\"t"
            + (n % 50)
            + "\"}";
    }

    private static double percentile(List<Long> sortedNanos, double p) {
        if (sortedNanos.isEmpty()) {
            return 0;
        }
        final int at = (int) Math.min(sortedNanos.size() - 1, Math.ceil(p * sortedNanos.size()) - 1);
        return sortedNanos.get(Math.max(0, at)) / 1e6;
    }

    private static long usedHeapBytes() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            System.gc();
            Thread.sleep(100);
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

    /** Open file descriptors by what they point at: a file's extension, or the kind of descriptor. */
    private static Map<String, Integer> descriptorsByKind() {
        final Map<String, Integer> out = new TreeMap<>();
        final java.nio.file.Path fds = org.opensearch.common.io.PathUtils.get("/proc/self/fd");
        if (java.nio.file.Files.isDirectory(fds) == false) {
            return out;
        }
        try (var listing = java.nio.file.Files.list(fds)) {
            listing.forEach(fd -> {
                String kind;
                try {
                    final String target = java.nio.file.Files.readSymbolicLink(fd).toString();
                    final String base = target.substring(target.lastIndexOf('/') + 1);
                    if (target.startsWith("socket:") || target.startsWith("pipe:") || target.startsWith("anon_inode:")) {
                        kind = target.substring(0, target.indexOf(':'));
                    } else if (base.equals("write.lock")) {
                        kind = "write.lock";
                    } else if (base.contains(".")) {
                        kind = base.substring(base.lastIndexOf('.') + 1);
                    } else {
                        kind = target.startsWith("/proc") || target.startsWith("/dev") ? "system" : "other";
                    }
                    if (kind.length() > 12) {
                        kind = "other";
                    }
                } catch (java.io.IOException | RuntimeException e) {
                    kind = "gone";
                }
                out.merge(kind, 1, Integer::sum);
            });
        } catch (java.io.IOException e) {
            out.put("unreadable", 1);
        }
        return out;
    }

    /** This process's {@code write_bytes} and {@code cancelled_write_bytes} from /proc: bytes sent to the disk, and bytes dirtied then deleted before they were. */
    private static long[] processDiskWrites() {
        final long[] out = { -1, -1 };
        try {
            for (String line : java.nio.file.Files.readAllLines(org.opensearch.common.io.PathUtils.get("/proc/self/io"))) {
                if (line.startsWith("write_bytes:")) {
                    out[0] = Long.parseLong(line.substring(12).trim());
                } else if (line.startsWith("cancelled_write_bytes:")) {
                    out[1] = Long.parseLong(line.substring(22).trim());
                }
            }
        } catch (java.io.IOException | RuntimeException e) {
            // not Linux
        }
        return out;
    }

    /** What the writers put on local disk and in the memory tier over a step, per acknowledged document. */
    private static String localWrites(
        org.opensearch.serverless.store.LocalSegmentTier tier,
        org.opensearch.serverless.store.LocalSegmentTier.Stats before,
        long[] ioBefore,
        long docs
    ) {
        final var d = tier.stats().since(before);
        final long[] io = processDiskWrites();
        final double per = Math.max(1, docs);
        return String.format(
            Locale.ROOT,
            "memory tier %s (budget %d MB): disk %.0f B/doc (flush %.0f, merge %.0f, other %.0f), memory %.0f B/doc flushed;"
                + " spilled while writing %d KB, evicted %d KB in %d files, deleted in memory %d KB; held now %d KB in %d files,"
                + " peak %d KB, opens from memory %d; process write_bytes %d KB, cancelled %d KB",
            tier.enabled() ? "on" : "off",
            tier.maxBytes() >> 20,
            d.diskTotal() / per,
            d.diskFlush() / per,
            d.diskMerge() / per,
            d.diskOther() / per,
            d.memoryFlush() / per,
            d.spilledWhileWriting() >> 10,
            d.evicted() >> 10,
            d.evictedFiles(),
            d.deletedInMemory() >> 10,
            d.heldBytes() >> 10,
            d.heldFiles(),
            d.peakBytes() >> 10,
            d.memoryOpens(),
            io[0] < 0 ? -1 : (io[0] - ioBefore[0]) >> 10,
            io[1] < 0 ? -1 : (io[1] - ioBefore[1]) >> 10
        );
    }

    @org.opensearch.common.SuppressForbidden(reason = "the process's CPU time, which only the JDK's own bean reports")
    private static long processCpuNanos() {
        final java.lang.management.OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof com.sun.management.OperatingSystemMXBean sun) {
            return sun.getProcessCpuTime();
        }
        return -1;
    }

    /** CPU time so far of every live thread, summed by thread group (its name with numbers and brackets taken out). */
    private static Map<String, Long> cpuByGroup() {
        final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        final Map<String, Long> out = new HashMap<>();
        for (java.lang.management.ThreadInfo info : threads.getThreadInfo(threads.getAllThreadIds())) {
            if (info == null) {
                continue;
            }
            final long cpu = threads.getThreadCpuTime(info.getThreadId());
            if (cpu > 0) {
                out.merge(group(info.getThreadName()), cpu, Long::sum);
            }
        }
        return out;
    }

    private static String group(String thread) {
        if (thread.startsWith("tput-client")) {
            return "client (write path)";
        }
        if (thread.contains("TEST-")) {
            return "test (arrivals)";
        }
        if (thread.startsWith("tput-publisher")) {
            return "publisher";
        }
        return thread.replaceAll("\\[[^\\]]*\\]", "").replaceAll("#?\\d+", "").replaceAll("[-_ ]+$", "");
    }

    private static String topCpu(Map<String, Long> before, Map<String, Long> after, double seconds, int top) {
        final Map<String, Long> delta = new HashMap<>();
        for (Map.Entry<String, Long> each : after.entrySet()) {
            final long d = each.getValue() - before.getOrDefault(each.getKey(), 0L);
            if (d > 0) {
                delta.put(each.getKey(), d);
            }
        }
        final List<Map.Entry<String, Long>> sorted = new ArrayList<>(delta.entrySet());
        sorted.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));
        final StringBuilder out = new StringBuilder();
        for (Map.Entry<String, Long> each : sorted.subList(0, Math.min(top, sorted.size()))) {
            if (out.length() > 0) {
                out.append(", ");
            }
            out.append(each.getKey()).append(' ').append(String.format(Locale.ROOT, "%.2f", each.getValue() / 1e9 / seconds));
        }
        return out + " cores";
    }

    private static BackgroundReconciler takeAll(
        ServerlessNode node,
        MetadataPlane plane,
        java.util.function.LongSupplier clock,
        int shards,
        List<String> log
    ) throws Exception {
        node.leases().renewLease(plane);
        plane.membership().refresh();
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setMaxShardsHeld(shards * 2)
            .setActivationConcurrency(
                Integer.parseInt(
                    System.getProperty("tests.tput.activation", String.valueOf(BackgroundReconciler.DEFAULT_ACTIVATION_CONCURRENCY))
                )
            );
        for (int i = 0; i < shards; i++) {
            loop.want(name(i), 0);
        }
        for (int pass = 0; pass < 200 && node.reconciler().openShards().size() < shards; pass++) {
            final long started = System.nanoTime();
            loop.tick(clock.getAsLong());
            log.add((System.nanoTime() - started) / 1_000_000 + " ms -> " + node.reconciler().openShards().size());
        }
        assertEquals(shards, node.reconciler().openShards().size());
        return loop;
    }

    /** The meter's clients: plain threads, since they stand for callers outside the node, not work inside it. */
    @org.opensearch.common.SuppressForbidden(reason = "stands for clients outside the node, which carry no thread context")
    private static ThreadPoolExecutor clientPool(int clients) {
        return new ThreadPoolExecutor(clients, clients, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), runnable -> {
            final Thread thread = new Thread(runnable, "tput-client");
            thread.setDaemon(true);
            return thread;
        });
    }

    private record Step(double offered, double acked, double p50, double p99, double max, long failures, long dropped, String line) {
    }

    private Step step(
        ServerlessNode node,
        BackgroundReconciler loop,
        CountingBlobStore store,
        LatencyInjectingBlobStore slow,
        List<ShardId> held,
        double rate,
        int batch,
        long seconds,
        ThreadPoolExecutor clients,
        AtomicLong ids,
        boolean scheduled,
        int active
    ) throws Exception {
        store.reset();
        slow.drain();
        node.writes().ackTimings().reset();
        node.nodeLogWriter().committer().timings().reset();
        final ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();
        final AtomicLong failures = new AtomicLong();
        final AtomicLong dropped = new AtomicLong();
        final AtomicLong docs = new AtomicLong();
        final Map<String, String> firstFailure = new ConcurrentHashMap<>();
        final AtomicLong inFlight = new AtomicLong();
        final Map<String, Long> cpuBefore = cpuByGroup();
        final long cpuStarted = processCpuNanos();
        node.localSegments().resetPeak();
        final var localBefore = node.localSegments().stats();
        final long[] ioBefore = processDiskWrites();
        final long started = System.nanoTime();
        final long end = started + TimeUnit.SECONDS.toNanos(seconds);
        final Random arrivals = new Random(random().nextLong());
        final double requestRate = rate / batch;
        // The node's own cadence, on its own thread so a slow publish never holds back arrivals: publish what is due and
        // checkpoint, once a second.
        final AtomicLong published = new AtomicLong();
        final java.util.concurrent.atomic.AtomicBoolean publishing = new java.util.concurrent.atomic.AtomicBoolean(true);
        final AtomicLong publishNanos = new AtomicLong();
        final Thread publisher = new Thread(() -> {
            while (publishing.get() && scheduled == false) {
                final long passStarted = System.nanoTime();
                try {
                    published.addAndGet(loop.publishAll().size());
                    loop.checkpointDue();
                } catch (Exception e) {
                    logger.warn("publish pass failed", e);
                }
                final long took = System.nanoTime() - passStarted;
                publishNanos.addAndGet(took);
                final long rest = TimeUnit.SECONDS.toNanos(1) - took;
                if (rest > 0) {
                    try {
                        TimeUnit.NANOSECONDS.sleep(rest);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        }, "tput-publisher");
        publisher.start();
        // A write that has waited more than a second is sampled: where its thread is, so the report names what held it.
        final Map<Thread, Long> writing = new ConcurrentHashMap<>();
        final Map<String, Integer> stuck = new ConcurrentHashMap<>();
        final Thread sampler = new Thread(() -> {
            while (publishing.get()) {
                final long now = System.nanoTime();
                for (Map.Entry<Thread, Long> each : writing.entrySet()) {
                    if (now - each.getValue() > TimeUnit.SECONDS.toNanos(1)) {
                        final StringBuilder where = new StringBuilder();
                        int frames = 0;
                        for (StackTraceElement frame : each.getKey().getStackTrace()) {
                            final String c = frame.getClassName();
                            if (c.startsWith("org.opensearch.serverless") && frames < 4) {
                                where.append(c.substring(c.lastIndexOf('.') + 1)).append('.').append(frame.getMethodName()).append(' ');
                                frames++;
                            } else if (frames == 0 && where.length() < 200) {
                                where.append(c.substring(c.lastIndexOf('.') + 1)).append('.').append(frame.getMethodName()).append(' ');
                            }
                        }
                        stuck.merge(where.toString().trim(), 1, Integer::sum);
                    }
                }
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }, "tput-sampler");
        sampler.start();
        long next = started;
        long late = 0;
        while (next < end) {
            final long wait = next - System.nanoTime();
            if (wait > 0) {
                TimeUnit.NANOSECONDS.sleep(wait);
            } else {
                late = Math.max(late, -wait);
            }
            final long arrived = next;
            next += (long) (-Math.log(1 - arrivals.nextDouble()) / requestRate * 1e9);
            if (clients.getQueue().size() >= MAX_WAITING) {
                dropped.addAndGet(batch);
                continue;
            }
            final ShardId shardId = held.get(arrivals.nextInt(Math.min(active, held.size())));
            final long first = ids.getAndAdd(batch);
            inFlight.incrementAndGet();
            clients.execute(() -> {
                writing.put(Thread.currentThread(), System.nanoTime());
                try {
                    if (batch == 1) {
                        node.writes().index(shardId, "w" + first, doc(first));
                    } else {
                        final List<WalRecord> records = new ArrayList<>(batch);
                        for (int i = 0; i < batch; i++) {
                            records.add(new WalRecord("w" + (first + i), doc(first + i)));
                        }
                        node.writes().bulk(shardId, records);
                    }
                    latencies.add(System.nanoTime() - arrived);
                    docs.addAndGet(batch);
                } catch (Throwable t) {
                    failures.addAndGet(batch);
                    firstFailure.putIfAbsent(t.getClass().getSimpleName(), String.valueOf(t.getMessage()));
                } finally {
                    writing.remove(Thread.currentThread());
                    inFlight.decrementAndGet();
                }
            });
        }
        // What arrived in the step is waited for, so a slow ack still counts against the rate that offered it.
        final long drainBy = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (inFlight.get() > 0 && System.nanoTime() < drainBy) {
            Thread.sleep(20);
        }
        publishing.set(false);
        publisher.join();
        sampler.join();
        final double secondsRun = (System.nanoTime() - started) / 1e9;
        final double cpuSeconds = (processCpuNanos() - cpuStarted) / 1e9;
        final String cpu = topCpu(cpuBefore, cpuByGroup(), secondsRun, 6);
        final Map<String, Integer> byOp = new TreeMap<>();
        for (LatencyInjectingBlobStore.Request request : slow.drain()) {
            final String key = request.key();
            final String kind = key.contains("node_log") || key.contains("nodelog") ? "log"
                : key.contains("/packs/") ? "packs"
                : key.contains("manifest") ? "manifest"
                : key.contains("/segments/") ? "segments"
                : key.contains("lease") ? "lease"
                : "other";
            byOp.merge(request.op() + " " + kind, 1, Integer::sum);
        }
        final List<Long> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);
        final long acked = docs.get();
        final double offered = rate;
        final double ackedPerSecond = acked / (double) seconds;
        final long logPuts = node.nodeLogWriter().committer().timings().puts();
        final long puts = store.blobWrites() + store.registerWrites();
        final long gets = store.blobReads() + store.registerReads();
        final Runtime runtime = Runtime.getRuntime();
        final String line = String.format(
            Locale.ROOT,
            "offered %.0f docs/s (batch %d, over %d shards) -> acked %.0f/s (%d docs, %d failed, %d dropped, %d still in flight)"
                + "|p50 %.0f p90 %.0f p99 %.0f max %.0f ms from arrival"
                + "|CPU %.2f cores of %d, %.3f ms per doc; by thread: %s"
                + "|heap used %d MB of %d MB (%d MB live after GC), fds %d %s, threads %d"
                + "|log PUTs %.1f/s, %.0f docs per log PUT; store PUT %.1f/s GET %.1f/s LIST %.1f/s; publishes %.1f/s"
                + " (publish passes busy %.0f%% of the time); arrivals at most %.0f ms late"
                + "|local writes: %s"
                + "|requests %s|steps %s|log %s%s%s",
            offered,
            batch,
            Math.min(active, held.size()),
            ackedPerSecond,
            acked,
            failures.get(),
            dropped.get(),
            inFlight.get(),
            percentile(sorted, 0.50),
            percentile(sorted, 0.90),
            percentile(sorted, 0.99),
            percentile(sorted, 1.0),
            cpuSeconds / secondsRun,
            Runtime.getRuntime().availableProcessors(),
            acked == 0 ? 0 : cpuSeconds * 1e3 / acked,
            cpu,
            (runtime.totalMemory() - runtime.freeMemory()) >> 20,
            runtime.maxMemory() >> 20,
            usedHeapBytes() >> 20,
            openFileDescriptors(),
            descriptorsByKind(),
            ManagementFactory.getThreadMXBean().getThreadCount(),
            logPuts / secondsRun,
            logPuts == 0 ? 0 : acked / (double) logPuts,
            puts / secondsRun,
            gets / secondsRun,
            store.listings() / secondsRun,
            published.get() / secondsRun,
            100.0 * publishNanos.get() / 1e9 / secondsRun,
            late / 1e6,
            localWrites(node.localSegments(), localBefore, ioBefore, acked),
            byOp,
            node.writes().ackTimings().describe(),
            node.nodeLogWriter().committer().timings().describe(),
            firstFailure.isEmpty() ? "" : "|failures " + firstFailure,
            stuck.isEmpty() ? "" : "|writes held over 1 s, sampled every 250 ms, by where: " + new TreeMap<>(stuck)
        );
        logger.info("TPUT|{}", line);
        return new Step(
            offered,
            ackedPerSecond,
            percentile(sorted, 0.50),
            percentile(sorted, 0.99),
            percentile(sorted, 1.0),
            failures.get(),
            dropped.get(),
            line
        );
    }

    private void measure(
        int shards,
        double[] rates,
        int batch,
        long stepSeconds,
        int clientCount,
        boolean takeover,
        String cadence,
        int active,
        String extra
    ) throws Exception {
        // Held still while the node takes its shards (one pass of thousands of opens is longer than a lease), then real time
        // plus an offset the takeover moves on: the node's own scheduler publishes and renews by this clock.
        final AtomicLong offset = new AtomicLong();
        final AtomicLong frozen = new AtomicLong(System.currentTimeMillis());
        final java.util.function.LongSupplier clock = () -> {
            final long still = frozen.get();
            return still >= 0 ? still : System.currentTimeMillis() + offset.get();
        };
        final org.opensearch.common.blobstore.BlobStore inner = MeterStores.create(createTempDir(), createTempDir());
        final LatencyInjectingBlobStore slow = new LatencyInjectingBlobStore(inner, 0);
        final List<String> report = new ArrayList<>();
        try (CountingBlobStore store = new CountingBlobStore(slow)) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock, TTL);
            final long createStarted = System.nanoTime();
            for (int i = 0; i < shards; i++) {
                plane.createIndex(new IndexDescriptor(name(i), "uuid-" + name(i), 1, MAPPING, null));
            }
            logger.info("TPUT|created {} indices in {} s", shards, (System.nanoTime() - createStarted) / 1_000_000_000);
            final ServerlessNode node = new ServerlessNode(nodeSettings("tput-writer", extra));
            final ThreadPoolExecutor clients = clientPool(clientCount);
            org.opensearch.serverless.reconcile.ReconcileScheduler scheduler = null;
            try (ServerlessNode successor = new ServerlessNode(nodeSettings("tput-successor", extra))) {
                node.startup().start();
                node.setMetadataPlane(plane);
                final long heapEmpty = usedHeapBytes();
                final long fdsEmpty = openFileDescriptors();
                final int threadsEmpty = ManagementFactory.getThreadMXBean().getThreadCount();
                MeterStores.slowDown(slow);
                store.reset();
                final List<String> passes = new ArrayList<>();
                final long openStarted = System.nanoTime();
                final long openCpu = processCpuNanos();
                final BackgroundReconciler loop = takeAll(node, plane, clock, shards, passes);
                final double openSeconds = (System.nanoTime() - openStarted) / 1e9;
                final double openCpuSeconds = (processCpuNanos() - openCpu) / 1e9;
                final long openRequests = store.total();
                final List<ShardId> held = new ArrayList<>(node.reconciler().openShards());
                // One write to each shard, so every shard is admitted to the log's epoch and has a live index writer.
                final long warmStarted = System.nanoTime();
                final ThreadPoolExecutor warm = clients;
                final java.util.concurrent.CountDownLatch warmed = new java.util.concurrent.CountDownLatch(held.size());
                // At most a few hundred at once, under the node's write limit.
                final java.util.concurrent.Semaphore warming = new java.util.concurrent.Semaphore(256);
                for (ShardId shardId : held) {
                    warming.acquire();
                    warm.execute(() -> {
                        try {
                            // Refused for back-pressure while thousands of shards join the log at once: wait as told, and again.
                            for (int attempt = 0;; attempt++) {
                                try {
                                    node.writes().index(shardId, "warm", doc(0));
                                    break;
                                } catch (org.opensearch.serverless.store.WriteBackpressure.ThrottledException e) {
                                    if (attempt >= 20) {
                                        throw e;
                                    }
                                    Thread.sleep(e.retryAfterSeconds() * 1000L);
                                }
                            }
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        } finally {
                            warming.release();
                            warmed.countDown();
                        }
                    });
                }
                assertTrue(warmed.await(10, TimeUnit.MINUTES));
                final double warmSeconds = (System.nanoTime() - warmStarted) / 1e9;
                // The write limiter cuts its limit while the first writes queue; it grows back a little per append.
                Thread.sleep(30_000);
                loop.publishAll();
                offset.set(frozen.get() - System.currentTimeMillis());
                frozen.set(-1L);
                node.leases().renewLease(plane);
                if (cadence.equals("node")) {
                    // A node built directly re-reads every head and descriptor on each renewal; a running one every five
                    // minutes. Without this, those reads are most of the GETs the meter counts.
                    node.setHeadVerifyIntervalMillis(
                        org.opensearch.serverless.shell.ServerlessBootstrap.DEFAULT_HEAD_VERIFY_INTERVAL_MILLIS
                    );
                    node.setDescriptorRefreshIntervalMillis(
                        org.opensearch.serverless.shell.ServerlessBootstrap.DEFAULT_DESCRIPTOR_REFRESH_INTERVAL_MILLIS
                    );
                    // Driven as a running node drives it (ServerlessBootstrap's wiring with its defaults): lazy publish on the
                    // log's window, edge publishes spaced out, lease renewals and the backstop on their own timers.
                    final long window = node.settings().getAsLong("serverless.wal.node_log.window_millis", 250L);
                    loop.setLazyPublish(
                        node.settings().getAsLong("serverless.wal.node_log.publish_max_age_millis", 1_000L),
                        node.settings().getAsLong("serverless.publish.max_unpublished_operations", 10_000L),
                        window
                    );
                    scheduler = new org.opensearch.serverless.reconcile.ReconcileScheduler(
                        loop,
                        node.threadPool(),
                        clock,
                        org.opensearch.common.unit.TimeValue.timeValueMillis(
                            TTL / org.opensearch.serverless.reconcile.ReconcileScheduler.RENEWALS_PER_TTL
                        ),
                        org.opensearch.common.unit.TimeValue.timeValueMillis(window),
                        org.opensearch.serverless.reconcile.ReconcileScheduler.DEFAULT_BACKSTOP_INTERVAL
                    );
                    scheduler.setPublishSpacing(
                        org.opensearch.common.unit.TimeValue.timeValueMillis(
                            node.settings().getAsLong("serverless.publish.min_interval_millis", 5_000L)
                        ),
                        node.settings().getAsLong("serverless.publish.max_unpublished_operations", 10_000L)
                    );
                    node.setSignals(scheduler);
                    scheduler.start();
                }
                final long heapOpen = usedHeapBytes();
                final long fdsOpen = openFileDescriptors();
                final int threadsOpen = ManagementFactory.getThreadMXBean().getThreadCount();
                final String opened = String.format(
                    Locale.ROOT,
                    "%d shards on one node, %s|open every shard: %.1f s (%.1f ms per shard, %.2f cores, %.1f requests per shard,"
                        + " %d passes: %s)|first write to every shard %.1f s"
                        + "|per open writer: %.1f fds, %d KB heap, %.2f threads (node %d fds %s, %d MB heap, %d threads)",
                    shards,
                    MeterStores.describe(),
                    openSeconds,
                    openSeconds * 1e3 / shards,
                    openCpuSeconds / openSeconds,
                    openRequests / (double) shards,
                    passes.size(),
                    passes.size() > 6 ? passes.subList(0, 6) + "..." : passes,
                    warmSeconds,
                    (fdsOpen - fdsEmpty) / (double) shards,
                    (heapOpen - heapEmpty) / shards / 1024,
                    (threadsOpen - threadsEmpty) / (double) shards,
                    fdsOpen,
                    descriptorsByKind(),
                    heapOpen >> 20,
                    threadsOpen
                );
                logger.info("TPUT|{}", opened);
                report.add(opened);

                final AtomicLong ids = new AtomicLong(1);
                double sustained = 0;
                for (double rate : rates) {
                    final Step step = step(
                        node,
                        loop,
                        store,
                        slow,
                        held,
                        rate,
                        batch,
                        stepSeconds,
                        clients,
                        ids,
                        scheduler != null,
                        active
                    );
                    report.add(step.line());
                    final boolean keepsUp = step.acked() >= KEEPS_UP * step.offered()
                        && step.p99() <= P99_LIMIT_MILLIS
                        && step.failures() == 0
                        && step.dropped() == 0;
                    if (keepsUp == false) {
                        report.add(String.format(Locale.ROOT, "stops keeping up at %.0f docs/s offered", rate));
                        break;
                    }
                    sustained = step.acked();
                }
                report.add(String.format(Locale.ROOT, "sustained: %.0f docs/s at batch %d", sustained, batch));

                if (takeover) {
                    // The writer stops; a successor takes every shard and replays what was not yet published.
                    final List<ShardId> unused = new ArrayList<>(held);
                    if (scheduler != null) {
                        scheduler.close();
                    }
                    offset.addAndGet(2 * TTL);
                    frozen.set(clock.getAsLong());
                    successor.startup().start();
                    successor.setMetadataPlane(plane);
                    store.reset();
                    final List<String> takePasses = new ArrayList<>();
                    final long takeStarted = System.nanoTime();
                    takeAll(successor, plane, clock, shards, takePasses);
                    final double takeSeconds = (System.nanoTime() - takeStarted) / 1e9;
                    final String took = String.format(
                        Locale.ROOT,
                        "takeover of %d shards: %.1f s (%.1f ms per shard, %.1f requests per shard, %d passes), log records read %d",
                        unused.size(),
                        takeSeconds,
                        takeSeconds * 1e3 / shards,
                        store.total() / (double) shards,
                        takePasses.size(),
                        successor.reconciler().logRecordsRead()
                    );
                    logger.info("TPUT|{}", took);
                    report.add(took);
                }
            } finally {
                if (scheduler != null) {
                    scheduler.close();
                }
                clients.shutdownNow();
                try {
                    node.close();
                } catch (Exception e) {
                    // It may have lost every shard to the successor.
                }
            }
        }
        logger.info("TPUTREPORT|cadence {}, {}{}", cadence, extra.isBlank() ? "defaults" : extra, "\n  " + String.join("\n  ", report));
    }

    public void testHowManyWritesOneNodeSustainsAcrossThousandsOfShards() throws Exception {
        final String stepProperty = System.getProperty("tests.tput.step_seconds", "");
        assumeTrue("the meter runs only when -Dtests.tput.step_seconds is set", stepProperty.isEmpty() == false);
        final long stepSeconds = Long.parseLong(stepProperty);
        final int shards = Integer.parseInt(System.getProperty("tests.tput.shards", "3000"));
        final String[] ladder = System.getProperty("tests.tput.rates", "500,1000,2000,4000,8000").split(",");
        final double[] rates = new double[ladder.length];
        for (int i = 0; i < ladder.length; i++) {
            rates[i] = Double.parseDouble(ladder[i].trim());
        }
        final int batch = Integer.parseInt(System.getProperty("tests.tput.batch", "1"));
        final int clients = Integer.parseInt(System.getProperty("tests.tput.clients", "4096"));
        final boolean takeover = Boolean.parseBoolean(System.getProperty("tests.tput.takeover", "false"));
        final String extra = System.getProperty("tests.tput.settings", "");
        final String cadence = System.getProperty("tests.tput.cadence", "node");
        final int active = Integer.parseInt(System.getProperty("tests.tput.active", String.valueOf(shards)));
        measure(shards, rates, batch, stepSeconds, clients, takeover, cadence, active, extra);
    }
}
