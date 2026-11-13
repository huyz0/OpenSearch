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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

/**
 * What large documents -- whole wiki pages, text pulled out of PDF and Word files -- do to the write path, next to the small
 * documents every other meter writes: acknowledgement latency by document size, the node log's blobs and PUTs, segment
 * publishes, heap and allocation per byte written, and the local translog on disk.
 *
 * <p>An open load (random arrivals at a fixed rate of documents per second) against one ingest node on a store that takes a
 * first-byte latency per request plus a transfer time per byte, as one connection to an object store does. Each document is
 * a JSON object with a title and a body of words drawn from a fixed vocabulary, so it compresses about as well as prose, with
 * typographic quotes and dashes in it as text copied from a document editor has; those make a Java string hold two bytes a
 * character.
 *
 * <p>Off unless {@code -Dtests.bigdoc.seconds} is set (per mix). {@code -Dtests.bigdoc.mixes} is a comma-separated list of
 * {@code 1k}, {@code 64k}, {@code 1m}, {@code 4m}, or {@code pages} (85% 4 KB, 12% 60 KB, 2.5% 600 KB, 0.5% 4 MB; about 46 KB
 * a document); {@code -Dtests.bigdoc.rates} the documents per second for each mix, in the same order (or one for all);
 * {@code -Dtests.bigdoc.shards} (20); {@code -Dtests.bigdoc.mbps} the per-request transfer rate in MB/s (80, about what one S3
 * connection moves; 0 for none); {@code -Dtests.bigdoc.ascii} true to leave the typographic characters out; and
 * {@code -Dtests.bigdoc.settings} extra node settings as {@code key=value,key=value}.
 */
@com.carrotsearch.randomizedtesting.annotations.TimeoutSuite(millis = 7_200_000)
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*")
public class LargeDocumentMeterTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING =
        "{\"properties\":{\"title\":{\"type\":\"text\"},\"body\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    /** A size class: its name, a document size, and its share of the mix. */
    private record Size(String name, int bytes, double share) {
    }

    private static List<Size> mix(String name) {
        return switch (name) {
            case "1k" -> List.of(new Size("1k", 1024, 1.0));
            case "64k" -> List.of(new Size("64k", 64 * 1024, 1.0));
            case "1m" -> List.of(new Size("1m", 1024 * 1024, 1.0));
            case "4m" -> List.of(new Size("4m", 4 * 1024 * 1024, 1.0));
            case "pages" -> List.of(
                new Size("4k", 4 * 1024, 0.85),
                new Size("60k", 60 * 1024, 0.12),
                new Size("600k", 600 * 1024, 0.025),
                new Size("4m", 4 * 1024 * 1024, 0.005)
            );
            default -> throw new IllegalArgumentException("unknown mix [" + name + "]");
        };
    }

    private static Size pick(List<Size> sizes, Random random) {
        double r = random.nextDouble();
        for (Size size : sizes) {
            r -= size.share();
            if (r < 0) {
                return size;
            }
        }
        return sizes.get(sizes.size() - 1);
    }

    private static final String[] WORDS = vocabulary();

    private static String[] vocabulary() {
        final Random random = new Random(42);
        final String[] words = new String[4096];
        for (int i = 0; i < words.length; i++) {
            final StringBuilder word = new StringBuilder();
            final int length = 2 + random.nextInt(9);
            for (int c = 0; c < length; c++) {
                word.append((char) ('a' + random.nextInt(26)));
            }
            words[i] = word.toString();
        }
        return words;
    }

    /** About {@code bytes} of UTF-8 JSON: a title and a body of prose-like text. */
    static String document(long n, int bytes, boolean ascii, Random random) {
        final StringBuilder body = new StringBuilder(bytes + 64);
        int written = 0;
        int sentence = 0;
        while (written < bytes - 80) {
            // Zipf-ish: common words much more often, as in prose.
            final String word = WORDS[(int) Math.min(WORDS.length - 1, Math.abs(random.nextGaussian()) * 400)];
            body.append(word);
            written += word.length();
            if (++sentence % 14 == 0) {
                if (ascii) {
                    body.append(". ");
                    written += 2;
                } else {
                    // A quoted phrase and a dash, as a document editor writes them: three bytes each in UTF-8.
                    body.append(" — “said”. ");
                    written += 16;
                }
            } else {
                body.append(' ');
                written++;
            }
        }
        return "{\"title\":\"page " + n + "\",\"n\":" + n + ",\"body\":\"" + body + "\"}";
    }

    private Settings nodeSettings(Path home, String extra) {
        final Settings.Builder builder = Settings.builder()
            .put("node.name", "bigdoc-writer")
            .put("cluster.name", "serverless-bigdoc")
            .put("path.home", home)
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
        return String.format(Locale.ROOT, "bigdoc-%03d", i);
    }

    private static double percentile(List<Long> sortedNanos, double p) {
        if (sortedNanos.isEmpty()) {
            return 0;
        }
        final int at = (int) Math.min(sortedNanos.size() - 1, Math.ceil(p * sortedNanos.size()) - 1);
        return sortedNanos.get(Math.max(0, at)) / 1e6;
    }

    @org.opensearch.common.SuppressForbidden(reason = "bytes allocated per thread, which only the JDK's own bean reports")
    private static long allocatedBytes() {
        final com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long total = 0;
        for (long id : threads.getAllThreadIds()) {
            final long each = threads.getThreadAllocatedBytes(id);
            if (each > 0) {
                total += each;
            }
        }
        return total;
    }

    @org.opensearch.common.SuppressForbidden(reason = "the process's CPU time, which only the JDK's own bean reports")
    private static long processCpuNanos() {
        return ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime();
    }

    /** Heap still in use after the last collection, summed over the heap pools: the live set, not garbage. */
    private static long liveAfterLastGc() {
        long total = 0;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP && pool.getCollectionUsage() != null) {
                total += pool.getCollectionUsage().getUsed();
            }
        }
        return total;
    }

    private static long translogBytes(Path home) throws IOException {
        final long[] total = { 0 };
        try (Stream<Path> walk = Files.walk(home)) {
            walk.filter(p -> p.getParent() != null && p.getParent().getFileName().toString().equals("translog"))
                .filter(Files::isRegularFile)
                .forEach(p -> {
                    try {
                        total[0] += Files.size(p);
                    } catch (IOException e) {
                        // Rolled away while walking.
                    }
                });
        } catch (java.io.UncheckedIOException e) {
            // A directory went while walking; the next sample counts it.
        }
        return total[0];
    }

    private static String mb(double bytes) {
        return String.format(Locale.ROOT, "%.1f", bytes / (1024 * 1024));
    }

    private void measure(String mixName, double rate, int shards, long seconds, long bytesPerSecond, boolean ascii, String extra)
        throws Exception {
        final List<Size> sizes = mix(mixName);
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final Path root = createTempDir();
        final Path home = createTempDir();
        final LatencyInjectingBlobStore slow = new LatencyInjectingBlobStore(new FsBlobStore(8192, root, false), 0);
        try (CountingBlobStore store = new CountingBlobStore(slow)) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            for (int i = 0; i < shards; i++) {
                plane.createIndex(new IndexDescriptor(name(i), "uuid-" + name(i), 1, MAPPING, null));
            }
            try (ServerlessNode node = new ServerlessNode(nodeSettings(home, extra))) {
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
                for (ShardId shardId : held) {
                    node.writes().index(shardId, "warm", "{\"title\":\"warm\",\"n\":0,\"body\":\"warm\"}");
                }
                // Documents made before timing starts, so building them costs the run nothing: a pool per size class.
                final Random random = new Random(random().nextLong());
                final Map<String, List<String>> pool = new TreeMap<>();
                for (Size size : sizes) {
                    final List<String> docs = new ArrayList<>();
                    final int count = size.bytes() >= 1024 * 1024 ? 4 : 32;
                    for (int i = 0; i < count; i++) {
                        docs.add(document(i, size.bytes(), ascii, random));
                    }
                    pool.put(size.name(), docs);
                }
                slow.setDelayMillis(20);
                slow.setTail(50, 200);
                slow.setBytesPerSecond(bytesPerSecond);
                store.reset();
                slow.resetPutBytes();
                node.writes().ackTimings().reset();
                node.nodeLogWriter().committer().timings().reset();

                final Map<String, ConcurrentLinkedQueue<Long>> acks = new ConcurrentHashMap<>();
                final Map<String, LongAdder> refused = new ConcurrentHashMap<>();
                final Map<String, LongAdder> failedBy = new ConcurrentHashMap<>();
                final LongAdder sourceBytes = new LongAdder();
                final AtomicBoolean running = new AtomicBoolean(true);
                final ExecutorService clients = Executors.newFixedThreadPool(256);
                final List<Future<?>> writing = new ArrayList<>();
                final long[] peakLive = { 0 };
                final long[] peakTranslog = { 0 };
                final long[] peakUsed = { 0 };
                final Thread sampler = new Thread(() -> {
                    while (running.get()) {
                        peakLive[0] = Math.max(peakLive[0], liveAfterLastGc());
                        peakUsed[0] = Math.max(peakUsed[0], Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
                        try {
                            peakTranslog[0] = Math.max(peakTranslog[0], translogBytes(home));
                            Thread.sleep(250);
                        } catch (IOException | InterruptedException e) {
                            return;
                        }
                    }
                }, "bigdoc-sampler");
                final boolean profile = Boolean.parseBoolean(System.getProperty("tests.bigdoc.profile", "false"));
                final Map<String, LongAdder> hot = new ConcurrentHashMap<>();
                final Thread profiler = new Thread(() -> {
                    final java.lang.management.ThreadMXBean threads = ManagementFactory.getThreadMXBean();
                    while (running.get()) {
                        for (java.lang.management.ThreadInfo info : threads.dumpAllThreads(false, false, 12)) {
                            if (info == null || info.getThreadState() != Thread.State.RUNNABLE) {
                                continue;
                            }
                            final StackTraceElement[] stack = info.getStackTrace();
                            if (stack.length == 0 || info.getThreadName().startsWith("bigdoc-")) {
                                continue;
                            }
                            final StringBuilder key = new StringBuilder();
                            for (int f = 0; f < Math.min(4, stack.length); f++) {
                                key.append(f == 0 ? "" : " < ")
                                    .append(stack[f].getClassName().replaceAll(".*\\.", ""))
                                    .append('.')
                                    .append(stack[f].getMethodName());
                            }
                            if (key.indexOf("EPoll") >= 0 || key.indexOf("socketAccept") >= 0 || key.indexOf("Unsafe.park") >= 0) {
                                continue;
                            }
                            hot.computeIfAbsent(key.toString(), k -> new LongAdder()).increment();
                        }
                        try {
                            Thread.sleep(20);
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                }, "bigdoc-profiler");
                if (profile) {
                    profiler.start();
                }
                final long allocatedBefore = allocatedBytes();
                final long cpuBefore = processCpuNanos();
                final long started = System.nanoTime();
                final long end = started + TimeUnit.SECONDS.toNanos(seconds);
                long published = 0;
                sampler.start();
                try {
                    final Thread arrivals = new Thread(() -> {
                        final Random arrivalRandom = new Random(random.nextLong());
                        long next = started;
                        long n = 0;
                        try {
                            while (next < end) {
                                final long wait = next - System.nanoTime();
                                if (wait > 0) {
                                    TimeUnit.NANOSECONDS.sleep(wait);
                                }
                                final ShardId shardId = held.get(arrivalRandom.nextInt(held.size()));
                                final Size size = pick(sizes, arrivalRandom);
                                final List<String> docs = pool.get(size.name());
                                final String source = docs.get(arrivalRandom.nextInt(docs.size()));
                                final long id = n++;
                                synchronized (writing) {
                                    writing.add(clients.submit(() -> {
                                        final long at = System.nanoTime();
                                        try {
                                            node.writes().index(shardId, "d" + id, source);
                                            acks.computeIfAbsent(size.name(), k -> new ConcurrentLinkedQueue<>())
                                                .add(System.nanoTime() - at);
                                            sourceBytes.add(org.opensearch.serverless.store.WalRecord.contentBytes(null, source));
                                        } catch (org.opensearch.serverless.store.WriteBackpressure.ThrottledException e) {
                                            refused.computeIfAbsent(size.name(), k -> new LongAdder()).increment();
                                        } catch (Exception e) {
                                            failedBy.computeIfAbsent(size.name() + " " + e.getClass().getSimpleName(), k -> new LongAdder())
                                                .increment();
                                        }
                                        return null;
                                    }));
                                }
                                next += (long) (-Math.log(1 - arrivalRandom.nextDouble()) / rate * 1e9);
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }, "bigdoc-arrivals");
                    arrivals.start();
                    while (System.nanoTime() < end) {
                        final long passStarted = System.nanoTime();
                        published += loop.publishAll().size();
                        final long rest = TimeUnit.SECONDS.toNanos(1) - (System.nanoTime() - passStarted);
                        if (rest > 0) {
                            TimeUnit.NANOSECONDS.sleep(Math.min(rest, Math.max(0, end - System.nanoTime())));
                        }
                    }
                    arrivals.join();
                    final List<Future<?>> all;
                    synchronized (writing) {
                        all = new ArrayList<>(writing);
                    }
                    for (Future<?> each : all) {
                        each.get();
                    }
                    final double secondsRun = (System.nanoTime() - started) / 1e9;
                    final double cpuSeconds = (processCpuNanos() - cpuBefore) / 1e9;
                    final long allocated = allocatedBytes() - allocatedBefore;
                    running.set(false);
                    sampler.join();
                    if (profile) {
                        profiler.join();
                        final StringBuilder top = new StringBuilder();
                        hot.entrySet()
                            .stream()
                            .sorted((a, b) -> Long.compare(b.getValue().sum(), a.getValue().sum()))
                            .limit(25)
                            .forEach(e -> top.append('\n').append(e.getValue().sum()).append(' ').append(e.getKey()));
                        logger.info("BIGDOCPROFILE|{}{}", mixName, top);
                    }

                    final StringBuilder bySize = new StringBuilder();
                    long acked = 0;
                    for (Size size : sizes) {
                        final List<Long> sorted = new ArrayList<>(acks.getOrDefault(size.name(), new ConcurrentLinkedQueue<>()));
                        Collections.sort(sorted);
                        acked += sorted.size();
                        bySize.append(
                            String.format(
                                Locale.ROOT,
                                " %s: n=%d p50=%.0f p99=%.0f max=%.0f refused=%d;",
                                size.name(),
                                sorted.size(),
                                percentile(sorted, 0.5),
                                percentile(sorted, 0.99),
                                percentile(sorted, 1.0),
                                refused.getOrDefault(size.name(), new LongAdder()).sum()
                            )
                        );
                    }
                    final StringBuilder puts = new StringBuilder();
                    for (Map.Entry<String, long[]> place : slow.putBytesByPlace().entrySet()) {
                        final long[] v = place.getValue();
                        puts.append(
                            String.format(
                                Locale.ROOT,
                                " %s: %d PUTs (%.2f/s) %s MB, mean %s KB, max %s MB;",
                                place.getKey(),
                                v[0],
                                v[0] / secondsRun,
                                mb(v[1]),
                                String.format(Locale.ROOT, "%.0f", v[0] == 0 ? 0 : v[1] / (double) v[0] / 1024),
                                mb(v[2])
                            )
                        );
                    }
                    final long logPuts = node.nodeLogWriter().committer().timings().puts();
                    final double ingested = sourceBytes.sum();
                    logger.info(
                        "BIGDOC|{} at {} docs/s, {} shards, {} s, {} MB/s per request{}{}|acked {} ({}/s, {} MB/s of source) failed {}"
                            + "|ack ms by size:{}|log PUTs {} ({}/s) store PUT {} GET {} LIST {} per doc PUT+LIST {}"
                            + "|bytes PUT by place:{}|publishes {}|cpu {} cores, {} ms per MB|allocated {} MB ({} bytes per source byte)"
                            + "|heap live-after-gc peak {} MB, used peak {} MB|translog on disk peak {} MB|steps {}|log {}",
                        mixName,
                        rate,
                        shards,
                        seconds,
                        bytesPerSecond / (1024 * 1024),
                        ascii ? ", ascii" : "",
                        extra.isBlank() ? "" : ", " + extra,
                        acked,
                        String.format(Locale.ROOT, "%.1f", acked / secondsRun),
                        mb(ingested / secondsRun),
                        failedBy.isEmpty() ? "0" : failedBy.toString(),
                        bySize,
                        logPuts,
                        String.format(Locale.ROOT, "%.2f", logPuts / secondsRun),
                        store.blobWrites() + store.registerWrites(),
                        store.blobReads() + store.registerReads(),
                        store.listings(),
                        String.format(
                            Locale.ROOT,
                            "%.3f",
                            (store.blobWrites() + store.registerWrites() + store.listings()) / (double) acked
                        ),
                        puts,
                        published,
                        String.format(Locale.ROOT, "%.2f", cpuSeconds / secondsRun),
                        String.format(Locale.ROOT, "%.0f", ingested == 0 ? 0 : cpuSeconds * 1e3 / (ingested / (1024 * 1024))),
                        mb(allocated),
                        String.format(Locale.ROOT, "%.1f", ingested == 0 ? 0 : allocated / ingested),
                        mb(peakLive[0]),
                        mb(peakUsed[0]),
                        mb(peakTranslog[0]),
                        node.writes().ackTimings().describe(),
                        node.nodeLogWriter().committer().timings().describe()
                    );
                } finally {
                    running.set(false);
                    clients.shutdownNow();
                    clients.awaitTermination(30, TimeUnit.SECONDS);
                }
            }
        }
    }

    public void testWhatLargeDocumentsDoToTheWritePath() throws Exception {
        final String secondsProperty = System.getProperty("tests.bigdoc.seconds", "");
        assumeTrue("the meter runs only when -Dtests.bigdoc.seconds is set", secondsProperty.isEmpty() == false);
        final long seconds = Long.parseLong(secondsProperty);
        final String[] mixes = System.getProperty("tests.bigdoc.mixes", "1k,64k,1m,4m,pages").split(",");
        final String[] rates = System.getProperty("tests.bigdoc.rates", "20").split(",");
        final int shards = Integer.parseInt(System.getProperty("tests.bigdoc.shards", "20"));
        final long bytesPerSecond = (long) (Double.parseDouble(System.getProperty("tests.bigdoc.mbps", "80")) * 1024 * 1024);
        final boolean ascii = Boolean.parseBoolean(System.getProperty("tests.bigdoc.ascii", "false"));
        final String extra = System.getProperty("tests.bigdoc.settings", "");
        logger.info(
            "BIGDOCJVM|{} {}|{}",
            System.getProperty("java.vm.name"),
            Runtime.version(),
            ManagementFactory.getRuntimeMXBean().getInputArguments()
        );
        for (int i = 0; i < mixes.length; i++) {
            final double rate = Double.parseDouble(rates[Math.min(i, rates.length - 1)].trim());
            measure(mixes[i].trim(), rate, shards, seconds, bytesPerSecond, ascii, extra);
        }
    }

    /**
     * The same documents through the REST API, where a single-document write runs on the node's {@code write} pool and holds
     * its thread through the engine and the log's window. Off unless {@code -Dtests.bigdoc.rest_seconds} is set;
     * {@code -Dtests.bigdoc.rest_mixes} and {@code -Dtests.bigdoc.rest_rates} as above. The store is the local filesystem with
     * no added latency, so what is left is the window.
     */
    public void testLargeDocumentsThroughRest() throws Exception {
        final String secondsProperty = System.getProperty("tests.bigdoc.rest_seconds", "");
        assumeTrue("the REST meter runs only when -Dtests.bigdoc.rest_seconds is set", secondsProperty.isEmpty() == false);
        final long seconds = Long.parseLong(secondsProperty);
        final String[] mixes = System.getProperty("tests.bigdoc.rest_mixes", "1k,pages").split(",");
        final String[] rates = System.getProperty("tests.bigdoc.rest_rates", "50").split(",");
        final int shards = Integer.parseInt(System.getProperty("tests.bigdoc.shards", "20"));
        final String extra = System.getProperty("tests.bigdoc.settings", "");
        for (int m = 0; m < mixes.length; m++) {
            final List<Size> sizes = mix(mixes[m].trim());
            final double rate = Double.parseDouble(rates[Math.min(m, rates.length - 1)].trim());
            final Settings.Builder settings = Settings.builder()
                .put(nodeSettings(createTempDir(), extra))
                .put("serverless.activation.max_shards", shards + 100)
                .put(org.opensearch.serverless.shell.ServerlessBootstrap.STORE_PATH, createTempDir().toString());
            final ExecutorService httpThreads = Executors.newFixedThreadPool(64);
            try (
                org.opensearch.serverless.shell.ServerlessBootstrap boot = org.opensearch.serverless.shell.ServerlessBootstrap.start(
                    settings.build()
                );
                java.net.http.HttpClient http = java.net.http.HttpClient.newBuilder().executor(httpThreads).build()
            ) {
                final int port = boot.node().boundHttpAddress().publishAddress().getPort();
                final String body = "{\"settings\":{\"number_of_shards\":1},\"mappings\":" + MAPPING + "}";
                for (int i = 0; i < shards; i++) {
                    final var created = http.send(
                        request(port, "PUT", "/" + name(i), body),
                        java.net.http.HttpResponse.BodyHandlers.ofString()
                    );
                    assertEquals(created.body(), 200, created.statusCode());
                }
                for (int i = 0; i < shards; i++) {
                    final var warm = http.send(
                        request(port, "PUT", "/" + name(i) + "/_doc/warm", "{\"title\":\"warm\",\"n\":0,\"body\":\"warm\"}"),
                        java.net.http.HttpResponse.BodyHandlers.ofString()
                    );
                    assertTrue(warm.body(), warm.statusCode() == 200 || warm.statusCode() == 201);
                }
                final Random random = new Random(random().nextLong());
                final Map<String, List<String>> pool = new TreeMap<>();
                for (Size size : sizes) {
                    final List<String> docs = new ArrayList<>();
                    for (int i = 0; i < (size.bytes() >= 1024 * 1024 ? 4 : 32); i++) {
                        docs.add(document(i, size.bytes(), false, random));
                    }
                    pool.put(size.name(), docs);
                }
                final Map<String, ConcurrentLinkedQueue<Long>> acks = new ConcurrentHashMap<>();
                final Map<String, LongAdder> statuses = new ConcurrentHashMap<>();
                final List<java.util.concurrent.CompletableFuture<?>> sent = new ArrayList<>();
                final long cpuBefore = processCpuNanos();
                final long started = System.nanoTime();
                final long end = started + TimeUnit.SECONDS.toNanos(seconds);
                long next = started;
                long n = 0;
                while (next < end) {
                    final long wait = next - System.nanoTime();
                    if (wait > 0) {
                        TimeUnit.NANOSECONDS.sleep(wait);
                    }
                    final Size size = pick(sizes, random);
                    final List<String> docs = pool.get(size.name());
                    final String source = docs.get(random.nextInt(docs.size()));
                    final String path = "/" + name(random.nextInt(shards)) + "/_doc/r" + (n++);
                    final long at = System.nanoTime();
                    sent.add(
                        http.sendAsync(request(port, "PUT", path, source), java.net.http.HttpResponse.BodyHandlers.ofString())
                            .whenComplete((r, e) -> {
                                final Throwable cause = e != null && e.getCause() != null ? e.getCause() : e;
                                final String status = e != null
                                    ? cause.getClass().getSimpleName() + ": " + String.valueOf(cause.getMessage()).replaceAll("[0-9]+", "N")
                                    : String.valueOf(r.statusCode());
                                statuses.computeIfAbsent(size.name() + " " + status, k -> new LongAdder()).increment();
                                if (e == null && (r.statusCode() == 200 || r.statusCode() == 201)) {
                                    acks.computeIfAbsent(size.name(), k -> new ConcurrentLinkedQueue<>()).add(System.nanoTime() - at);
                                }
                            })
                    );
                    next += (long) (-Math.log(1 - random.nextDouble()) / rate * 1e9);
                }
                for (java.util.concurrent.CompletableFuture<?> each : sent) {
                    try {
                        each.get(120, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        // Counted by its status.
                    }
                }
                final double secondsRun = (System.nanoTime() - started) / 1e9;
                final double cpu = (processCpuNanos() - cpuBefore) / 1e9 / secondsRun;
                final StringBuilder bySize = new StringBuilder();
                long acked = 0;
                for (Size size : sizes) {
                    final List<Long> sorted = new ArrayList<>(acks.getOrDefault(size.name(), new ConcurrentLinkedQueue<>()));
                    Collections.sort(sorted);
                    acked += sorted.size();
                    bySize.append(
                        String.format(
                            Locale.ROOT,
                            " %s: n=%d p50=%.0f p99=%.0f max=%.0f;",
                            size.name(),
                            sorted.size(),
                            percentile(sorted, 0.5),
                            percentile(sorted, 0.99),
                            percentile(sorted, 1.0)
                        )
                    );
                }
                logger.info(
                    "BIGDOCREST|{} at {} docs/s, {} shards, {} s, write pool {} threads{}|acked {} ({}/s)|ack ms by size:{}|statuses {}|cpu {} cores",
                    mixes[m].trim(),
                    rate,
                    shards,
                    seconds,
                    boot.node().threadPool().info(org.opensearch.threadpool.ThreadPool.Names.WRITE).getMax(),
                    extra.isBlank() ? "" : ", " + extra,
                    acked,
                    String.format(Locale.ROOT, "%.1f", acked / secondsRun),
                    bySize,
                    new TreeMap<>(statuses),
                    String.format(Locale.ROOT, "%.2f", cpu)
                );
            } finally {
                httpThreads.shutdownNow();
                httpThreads.awaitTermination(10, TimeUnit.SECONDS);
            }
        }
    }

    private static java.net.http.HttpRequest request(int port, String method, String path, String body) {
        return java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:" + port + path))
            .header("Content-Type", "application/json")
            .timeout(java.time.Duration.ofSeconds(120))
            .method(method, java.net.http.HttpRequest.BodyPublishers.ofString(body))
            .build();
    }
}
