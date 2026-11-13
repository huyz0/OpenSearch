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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * What batches of about 100 MB, several at once, do to a writer's heap: peak heap, live heap after collections, how many
 * were admitted or refused, and how large the node-log blobs carrying them grew. A measurement, not part of check:
 *
 * <pre>
 *   ./gradlew :serverless:testkit:bigBulkTest [-Dtests.bigbulk.mb=95] [-Dtests.bigbulk.concurrent=6]
 *       [-Dtests.bigbulk.rounds=3] [-Dtests.bigbulk.shards=1] [-Dtests.bigbulk.heap=4g] [-Dtests.bigbulk.settings=k=v,...]
 * </pre>
 *
 * <p>Every batch is one shared body, read as the socket takes it, so the client holds one copy whatever the concurrency;
 * the baseline heap is taken with that body already built.
 */
@com.carrotsearch.randomizedtesting.annotations.TimeoutSuite(millis = 7_200_000)
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*")
public class BigBulkMeterTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING =
        "{\"properties\":{\"title\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"},\"body\":{\"type\":\"text\"}}}";
    private static final String[] WORDS = {
        "the",
        "page",
        "release",
        "notes",
        "meeting",
        "design",
        "service",
        "customer",
        "request",
        "team",
        "update",
        "review",
        "deploy",
        "project",
        "document",
        "search" };

    private Settings nodeSettings(Path home, String extra) {
        final Settings.Builder builder = Settings.builder()
            .put("node.name", "bigbulk-writer")
            .put("cluster.name", "serverless-bigbulk")
            .put("path.home", home)
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

    private static String index(int i) {
        return String.format(Locale.ROOT, "bulk-%03d", i);
    }

    /** A body of about this many bytes: 100 KB pages of prose, spread round-robin over the indices. */
    private static byte[] body(long bytes, int shards) {
        final StringBuilder page = new StringBuilder(100 * 1024 + 64);
        int w = 0;
        while (page.length() < 100 * 1024) {
            page.append(WORDS[w++ % WORDS.length]).append(w % 7 == 0 ? ". " : " ");
        }
        final StringBuilder out = new StringBuilder((int) bytes + 1024);
        int n = 0;
        while (out.length() < bytes - page.length() - 256) {
            out.append("{\"index\":{\"_index\":\"").append(index(n % shards)).append("\",\"_id\":\"d").append(n).append("\"}}\n");
            out.append("{\"title\":\"page ").append(n).append("\",\"n\":").append(n).append(",\"body\":\"").append(page).append("\"}\n");
            n++;
        }
        return out.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static long liveAfterLastGc() {
        long total = 0;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP && pool.getCollectionUsage() != null) {
                total += pool.getCollectionUsage().getUsed();
            }
        }
        return total;
    }

    private static long used() {
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }

    private static List<Long> blobSizes(Path root) throws java.io.IOException {
        final List<Long> sizes = new ArrayList<>();
        try (var files = Files.walk(root)) {
            for (Path file : (Iterable<Path>) files::iterator) {
                if (Files.isRegularFile(file)
                    && file.toString().contains("nodelog")
                    && file.getFileName().toString().startsWith("extra") == false) {
                    sizes.add(Files.size(file));
                }
            }
        }
        return sizes;
    }

    @org.opensearch.common.SuppressForbidden(reason = "a meter asks for collections between runs, so each starts from the same heap")
    private static void settle() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            System.gc();
            Thread.sleep(200);
        }
    }

    public void testBigBatchesAtOnce() throws Exception {
        final long mb = Long.parseLong(System.getProperty("tests.bigbulk.mb", "95"));
        final int concurrent = Integer.parseInt(System.getProperty("tests.bigbulk.concurrent", "6"));
        final int rounds = Integer.parseInt(System.getProperty("tests.bigbulk.rounds", "3"));
        final int shards = Integer.parseInt(System.getProperty("tests.bigbulk.shards", "1"));
        final String extra = System.getProperty("tests.bigbulk.settings", "");

        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final Path root = createTempDir();
        final LatencyInjectingBlobStore slow = new LatencyInjectingBlobStore(new FsBlobStore(8192, root, false), 0);
        final MetadataPlane plane = new MetadataPlane(slow, BlobPath.cleanPath(), clock::get, TTL);
        for (int i = 0; i < shards; i++) {
            plane.createIndex(new IndexDescriptor(index(i), "uuid-" + index(i), 1, MAPPING, null));
        }
        try (ServerlessNode node = new ServerlessNode(nodeSettings(createTempDir(), extra))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            node.leases().renewLease(plane);
            plane.membership().refresh();
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setMaxShardsHeld(shards * 2);
            for (int i = 0; i < shards; i++) {
                loop.want(index(i), 0);
            }
            for (int pass = 0; pass < 50 && node.reconciler().openShards().size() < shards; pass++) {
                loop.tick(clock.get());
            }
            assertEquals(shards, node.reconciler().openShards().size());

            final byte[] body = body(mb * 1024 * 1024, shards);
            slow.setDelayMillis(20);
            slow.setTail(50, 200);
            // A store that takes 100 MB/s per PUT, so a blob's size shows in its PUT's time.
            slow.setBytesPerSecond(100L * 1024 * 1024);
            settle();
            final long baseline = liveAfterLastGc();
            final long heapMax = Runtime.getRuntime().maxMemory();

            final AtomicBoolean running = new AtomicBoolean(true);
            final long[] peakUsed = { 0 };
            final long[] peakLive = { 0 };
            final Thread sampler = new Thread(() -> {
                while (running.get()) {
                    peakUsed[0] = Math.max(peakUsed[0], used());
                    peakLive[0] = Math.max(peakLive[0], liveAfterLastGc());
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }, "bigbulk-sampler");
            sampler.start();

            final Map<Integer, LongAdder> statuses = new ConcurrentHashMap<>();
            final LongAdder itemErrors = new LongAdder();
            final List<Long> latencies = java.util.Collections.synchronizedList(new ArrayList<>());
            final var address = node.boundHttpAddress().publishAddress();
            final URI uri = URI.create("http://" + address.getAddress() + ":" + address.getPort() + "/_bulk");
            final long started = System.nanoTime();
            for (int round = 0; round < rounds; round++) {
                final List<Thread> clients = new ArrayList<>();
                for (int c = 0; c < concurrent; c++) {
                    final Thread client = new Thread(() -> {
                        try (
                            HttpClient http = HttpClient.newBuilder()
                                .version(HttpClient.Version.HTTP_1_1)
                                .connectTimeout(Duration.ofSeconds(10))
                                .build()
                        ) {
                            final long sent = System.nanoTime();
                            final HttpResponse<String> response = http.send(
                                HttpRequest.newBuilder(uri)
                                    .timeout(Duration.ofMinutes(10))
                                    .header("Content-Type", "application/x-ndjson")
                                    .POST(
                                        // Read from the shared body as the socket takes it: ofByteArray copies the whole
                                        // body up front, 95 MB per client in this JVM, which swamped the node's heap.
                                        HttpRequest.BodyPublishers.fromPublisher(
                                            HttpRequest.BodyPublishers.ofInputStream(() -> new java.io.ByteArrayInputStream(body)),
                                            body.length
                                        )
                                    )
                                    .build(),
                                HttpResponse.BodyHandlers.ofString()
                            );
                            latencies.add((System.nanoTime() - sent) / 1_000_000);
                            statuses.computeIfAbsent(response.statusCode(), k -> new LongAdder()).increment();
                            if (response.statusCode() != 200) {
                                logger.info(
                                    "BIGBULK-REFUSED|{}|{}",
                                    response.statusCode(),
                                    response.body().substring(0, Math.min(300, response.body().length()))
                                );
                            }
                            if (response.statusCode() == 200 && response.body().contains("\"errors\":true")) {
                                itemErrors.increment();
                                final String all = response.body();
                                final int at = all.indexOf("\"error\":{");
                                final java.util.regex.Matcher failed = java.util.regex.Pattern.compile("\"status\":(\\d+)").matcher(all);
                                final Map<String, Integer> byStatus = new TreeMap<>();
                                while (failed.find()) {
                                    byStatus.merge(failed.group(1), 1, Integer::sum);
                                }
                                logger.info(
                                    "BIGBULK-ITEMS|{}|{}",
                                    byStatus,
                                    at < 0 ? "" : all.substring(at, Math.min(all.length(), at + 300))
                                );
                            }
                        } catch (Exception e) {
                            statuses.computeIfAbsent(-1, k -> new LongAdder()).increment();
                            logger.warn("bigbulk request failed", e);
                        }
                    }, "bigbulk-client-" + c);
                    client.start();
                    clients.add(client);
                }
                for (Thread client : clients) {
                    client.join();
                }
            }
            final long tookMillis = (System.nanoTime() - started) / 1_000_000;
            running.set(false);
            sampler.join();

            final List<Long> blobs = blobSizes(root);
            final long maxBlob = blobs.stream().mapToLong(Long::longValue).max().orElse(0);
            final double meanBlob = blobs.stream().mapToLong(Long::longValue).average().orElse(0);
            final Map<Integer, Long> byStatus = new TreeMap<>();
            statuses.forEach((k, v) -> byStatus.put(k, v.sum()));
            latencies.sort(Long::compare);
            final var committer = node.nodeLogWriter().committer();
            logger.info(
                "BIGBULK|{} MB x {} at once x {} rounds over {} shards [{}]|statuses {} ({} with item errors)|latency min {} median {} max {} ms, "
                    + "{} s in all|heap max {} MB, baseline live {} MB, peak used {} MB, peak live after GC {} MB (+{} MB over baseline)"
                    + "|node-log blobs {} (mean {} MB, max {} MB), hedges {} (won {})",
                mb,
                concurrent,
                rounds,
                shards,
                extra,
                byStatus,
                itemErrors.sum(),
                latencies.isEmpty() ? 0 : latencies.get(0),
                latencies.isEmpty() ? 0 : latencies.get(latencies.size() / 2),
                latencies.isEmpty() ? 0 : latencies.get(latencies.size() - 1),
                tookMillis / 1000,
                heapMax >> 20,
                baseline >> 20,
                peakUsed[0] >> 20,
                peakLive[0] >> 20,
                (peakLive[0] - baseline) >> 20,
                blobs.size(),
                String.format(Locale.ROOT, "%.1f", meanBlob / (1024 * 1024)),
                String.format(Locale.ROOT, "%.1f", maxBlob / (1024.0 * 1024)),
                committer.timings().hedges(),
                committer.timings().hedgesWon()
            );
        }
    }
}
