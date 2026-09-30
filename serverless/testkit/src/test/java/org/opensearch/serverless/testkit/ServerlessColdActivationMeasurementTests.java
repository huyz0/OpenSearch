/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.reconcile.ReconcileScheduler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * What the first write and the first search to a dormant shard cost, on a real S3 API with latency injected.
 *
 * <p>A dormant shard is one with published data and nobody holding it: its writer went idle and let the head
 * go. The first write to it has to take the head, fence the log and open the shard before it can append; the
 * first search has to open a reader from the published commit. Both are serial chains of object-store round
 * trips, so what a caller waits is roughly the chain's length times the store's latency -- which is why the
 * latency is injected: a local S3 answers in a millisecond and would hide the chain.
 *
 * <p>Reported per delay: the request sequence of one sample, the request count, and p50/p99 over the samples.
 * A write is timed from the first attempt to the first acknowledgement, retrying on 421 and 503 as a client
 * would. Samples from {@code tests.serverless.cold.samples} (default 12), delays from
 * {@code tests.serverless.cold.delays} (default {@code 20,100}). Skipped without an endpoint.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ServerlessColdActivationMeasurementTests extends OpenSearchTestCase {

    private static final long TTL = 600_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private final List<String> created = new ArrayList<>();
    private int port;

    private String endpoint() {
        return System.getProperty(MinioBlobContainerConformanceTests.ENDPOINT, "http://127.0.0.1:9000");
    }

    private static String accessKey() {
        return System.getProperty("tests.serverless.s3.access_key", "minioadmin");
    }

    private static String secretKey() {
        return System.getProperty("tests.serverless.s3.secret_key", "minioadmin");
    }

    public void testWhatTheFirstWriteAndFirstSearchToADormantShardCost() throws Exception {
        assumeTrue("no S3-compatible endpoint at " + endpoint(), reachable(endpoint()));
        final int samples = Integer.parseInt(System.getProperty("tests.serverless.cold.samples", "12"));
        final String[] delays = System.getProperty("tests.serverless.cold.delays", "20,100").split(",");

        final String bucket = "cold-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        final BlobStore s3 = org.opensearch.repositories.s3.MinioBlobStores.create(
            endpoint(),
            accessKey(),
            secretKey(),
            bucket,
            createTempDir()
        );
        created.add(bucket);
        final LatencyInjectingBlobStore store = new LatencyInjectingBlobStore(s3, 0);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), System::currentTimeMillis, TTL);

        // Dormant indices: written, published and let go by a node that then goes away.
        final int indices = delays.length * samples * 2;
        // Ingest only, so that once it is gone no search is placed on it: its lease outlives it.
        try (
            ServerlessNode seeder = new ServerlessNode(
                Settings.builder().put(nodeSettings("seeder")).put("serverless.roles", "ingest").build()
            )
        ) {
            seeder.start();
            seeder.setMetadataPlane(plane);
            port = seeder.boundHttpAddress().publishAddress().getPort();
            final BackgroundReconciler loop = new BackgroundReconciler(seeder, plane).setDemandDrivenActivation(true);
            for (int i = 0; i < indices; i++) {
                plane.createIndex(new IndexDescriptor(name(i), "uuid-" + i, 1, MAPPING, null));
                loop.want(name(i), 0);
            }
            loop.activateWanted();
            for (int i = 0; i < indices; i++) {
                for (int d = 0; d < 5; d++) {
                    final Answer put = call("PUT", "/" + name(i) + "/_doc/seed-" + d, "{\"msg\":\"hello " + d + "\",\"n\":" + d + "}");
                    assertEquals(put.body(), 201, put.status());
                }
            }
            loop.publishAll();
            loop.setIdleAfterMillis(1);
            loop.releaseIdle(System.currentTimeMillis() + 60_000L);
            assertTrue("every seeded shard let go", seeder.reconciler().openShards().isEmpty());
        }

        final Map<String, String> report = new TreeMap<>();
        int next = 0;
        for (String delay : delays) {
            final long delayMillis = Long.parseLong(delay.trim());
            try (ServerlessNode node = new ServerlessNode(nodeSettings("cold-" + delayMillis))) {
                node.start();
                node.setMetadataPlane(plane);
                port = node.boundHttpAddress().publishAddress().getPort();
                final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true);
                try (
                    ReconcileScheduler scheduler = new ReconcileScheduler(
                        loop,
                        node.threadPool(),
                        plane.clock(),
                        TimeValue.timeValueHours(1),
                        TimeValue.timeValueMillis(50),
                        null
                    )
                ) {
                    node.setSignals(scheduler);
                    scheduler.start();
                    // Warm the node itself -- its lease, its first descriptor reads -- on an index nobody measures.
                    call("GET", "/_cluster/health", null);
                    store.setDelayMillis(delayMillis);

                    final List<Long> writeMillis = new ArrayList<>();
                    final List<Integer> writeRequests = new ArrayList<>();
                    final List<Integer> writeAttempts = new ArrayList<>();
                    List<LatencyInjectingBlobStore.Request> writeSequence = null;
                    Map<String, Long> writeS3 = null;
                    for (int s = 0; s < samples; s++) {
                        final String index = name(next++);
                        store.drain();
                        final Map<String, Long> before = org.opensearch.repositories.s3.MinioBlobStores.requestCounts(s3);
                        final long startedAt = System.nanoTime();
                        int attempts = 0;
                        Answer put;
                        do {
                            if (attempts++ > 0) {
                                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
                            }
                            put = call("PUT", "/" + index + "/_doc/first", "{\"msg\":\"first\",\"n\":100}");
                            assertTrue("gave up on " + index + ": " + put.body(), attempts < 2_000);
                        } while (put.status() == 421 || put.status() == 503);
                        assertEquals(put.body(), 201, put.status());
                        writeMillis.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
                        writeAttempts.add(attempts);
                        final List<LatencyInjectingBlobStore.Request> sequence = store.drain();
                        writeRequests.add(sequence.size());
                        if (s == 0) {
                            writeSequence = sequence;
                            writeS3 = delta(before, org.opensearch.repositories.s3.MinioBlobStores.requestCounts(s3));
                        }
                    }

                    // The writes' own publishes, done now rather than inside the searches' window.
                    loop.publishAll();
                    store.drain();

                    final List<Long> searchMillis = new ArrayList<>();
                    final List<Integer> searchRequests = new ArrayList<>();
                    List<LatencyInjectingBlobStore.Request> searchSequence = null;
                    Map<String, Long> searchS3 = null;
                    for (int s = 0; s < samples; s++) {
                        final String index = name(next++);
                        store.drain();
                        final Map<String, Long> before = org.opensearch.repositories.s3.MinioBlobStores.requestCounts(s3);
                        final long startedAt = System.nanoTime();
                        final Answer search = call("GET", "/" + index + "/_search?q=msg:hello", null);
                        searchMillis.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
                        assertEquals(search.body(), 200, search.status());
                        assertTrue("the published documents are found: " + search.body(), search.body().contains("\"value\":5"));
                        final List<LatencyInjectingBlobStore.Request> sequence = store.drain();
                        searchRequests.add(sequence.size());
                        if (s == 0) {
                            searchSequence = sequence;
                            searchS3 = delta(before, org.opensearch.repositories.s3.MinioBlobStores.requestCounts(s3));
                        }
                    }
                    store.setDelayMillis(0);

                    logger.info(
                        "cold[{}ms]: first write p50 {}ms p99 {}ms, {} store requests (p50), attempts {}; s3 {}; sequence:\n  {}",
                        delayMillis,
                        percentile(writeMillis, 50),
                        percentile(writeMillis, 99),
                        percentile(writeRequests, 50),
                        writeAttempts,
                        writeS3,
                        String.join("\n  ", render(writeSequence))
                    );
                    logger.info(
                        "cold[{}ms]: first search p50 {}ms p99 {}ms, {} store requests (p50); s3 {}; sequence:\n  {}",
                        delayMillis,
                        percentile(searchMillis, 50),
                        percentile(searchMillis, 99),
                        percentile(searchRequests, 50),
                        searchS3,
                        String.join("\n  ", render(searchSequence))
                    );
                    report.put(
                        delayMillis + "ms",
                        "write p50 "
                            + percentile(writeMillis, 50)
                            + "ms p99 "
                            + percentile(writeMillis, 99)
                            + "ms "
                            + percentile(writeRequests, 50)
                            + " requests; search p50 "
                            + percentile(searchMillis, 50)
                            + "ms p99 "
                            + percentile(searchMillis, 99)
                            + "ms "
                            + percentile(searchRequests, 50)
                            + " requests"
                    );
                }
            }
        }
        logger.info("cold activation summary: {}", report);
    }

    private static List<String> render(List<LatencyInjectingBlobStore.Request> sequence) {
        final List<String> lines = new ArrayList<>();
        for (LatencyInjectingBlobStore.Request r : sequence) {
            lines.add(r.op() + " " + r.key() + "  [" + r.thread().replaceAll(".*\\[", "").replace("]", "") + "]");
        }
        return lines;
    }

    private static <T extends Comparable<T>> T percentile(List<T> values, int p) {
        final List<T> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        return sorted.get(Math.min(sorted.size() - 1, (int) Math.ceil(p / 100.0 * sorted.size()) - 1));
    }

    private static String name(int i) {
        return String.format(Locale.ROOT, "dormant-%03d", i);
    }

    private static Map<String, Long> delta(Map<String, Long> before, Map<String, Long> after) {
        final Map<String, Long> delta = new HashMap<>();
        for (Map.Entry<String, Long> entry : after.entrySet()) {
            final long d = entry.getValue() - before.getOrDefault(entry.getKey(), 0L);
            if (d != 0L) {
                delta.put(entry.getKey(), d);
            }
        }
        return delta;
    }

    private record Answer(int status, String body) {
    }

    private Answer call(String method, String path, String body) throws Exception {
        try (HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofMinutes(2))
                .header("Content-Type", "application/json");
            request.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
            final HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
            return new Answer(response.statusCode(), response.body());
        }
    }

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-cold")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .build();
    }

    @org.junit.After
    public void removeBucketsThisTestMade() throws Exception {
        for (String bucket : created) {
            org.opensearch.repositories.s3.MinioBlobStores.deleteBucket(endpoint(), accessKey(), secretKey(), bucket, createTempDir());
        }
        created.clear();
    }

    private static boolean reachable(String endpoint) {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpoint + "/minio/health/live"))
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();
            return client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() < 500;
        } catch (Exception e) {
            return false;
        }
    }
}
