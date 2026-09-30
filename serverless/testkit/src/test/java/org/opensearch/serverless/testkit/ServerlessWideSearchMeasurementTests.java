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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A {@code logs-*} search over thousands of time-partitioned indices on a real S3 API.
 *
 * <p>The deployment: one index per hour, each with four documents inside its hour, published and let go -- every
 * shard dormant, as an older day's shards are. The search asks for a window of hours a twentieth as wide as the
 * deployment. What it should cost: one listing per thousand names to resolve the pattern, a head and a manifest
 * read per shard to prune it, and an activation only for the shards in the window. What it must return: exactly
 * the documents in the window -- a count the test knows, so a digest that pruned one shard too many is a wrong
 * total, not a silent miss.
 *
 * <p>Population from {@code tests.serverless.wide.indices} (default {@code 1000}; {@code STATUS.md} reports
 * {@code 5000}), injected per-request latency from {@code tests.serverless.wide.delay} (default 0). Skipped
 * without an endpoint.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ServerlessWideSearchMeasurementTests extends OpenSearchTestCase {

    private static final long TTL = 600_000L;
    private static final String MAPPING = "{\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"n\":{\"type\":\"long\"},"
        + "\"msg\":{\"type\":\"text\"}}}";
    private static final long START = Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();
    private static final long HOUR = 3_600_000L;
    private static final int BATCH = 250;
    private static final Pattern TOTAL = Pattern.compile("\"total\":\\{\"value\":(\\d+)");
    private static final Pattern SKIPPED = Pattern.compile("\"skipped\":(\\d+)");
    private static final Pattern FAILED = Pattern.compile("\"failed\":(\\d+)");

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

    private static String name(int i) {
        return String.format(Locale.ROOT, "logs-%05d", i);
    }

    public void testAWideSearchPrunesWhatItCannotMatchAndFindsEverythingItCan() throws Exception {
        assumeTrue("no S3-compatible endpoint at " + endpoint(), reachable(endpoint()));
        final int population = Integer.parseInt(System.getProperty("tests.serverless.wide.indices", "1000"));
        final long delay = Long.parseLong(System.getProperty("tests.serverless.wide.delay", "0"));
        final String bucket = "wide-" + population + "-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
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

        final long seedingSince = System.nanoTime();
        try (ServerlessNode seeder = new ServerlessNode(nodeSettings("wide-seeder", "ingest"))) {
            seeder.start();
            seeder.setMetadataPlane(plane);
            port = seeder.boundHttpAddress().publishAddress().getPort();
            final BackgroundReconciler loop = new BackgroundReconciler(seeder, plane).setDemandDrivenActivation(true);
            loop.setIdleAfterMillis(1);
            final ExecutorService creators = Executors.newFixedThreadPool(32);
            try {
                for (int from = 0; from < population; from += BATCH) {
                    final int end = Math.min(population, from + BATCH);
                    final List<Future<?>> pending = new ArrayList<>();
                    for (int i = from; i < end; i++) {
                        final int index = i;
                        pending.add(creators.submit(() -> {
                            plane.createIndex(new IndexDescriptor(name(index), "uuid-" + index, 1, MAPPING, null));
                            return null;
                        }));
                    }
                    for (Future<?> f : pending) {
                        f.get();
                    }
                    for (int i = from; i < end; i++) {
                        loop.want(name(i), 0);
                    }
                    loop.activateWanted();
                    final StringBuilder bulk = new StringBuilder();
                    for (int i = from; i < end; i++) {
                        for (int q = 0; q < 4; q++) {
                            bulk.append("{\"index\":{\"_index\":\"").append(name(i)).append("\",\"_id\":\"").append(q).append("\"}}\n");
                            bulk.append("{\"@timestamp\":")
                                .append(START + i * HOUR + q * 15 * 60_000L)
                                .append(",\"n\":")
                                .append(i)
                                .append(",\"msg\":\"line\"}\n");
                        }
                    }
                    final Answer written = call("POST", "/_bulk", bulk.toString());
                    assertEquals(written.body(), 200, written.status());
                    assertFalse(
                        "every document written: " + written.body().substring(0, Math.min(500, written.body().length())),
                        written.body().contains("\"errors\":true")
                    );
                    loop.publishAll();
                    // Demand-driven, so letting go also stops wanting: the next batch's pass does not retake these.
                    loop.releaseIdle(System.currentTimeMillis() + 60_000L);
                    assertTrue("the batch let go", seeder.reconciler().openShards().isEmpty());
                }
            } finally {
                creators.shutdown();
                assertTrue(creators.awaitTermination(1, TimeUnit.MINUTES));
            }
        }
        final long seedingSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - seedingSince);

        // The window: a twentieth of the hours, somewhere in the middle.
        final int width = Math.max(1, population / 20);
        final int first = population / 3;
        final String query = "{\"size\":10,\"track_total_hits\":true,\"query\":{\"bool\":{\"filter\":[{\"range\":{\"@timestamp\":{\"gte\":"
            + (START + first * HOUR)
            + ",\"lt\":"
            + (START + (first + width) * HOUR)
            + "}}}]}},\"aggs\":{\"hours\":{\"cardinality\":{\"field\":\"n\"}}}}";

        final Map<String, String> report = new TreeMap<>();
        try (ServerlessNode node = new ServerlessNode(nodeSettings("wide-search", null))) {
            node.start();
            node.setMetadataPlane(plane);
            port = node.boundHttpAddress().publishAddress().getPort();
            call("GET", "/_cluster/health", null);
            store.setDelayMillis(delay);
            // What resolving the pattern alone costs: its listing pages are one after another by nature.
            final long listingSince = System.nanoTime();
            assertEquals(population, plane.namesWithPrefix("logs-", 10_000).size());
            report.put("listing", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - listingSince) + "ms to list the pattern's names");
            for (String pass : new String[] { "cold", "warm", "warm2", "warm3" }) {
                store.drain();
                final Map<String, Long> before = org.opensearch.repositories.s3.MinioBlobStores.requestCounts(s3);
                final long startedAt = System.nanoTime();
                final Answer answer = call("POST", "/logs-*/_search", query);
                final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
                final Map<String, Long> s3Cost = delta(before, org.opensearch.repositories.s3.MinioBlobStores.requestCounts(s3));
                final List<LatencyInjectingBlobStore.Request> sequence = store.drain();
                assertEquals(answer.body().substring(0, Math.min(2000, answer.body().length())), 200, answer.status());
                assertEquals("exactly the documents in the window", 4L * width, first(TOTAL, answer.body()));
                assertEquals("no shard failed", 0L, first(FAILED, answer.body()));
                final long skipped = first(SKIPPED, answer.body());
                assertTrue("at least nine in ten shards pruned: " + skipped + " of " + population, skipped >= population * 9L / 10);
                final Map<String, Integer> byOp = new TreeMap<>();
                for (LatencyInjectingBlobStore.Request r : sequence) {
                    byOp.merge(r.op() + " " + r.key().replaceAll("/.*", "/"), 1, Integer::sum);
                }
                report.put(
                    pass,
                    millis
                        + "ms, "
                        + skipped
                        + " of "
                        + population
                        + " shards pruned ("
                        + (100 * skipped / population)
                        + "%), "
                        + sequence.size()
                        + " store requests "
                        + byOp
                        + ", s3 "
                        + s3Cost
                );
            }
            store.setDelayMillis(0);
        }
        logger.info(
            "wide search over {} hourly indices, window {} hours, delay {}ms, seeding {}s: {}",
            population,
            width,
            delay,
            seedingSeconds,
            report
        );
    }

    private static long first(Pattern pattern, String body) {
        final Matcher m = pattern.matcher(body);
        assertTrue("no " + pattern + " in " + body.substring(0, Math.min(2000, body.length())), m.find());
        return Long.parseLong(m.group(1));
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
                .timeout(Duration.ofMinutes(10))
                .header("Content-Type", "application/json");
            request.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
            final HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
            return new Answer(response.statusCode(), response.body());
        }
    }

    private Settings nodeSettings(String name, String roles) {
        final Settings.Builder b = Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-wide")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0");
        if (roles != null) {
            b.put("serverless.roles", roles);
        }
        return b.build();
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
