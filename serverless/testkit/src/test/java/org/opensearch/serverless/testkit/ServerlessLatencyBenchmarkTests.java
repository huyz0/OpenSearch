/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Explicit open-loop HTTP baseline. The default forks one real node over a filesystem object store;
 * tests.latency.url selects an existing deployment instead. Neither mode simulates a three-AZ quorum.
 * The output includes scheduling/queue delay, admission losses, failures, and post-ACK search visibility.
 */
public class ServerlessLatencyBenchmarkTests extends OpenSearchTestCase {

    public void testOpenLoopBaseline() throws Exception {
        final int rate = setting("rate", 50, 1, 100_000);
        final int seconds = setting("seconds", 10, 1, 600);
        final int warmup = setting("warmup_seconds", 2, 0, 60);
        final int workers = setting("workers", 8, 1, 128);
        final int queue = setting("queue", 64, 1, 10_000);
        final int shards = setting("shards", 1, 1, 128);
        final int indices = setting("indices", 1, 1, 100);
        final int sourceBytes = setting("source_bytes", 1024, 128, 1_048_576);
        final int searchEvery = setting("search_every", 1, 0, 1_000_000);
        final boolean refresh = Boolean.parseBoolean(System.getProperty("tests.latency.refresh", "true"));
        final long requests = (long) rate * (seconds + warmup);
        assertTrue("at most one million offered requests per run", requests <= 1_000_000);
        final String external = System.getProperty("tests.latency.url");
        final NodeProcess local = external == null
            ? NodeProcess.start("latency-baseline", createTempDir(), createTempDir(), Map.of(), List.of("-Xms256m", "-Xmx512m"))
            : null;
        try (local; HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final ThreadContext context = new ThreadContext(Settings.EMPTY);
            final String url = external == null ? "http://" + local.http() : external.replaceAll("/+$", "");
            assertNull("credentials must not be embedded in the benchmark URL", URI.create(url).getUserInfo());
            final String prefix = "latency-" + randomAlphaOfLength(12).toLowerCase(java.util.Locale.ROOT);
            for (int index = 0; index < indices; index++) {
                final var response = send(
                    client,
                    url,
                    "PUT",
                    "/" + prefix + "-" + index,
                    "{\"settings\":{\"number_of_shards\":"
                        + shards
                        + "},\"mappings\":{\"properties\":{"
                        + "\"n\":{\"type\":\"long\"},\"msg\":{\"type\":\"text\"}}}}"
                );
                assertEquals(response.body(), 200, response.statusCode());
            }
            final Map<String, Object> before = json(send(client, url, "GET", "/_serverless/stats", null));
            final Samples samples = new Samples(rate * seconds);
            final long started = System.nanoTime();
            final long period = TimeUnit.SECONDS.toNanos(1) / rate;
            final AtomicInteger sequence = new AtomicInteger();
            final var offeringDone = new CountDownLatch(1);
            final var scheduler = Executors.newSingleThreadScheduledExecutor();
            final var executor = OpenSearchExecutors.newFixed(
                "latency-generator",
                workers,
                queue,
                OpenSearchExecutors.daemonThreadFactory("latency-generator"),
                context
            );
            final String padding = "x".repeat(sourceBytes - 64);
            try {
                scheduler.scheduleAtFixedRate(() -> {
                    final int id = sequence.getAndIncrement();
                    if (id >= requests) {
                        offeringDone.countDown();
                        return;
                    }
                    final int sample = id - rate * warmup;
                    try {
                        executor.execute(
                            () -> measure(
                                client,
                                url,
                                prefix + "-" + (id % indices),
                                id,
                                sample,
                                started + id * period,
                                refresh,
                                searchEvery,
                                padding,
                                samples
                            )
                        );
                    } catch (java.util.concurrent.RejectedExecutionException e) {
                        if (sample >= 0) {
                            samples.rejected.incrementAndGet();
                        }
                    }
                }, 0, period, TimeUnit.NANOSECONDS);
                assertTrue("load generator stalled", offeringDone.await(seconds + warmup + 60L, TimeUnit.SECONDS));
            } finally {
                scheduler.shutdownNow();
                assertTrue(scheduler.awaitTermination(30, TimeUnit.SECONDS));
                executor.shutdown();
                if (executor.awaitTermination(120, TimeUnit.SECONDS) == false) {
                    executor.shutdownNow();
                    assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
                    fail("benchmark workers did not drain");
                }
            }
            final Map<String, Object> after = json(send(client, url, "GET", "/_serverless/stats", null));
            try (XContentBuilder report = org.opensearch.common.xcontent.json.JsonXContent.contentBuilder().prettyPrint()) {
                report.startObject();
                report.field("storage", external == null ? "filesystem_object_store" : "external_deployment");
                report.field("java_version", System.getProperty("java.version"));
                report.field("os", System.getProperty("os.name"));
                report.field("architecture", System.getProperty("os.arch"));
                report.field("available_processors", Runtime.getRuntime().availableProcessors());
                report.field("index_prefix", prefix);
                report.field("offered_rate_per_second", rate);
                report.field("measurement_seconds", seconds);
                report.field("warmup_seconds", warmup);
                report.field("workers", workers);
                report.field("queue_capacity", queue);
                report.field("indices", indices);
                report.field("shards_per_index", shards);
                report.field("nominal_source_bytes", sourceBytes);
                report.field("refresh", refresh);
                report.field("search_every", searchEvery);
                report.field("offered", samples.ack.length);
                report.field("admission_rejected", samples.rejected.get());
                report.field("write_failures", samples.failures.get());
                report.field("freshness_failures", samples.freshnessFailures.get());
                report.field("search_attempts", samples.searchAttempts.get());
                report.field("ack_from_scheduled_arrival", distribution(samples.ack));
                report.field("ack_service_time", distribution(samples.service));
                report.field("search_visible_after_ack", distribution(samples.visible));
                report.field("stats_window", "warmup_and_measurement_including_drain");
                report.field("stats_before", before);
                report.field("stats_after", after);
                report.endObject();
                final Path output = Path.of(System.getProperty("tests.latency.output"));
                Files.createDirectories(output.toAbsolutePath().getParent());
                Files.write(output, BytesReference.toBytes(BytesReference.bytes(report)));
                logger.info("Latency baseline written to {}: {}", output, BytesReference.bytes(report).utf8ToString());
            }
            assertEquals(
                "accepted + failed + rejected must cover offered measurement traffic",
                samples.ack.length,
                Arrays.stream(samples.ack).filter(n -> n >= 0).count() + samples.failures.get() + samples.rejected.get()
            );
        }
    }

    private static void measure(
        HttpClient client,
        String url,
        String index,
        int id,
        int sample,
        long scheduled,
        boolean refresh,
        int searchEvery,
        String padding,
        Samples samples
    ) {
        final long started = System.nanoTime();
        try {
            final var write = send(
                client,
                url,
                "PUT",
                "/" + index + "/_doc/" + id + "?refresh=" + refresh,
                "{\"n\":" + id + ",\"msg\":\"" + padding + "\"}"
            );
            if (write.statusCode() != 201) {
                if (sample >= 0) {
                    samples.failures.incrementAndGet();
                }
                return;
            }
            final long acknowledged = System.nanoTime();
            if (sample >= 0) {
                samples.ack[sample] = acknowledged - scheduled;
                samples.service[sample] = acknowledged - started;
            }
            if (sample >= 0 && searchEvery > 0 && sample % searchEvery == 0) {
                while (System.nanoTime() - acknowledged < TimeUnit.SECONDS.toNanos(10)) {
                    samples.searchAttempts.incrementAndGet();
                    final var response = send(
                        client,
                        url,
                        "POST",
                        "/" + index + "/_search",
                        "{\"size\":1,\"query\":{\"ids\":{\"values\":[\"" + id + "\"]}}}"
                    );
                    if (response.statusCode() == 200) {
                        final Map<?, ?> hits = (Map<?, ?>) json(response).get("hits");
                        final List<?> matches = (List<?>) hits.get("hits");
                        if (matches.isEmpty() == false) {
                            final Map<?, ?> hit = (Map<?, ?>) matches.getFirst();
                            if (Integer.toString(id).equals(hit.get("_id")) == false
                                || ((Number) ((Map<?, ?>) hit.get("_source")).get("n")).intValue() != id) {
                                throw new IllegalStateException("search returned the wrong document version");
                            }
                            samples.visible[sample] = System.nanoTime() - acknowledged;
                            return;
                        }
                    }
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                samples.freshnessFailures.incrementAndGet();
            }
        } catch (Exception e) {
            if (sample >= 0) {
                if (samples.ack[sample] >= 0) {
                    samples.freshnessFailures.incrementAndGet();
                } else {
                    samples.failures.incrementAndGet();
                }
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static HttpResponse<String> send(HttpClient client, String url, String method, String path, String body) throws Exception {
        return client.send(
            HttpRequest.newBuilder(URI.create(url + path))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString()
        );
    }

    private static Map<String, Object> json(HttpResponse<String> response) {
        return XContentHelper.convertToMap(new BytesArray(response.body()), false, MediaTypeRegistry.JSON).v2();
    }

    private static int setting(String key, int fallback, int min, int max) {
        final int value = Integer.parseInt(System.getProperty("tests.latency." + key, Integer.toString(fallback)));
        if (value < min || value > max) {
            throw new IllegalArgumentException(key + " must be between " + min + " and " + max);
        }
        return value;
    }

    private static Map<String, Long> distribution(long[] values) {
        final long[] sorted = Arrays.stream(values).filter(value -> value >= 0).sorted().toArray();
        if (sorted.length == 0) {
            return Map.of("samples", 0L);
        }
        return Map.of(
            "samples",
            (long) sorted.length,
            "p50_nanos",
            sorted[(int) Math.ceil(sorted.length * .50) - 1],
            "p95_nanos",
            sorted[(int) Math.ceil(sorted.length * .95) - 1],
            "p99_nanos",
            sorted[(int) Math.ceil(sorted.length * .99) - 1],
            "max_nanos",
            sorted[sorted.length - 1]
        );
    }

    private static final class Samples {
        private final long[] ack;
        private final long[] service;
        private final long[] visible;
        private final AtomicLong rejected = new AtomicLong();
        private final AtomicLong failures = new AtomicLong();
        private final AtomicLong freshnessFailures = new AtomicLong();
        private final AtomicLong searchAttempts = new AtomicLong();

        private Samples(int count) {
            ack = new long[count];
            service = new long[count];
            visible = new long[count];
            Arrays.fill(ack, -1);
            Arrays.fill(service, -1);
            Arrays.fill(visible, -1);
        }
    }
}
