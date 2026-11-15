/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.usage;

import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.serverless.store.ObjectStoreSettings;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A quiet tenant's search latency on one node, alone, and beside a noisy tenant that keeps many searches running, with
 * the search fair share off and on.
 *
 * <p>Off unless {@code -Dtests.meter.search_share=true}; {@code -Dtests.meter.search_share.noisy} sets the noisy
 * tenant's concurrent searchers (32).
 */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*")
public class SearchFairShareMeterTests extends OpenSearchTestCase {

    private static final int QUIET_SEARCHES = 300;
    private static final String QUERY = "{\"size\":20,\"query\":{\"match\":{\"body\":\"alpha beta gamma\"}},"
        + "\"aggs\":{\"n\":{\"terms\":{\"field\":\"tag\",\"size\":50}}}}";

    public void testQuietTenantLatencyBesideANoisyOne() throws Exception {
        assumeTrue(
            "a meter, run on demand",
            org.opensearch.common.Booleans.parseBoolean(System.getProperty("tests.meter.search_share", "false"))
        );
        final int noisy = Integer.getInteger("tests.meter.search_share.noisy", 32);
        final double[] alone = run(0.25, 0);
        final double[] off = run(0.0, noisy);
        final double[] on = run(0.25, noisy);
        logger.info(
            String.format(
                Locale.ROOT,
                "SHARE|alone (share 0.25)|p50 %.1f ms|p99 %.1f ms|noisy %.0f searches/s",
                alone[0],
                alone[1],
                alone[2]
            )
        );
        logger.info(
            String.format(
                Locale.ROOT,
                "SHARE|noisy x%d, share off|p50 %.1f ms|p99 %.1f ms|noisy %.0f searches/s",
                noisy,
                off[0],
                off[1],
                off[2]
            )
        );
        logger.info(
            String.format(
                Locale.ROOT,
                "SHARE|noisy x%d, share 0.25|p50 %.1f ms|p99 %.1f ms|noisy %.0f searches/s",
                noisy,
                on[0],
                on[1],
                on[2]
            )
        );
    }

    /** Returns the quiet tenant's p50 and p99 in milliseconds, and the noisy one's searches a second meanwhile. */
    private double[] run(double share, int noisyThreads) throws Exception {
        final Settings settings = Settings.builder()
            .put("node.name", "share")
            .put("cluster.name", "serverless-share")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(ObjectStoreSettings.PATH.getKey(), createTempDir().toString())
            .put(UsageSettings.SEARCH_TENANT_SHARE.getKey(), share)
            .build();
        try (
            ServerlessBootstrap boot = ServerlessBootstrap.start(settings);
            HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(10)).build()
        ) {
            final int port = boot.node().boundHttpAddress().publishAddress().getPort();
            for (String index : new String[] { "noisy", "quiet" }) {
                send(
                    http,
                    port,
                    "PUT",
                    "/" + index,
                    "{\"settings\":{\"number_of_shards\":1},"
                        + "\"mappings\":{\"properties\":{\"body\":{\"type\":\"text\"},\"tag\":{\"type\":\"keyword\"}}}}"
                );
                for (int batch = 0; batch < 10; batch++) {
                    final StringBuilder bulk = new StringBuilder();
                    for (int i = 0; i < 500; i++) {
                        bulk.append("{\"index\":{\"_index\":\"")
                            .append(index)
                            .append("\"}}\n{\"body\":\"alpha beta gamma delta ")
                            .append(randomAlphaOfLength(8))
                            .append("\",\"tag\":\"t")
                            .append(i % 97)
                            .append("\"}\n");
                    }
                    send(http, port, "POST", "/_bulk" + (batch == 9 ? "?refresh=true" : ""), bulk.toString());
                }
            }
            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicLong noisyDone = new AtomicLong();
            final List<Thread> threads = new ArrayList<>();
            for (int t = 0; t < noisyThreads; t++) {
                final Thread thread = new Thread(() -> {
                    while (stop.get() == false) {
                        try {
                            send(http, port, "POST", "/noisy/_search", QUERY);
                            noisyDone.incrementAndGet();
                        } catch (Exception e) {
                            return;
                        }
                    }
                });
                thread.start();
                threads.add(thread);
            }
            Thread.sleep(noisyThreads == 0 ? 0L : 2_000L);
            final List<Double> latencies = new ArrayList<>();
            final long noisyBefore = noisyDone.get();
            final long quietStarted = System.nanoTime();
            for (int i = 0; i < QUIET_SEARCHES; i++) {
                final long start = System.nanoTime();
                send(http, port, "POST", "/quiet/_search", QUERY);
                latencies.add((System.nanoTime() - start) / 1e6);
            }
            final double noisyDuring = (noisyDone.get() - noisyBefore) / ((System.nanoTime() - quietStarted) / 1e9);
            stop.set(true);
            for (Thread thread : threads) {
                thread.join(60_000L);
            }
            Collections.sort(latencies);
            return new double[] { latencies.get(latencies.size() / 2), latencies.get(latencies.size() * 99 / 100), noisyDuring };
        }
    }

    private static void send(HttpClient http, int port, String method, String path, String body) throws Exception {
        final HttpResponse<String> response = http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .header("Content-Type", path.contains("_bulk") ? "application/x-ndjson" : "application/json")
                .timeout(Duration.ofSeconds(120))
                .method(method, HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString()
        );
        assertTrue(
            path + ": " + response.statusCode() + " " + response.body(),
            response.statusCode() == 200 || response.statusCode() == 201
        );
    }
}
