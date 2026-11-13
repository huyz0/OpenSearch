/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.rest.BulkHandler;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.serverless.store.MeteredBlobStore;
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

/**
 * Small requests over HTTP with and without {@link BulkHandler#STREAMING_SETTING}: single-document writes, small bulks
 * and searches, closed loop, each mode run twice in alternation, with the object-store requests each mode made. Off
 * unless {@code -Dtests.restlat.requests} is set:
 *
 * <pre>
 *   ./gradlew :serverless:testkit:test --tests '*RestPathLatencyMeterTests' -Dtests.restlat.requests=1000
 * </pre>
 */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*")
public class RestPathLatencyMeterTests extends OpenSearchTestCase {

    private static final String DOC = "{\"title\":\"small document %d\",\"n\":%d,\"body\":\"" + "word ".repeat(180) + "\"}";

    public void testSmallRequestsWithAndWithoutStreaming() throws Exception {
        final String requestsProperty = System.getProperty("tests.restlat.requests", "");
        assumeTrue("runs only when -Dtests.restlat.requests is set", requestsProperty.isEmpty() == false);
        final int requests = Integer.parseInt(requestsProperty);
        for (boolean streaming : new boolean[] { false, true, false, true }) {
            run(streaming, requests);
        }
    }

    private void run(boolean streaming, int requests) throws Exception {
        final Settings settings = Settings.builder()
            .put("node.name", "restlat")
            .put("cluster.name", "serverless-restlat")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(BulkHandler.STREAMING_SETTING, streaming)
            .put(ServerlessBootstrap.STORE_PATH, createTempDir().toString())
            .build();
        try (
            ServerlessBootstrap boot = ServerlessBootstrap.start(settings);
            HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(10)).build()
        ) {
            final int port = boot.node().boundHttpAddress().publishAddress().getPort();
            final String mapping = "{\"properties\":{\"title\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"},\"body\":{\"type\":\"text\"}}}";
            expectOk(http, port, "PUT", "/restlat", "{\"settings\":{\"number_of_shards\":1},\"mappings\":" + mapping + "}");
            expectOk(http, port, "PUT", "/restlat/_doc/warm?refresh=true", String.format(Locale.ROOT, DOC, 0, 0));
            for (int i = 0; i < 50; i++) {
                expectOk(http, port, "PUT", "/restlat/_doc/w" + i, String.format(Locale.ROOT, DOC, i, i));
                expectOk(http, port, "POST", "/restlat/_search", "{\"query\":{\"match\":{\"title\":\"document\"}}}");
            }
            final MeteredBlobStore store = (MeteredBlobStore) boot.plane().blobStore();
            final long requestsBefore = store.impliedS3Requests();
            final List<Long> docs = new ArrayList<>();
            final List<Long> bulks = new ArrayList<>();
            final List<Long> searches = new ArrayList<>();
            for (int i = 0; i < requests; i++) {
                docs.add(timed(http, port, "PUT", "/restlat/_doc/d" + i, String.format(Locale.ROOT, DOC, i, i)));
                final StringBuilder bulk = new StringBuilder();
                for (int j = 0; j < 10; j++) {
                    bulk.append("{\"index\":{\"_id\":\"b").append(i).append('-').append(j).append("\"}}\n");
                    bulk.append(String.format(Locale.ROOT, DOC, j, j)).append('\n');
                }
                bulks.add(timed(http, port, "POST", "/restlat/_bulk", bulk.toString()));
                searches.add(timed(http, port, "POST", "/restlat/_search", "{\"query\":{\"match\":{\"title\":\"document\"}}}"));
            }
            final long storeRequests = store.impliedS3Requests() - requestsBefore;
            logger.info(
                "RESTLAT|streaming={}|{} each|doc p50 {} p99 {} ms|bulk p50 {} p99 {} ms|search p50 {} p99 {} ms|store requests {}",
                streaming,
                requests,
                ms(docs, 0.5),
                ms(docs, 0.99),
                ms(bulks, 0.5),
                ms(bulks, 0.99),
                ms(searches, 0.5),
                ms(searches, 0.99),
                storeRequests
            );
        }
    }

    private static String ms(List<Long> nanos, double p) {
        final List<Long> sorted = new ArrayList<>(nanos);
        Collections.sort(sorted);
        final long value = sorted.get(Math.min(sorted.size() - 1, (int) Math.ceil(p * sorted.size()) - 1));
        return String.format(Locale.ROOT, "%.1f", value / 1_000_000d);
    }

    private static long timed(HttpClient http, int port, String method, String path, String body) throws Exception {
        final long started = System.nanoTime();
        expectOk(http, port, method, path, body);
        return System.nanoTime() - started;
    }

    private static void expectOk(HttpClient http, int port, String method, String path, String body) throws Exception {
        final HttpResponse<String> response = http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .header("Content-Type", path.contains("_bulk") ? "application/x-ndjson" : "application/json")
                .timeout(Duration.ofSeconds(60))
                .method(method, HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString()
        );
        assertTrue(path + ": " + response.body(), response.statusCode() == 200 || response.statusCode() == 201);
        assertFalse(path + ": " + response.body(), path.contains("_bulk") && response.body().contains("\"errors\":true"));
    }
}
