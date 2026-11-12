/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;

/**
 * Heap an index holds after a write, over time, on a node started the way production starts one.
 *
 * <p>Creates {@code tests.writeheap.indices} indices (default 1,000) with the shared-schema test mapping, writes one
 * document to each, then reads the heap right after the writes and again at {@code tests.writeheap.waits} seconds
 * (default 5,15,75). What a write leaves behind until the shard next flushes is Lucene's in-memory indexing buffers.
 * Extra node settings come from {@code tests.writeheap.node.*} (prefix stripped), so a switch can be compared on and off.
 *
 * <p>A measurement, not a regression check: {@code ./gradlew :serverless:testkit:schemaHeapTest --tests '*WriteBufferHeapTests'}.
 */
@LuceneTestCase.SuppressFileSystems("*")
public class WriteBufferHeapTests extends OpenSearchTestCase {

    private static long usedHeapBytes() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            System.gc();
        }
        Thread.sleep(200);
        final Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static HttpResponse<String> call(HttpClient http, int port, String method, String path, String body) throws Exception {
        return http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString()
        );
    }

    public void testHeapHeldAfterOneWritePerIndex() throws Exception {
        final int indices = Integer.parseInt(System.getProperty("tests.writeheap.indices", "1000"));
        final int[] waits = Arrays.stream(System.getProperty("tests.writeheap.waits", "5,15,75").split(","))
            .mapToInt(Integer::parseInt)
            .toArray();
        final Settings.Builder settings = Settings.builder()
            .put("node.name", "write-heap")
            .put("cluster.name", "write-heap")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.activation.max_shards", indices + 100)
            .put(ServerlessBootstrap.STORE_PATH, createTempDir().toString());
        for (String key : java.util.Collections.list(org.opensearch.bootstrap.BootstrapInfo.getSystemProperties().keys())
            .stream()
            .map(String::valueOf)
            .toList()) {
            if (key.startsWith("tests.writeheap.node.")) {
                settings.put(key.substring("tests.writeheap.node.".length()), System.getProperty(key));
            }
        }
        final String body = "{\"settings\":{\"number_of_shards\":1},\"mappings\":" + SharedSchemaHeapTests.realisticMapping() + "}";
        try (ServerlessBootstrap boot = ServerlessBootstrap.start(settings.build()); HttpClient http = HttpClient.newHttpClient()) {
            final int port = boot.node().boundHttpAddress().publishAddress().getPort();
            for (int i = 0; i <= indices; i++) {
                final var created = call(http, port, "PUT", "/t-" + i, body);
                assertEquals(created.body(), 200, created.statusCode());
            }
            // t-0 warms the write path, so class loading is not billed to the measured indices.
            assertEquals(201, call(http, port, "POST", "/t-0/_doc", "{\"tags\":\"x\"}").statusCode());
            Thread.sleep(2_000);
            final long before = usedHeapBytes();
            final long[] nanos = new long[indices];
            for (int i = 1; i <= indices; i++) {
                final long sent = System.nanoTime();
                final var written = call(
                    http,
                    port,
                    "POST",
                    "/t-" + i + "/_doc",
                    "{\"tags\":\"x\",\"message\":\"hello world\",\"user\":{\"f0\":\"x\",\"f2\":1,\"f4\":true}}"
                );
                nanos[i - 1] = System.nanoTime() - sent;
                assertEquals(written.body(), 201, written.statusCode());
            }
            Arrays.sort(nanos);
            final long started = System.nanoTime();
            logger.info(
                "write heap: {} indices, first write p50 {} ms, p99 {} ms, heap +{} KB/index right after",
                indices,
                nanos[indices / 2] / 1e6,
                nanos[(int) (indices * 0.99)] / 1e6,
                Math.round((usedHeapBytes() - before) / 1024.0 / indices)
            );
            for (int wait : waits) {
                final long remaining = started + wait * 1_000_000_000L - System.nanoTime();
                if (remaining > 0) {
                    Thread.sleep(remaining / 1_000_000L);
                }
                logger.info(
                    "write heap: {} s after the writes, +{} KB/index, {} open shards",
                    wait,
                    Math.round((usedHeapBytes() - before) / 1024.0 / indices),
                    boot.node().reconciler().openShards().size()
                );
            }
            logger.info("write heap: histogram at the end\n{}", SharedSchemaHeapTests.classHistogram(60));
        }
    }
}
