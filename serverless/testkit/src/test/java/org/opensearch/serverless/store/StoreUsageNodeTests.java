/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.serverless.store.IndexUsage.Kind.BLOB_READ;
import static org.opensearch.serverless.store.IndexUsage.Kind.BLOB_WRITE;
import static org.opensearch.serverless.store.IndexUsage.Kind.BYTES_WRITTEN;
import static org.opensearch.serverless.store.IndexUsage.Kind.REGISTER_WRITE;

/**
 * A real node writing and searching two indices: every request it made is charged to exactly one line, and the
 * indices' own work -- their share of the node log, their publishes, heads and reads -- is charged to them, not to the
 * node.
 */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*")
public class StoreUsageNodeTests extends OpenSearchTestCase {

    public void testEveryRequestOfARealNodeIsChargedOnce() throws Exception {
        final Settings settings = Settings.builder()
            .put("node.name", "usage")
            .put("cluster.name", "serverless-usage")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(ObjectStoreSettings.PATH.getKey(), createTempDir().toString())
            .build();
        try (
            ServerlessBootstrap boot = ServerlessBootstrap.start(settings);
            HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(10)).build()
        ) {
            final int port = boot.node().boundHttpAddress().publishAddress().getPort();
            final MeteredBlobStore store = (MeteredBlobStore) boot.plane().blobStore();
            for (String index : new String[] { "small-tenant", "big-tenant" }) {
                send(http, port, "PUT", "/" + index, "{\"settings\":{\"number_of_shards\":1}}");
            }
            for (int round = 0; round < 5; round++) {
                // Both written in the same windows, the big one with ten times the bytes.
                final StringBuilder bulk = new StringBuilder();
                for (int i = 0; i < 10; i++) {
                    bulk.append("{\"index\":{\"_index\":\"big-tenant\"}}\n{\"body\":\"").append("word ".repeat(200)).append("\"}\n");
                }
                bulk.append("{\"index\":{\"_index\":\"small-tenant\"}}\n{\"body\":\"").append("word ".repeat(20)).append("\"}\n");
                send(http, port, "POST", "/_bulk?refresh=true", bulk.toString());
                send(http, port, "POST", "/small-tenant,big-tenant/_search", "{\"query\":{\"match\":{\"body\":\"word\"}}}");
            }

            final StoreUsage usage = store.usage();
            assertNotNull("metering is on by default", usage);
            for (Map.Entry<String, IndexUsage> line : usage.lines().entrySet()) {
                final IndexUsage u = line.getValue();
                logger.info(
                    String.format(
                        Locale.ROOT,
                        "USAGE|%s|register r %.2f w %.2f|blob r %.2f w %.2f|list %.2f|delete %.2f|bytes r %d w %d|cost %.2f",
                        line.getKey(),
                        u.get(IndexUsage.Kind.REGISTER_READ) / 1000d,
                        u.get(REGISTER_WRITE) / 1000d,
                        u.get(BLOB_READ) / 1000d,
                        u.get(BLOB_WRITE) / 1000d,
                        u.get(IndexUsage.Kind.LISTING) / 1000d,
                        u.get(IndexUsage.Kind.DELETE) / 1000d,
                        u.get(IndexUsage.Kind.BYTES_READ),
                        u.get(BYTES_WRITTEN),
                        u.costMilliUnits() / 1000d
                    )
                );
            }
            StoreUsageTests.assertAddsUp(store);

            final IndexUsage small = usage.of("small-tenant");
            final IndexUsage big = usage.of("big-tenant");
            final IndexUsage node = usage.of(StoreUsage.NODE);
            for (IndexUsage tenant : new IndexUsage[] { small, big }) {
                assertTrue("heads and commits", tenant.get(REGISTER_WRITE) > 0L);
                assertTrue("log shares and publishes", tenant.get(BLOB_WRITE) > 0L);
            }
            assertTrue("the bigger writer pays for more of the bytes", big.get(BYTES_WRITTEN) > 5 * small.get(BYTES_WRITTEN));
            // The node log is one PUT for both indices: charged to them by bytes, so none of it is the node's.
            final long nodeLogBytesOnNode = node.get(BYTES_WRITTEN);
            assertTrue(
                "node line wrote " + nodeLogBytesOnNode + " of " + store.bytesWritten() + " bytes",
                nodeLogBytesOnNode * 10 < store.bytesWritten()
            );
        }
    }

    private static void send(HttpClient http, int port, String method, String path, String body) throws Exception {
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
