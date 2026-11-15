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
import org.opensearch.serverless.store.IndexUsage;
import org.opensearch.serverless.store.MeteredBlobStore;
import org.opensearch.serverless.store.ObjectStoreSettings;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * A real node enforcing: a tenant set to level one from the cluster's settings publishes less often than its neighbour
 * written at the same rate, and pays fewer store requests for it.
 */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*")
public class PublishStretchNodeTests extends OpenSearchTestCase {

    public void testATenantAtLevelOnePublishesLessOften() throws Exception {
        final Settings settings = Settings.builder()
            .put("node.name", "stretch")
            .put("cluster.name", "serverless-stretch")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(ObjectStoreSettings.PATH.getKey(), createTempDir().toString())
            .put(UsageSettings.MODE.getKey(), "enforce")
            // Published by age every second, so a short run sees many publishes.
            .put("serverless.search.tail_overlay.publish_max_age_millis", 1_000L)
            .put("serverless.wal.node_log.publish_max_age_millis", 1_000L)
            .build();
        try (
            ServerlessBootstrap boot = ServerlessBootstrap.start(settings);
            HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(10)).build()
        ) {
            final int port = boot.node().boundHttpAddress().publishAddress().getPort();
            final MeteredBlobStore store = (MeteredBlobStore) boot.plane().blobStore();
            for (String index : new String[] { "held", "free" }) {
                send(http, port, "PUT", "/" + index, "{\"settings\":{\"number_of_shards\":1}}");
                send(http, port, "PUT", "/" + index + "/_doc/0", "{\"n\":0}");
            }
            send(http, port, "PUT", "/_cluster/settings", "{\"persistent\":{\"serverless.cost.tenant_levels\":\"held=1\"}}");
            assertEquals(1, boot.node().part(Guardrails.class).level("held"));
            // Let the first writes publish, then count from here.
            Thread.sleep(3_000L);
            final long heldBefore = costOf(store, "held");
            final long freeBefore = costOf(store, "free");
            final long until = System.nanoTime() + 12_000_000_000L;
            int n = 1;
            while (System.nanoTime() < until) {
                for (String index : new String[] { "held", "free" }) {
                    send(http, port, "PUT", "/" + index + "/_doc/" + n, "{\"n\":" + n + "}");
                }
                n++;
                Thread.sleep(100L);
            }
            final long held = costOf(store, "held") - heldBefore;
            final long free = costOf(store, "free") - freeBefore;
            logger.info("STRETCH|docs {}|held {} thousandths of a PUT|free {} thousandths of a PUT", n, held, free);
            assertTrue("held " + held + " free " + free, held > 0L);
            // Both pay the same share of the node log; only the publishes differ.
            assertTrue("held " + held + " free " + free, held * 4 < free * 3);
        }
    }

    // A publish uploads the new segments, one blob each; the shard's head moves at the log's checkpoint, not per publish.
    private static long costOf(MeteredBlobStore store, String index) {
        return store.usage().of(index).get(IndexUsage.Kind.BLOB_WRITE);
    }

    private static void send(HttpClient http, int port, String method, String path, String body) throws Exception {
        final HttpResponse<String> response = http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(60))
                .method(method, HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString()
        );
        assertTrue(path + ": " + response.body(), response.statusCode() == 200 || response.statusCode() == 201);
    }
}
