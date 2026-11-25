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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An index a snapshot restore marked and did not finish serves nothing, as it does on a Rust node.
 *
 * <p>A Rust restore sets {@code index.restore.in_progress} on the descriptor until every shard has landed, so a node
 * that dies part-way leaves an index that refuses to answer rather than one that answers from empty shards. This node
 * reads that mark: a search is 503 {@code cluster_block_exception} and a write finds no index.
 */
public class ServerlessRestoreMarkTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"keyword\"}}}";
    private static final Settings MARK = Settings.builder().put(IndexDescriptor.RESTORE_IN_PROGRESS_SETTING, "true").build();

    public void testTheMarkIsReadFromTheDescriptorSettings() {
        assertTrue(new IndexDescriptor("marked", "uuid-marked-000000000", 1, MAPPING, MARK).isRestoring());
        assertFalse(new IndexDescriptor("whole", "uuid-whole-0000000000", 1, MAPPING, null).isRestoring());
        assertFalse(
            new IndexDescriptor("other", "uuid-other-000000000", 1, MAPPING, Settings.builder().put("index.refresh_interval", "5s").build())
                .isRestoring()
        );
    }

    public void testAMarkedIndexAnswersNoSearchAndTakesNoWrite() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(
            new FsBlobStore(1024, createTempDir(), false),
            BlobPath.cleanPath(),
            clock::get,
            30_000L
        );
        plane.createIndex(new IndexDescriptor("marked", "uuid-marked-000000000", 1, MAPPING, MARK));
        plane.createIndex(new IndexDescriptor("whole", "uuid-whole-0000000000", 1, MAPPING, null));
        final Settings settings = Settings.builder()
            .put("node.name", "restore-mark")
            .put("cluster.name", "serverless-restore-mark")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
        try (ServerlessNode node = new ServerlessNode(settings)) {
            node.startup().start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("whole", 0);
            loop.tick(clock.get());

            assertEquals(201, send(node, "PUT", "/whole/_doc/1?refresh=true", "{\"msg\":\"a\"}").status());
            final Response whole = send(node, "POST", "/whole/_search", "{\"query\":{\"match_all\":{}}}");
            assertEquals(whole.body(), 200, whole.status());

            final Response search = send(node, "POST", "/marked/_search", "{\"query\":{\"match_all\":{}}}");
            assertEquals(search.body(), 503, search.status());
            assertTrue(search.body(), search.body().contains("cluster_block_exception"));
            assertTrue(search.body(), search.body().contains("restore"));

            // Named beside a whole index, the whole search is refused rather than answered without it.
            final Response both = send(node, "POST", "/whole,marked/_search", "{\"query\":{\"match_all\":{}}}");
            assertEquals(both.body(), 503, both.status());

            final Response write = send(node, "PUT", "/marked/_doc/1", "{\"msg\":\"a\"}");
            assertEquals(write.body(), 404, write.status());
            assertTrue(write.body(), write.body().contains("index_not_found"));
        }
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .method(method, HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
