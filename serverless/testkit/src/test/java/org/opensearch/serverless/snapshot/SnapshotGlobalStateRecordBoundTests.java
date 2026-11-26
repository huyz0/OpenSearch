/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.snapshot;

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

/** A take whose record, with its global state, would pass the record bound is refused and leaves nothing behind. */
public class SnapshotGlobalStateRecordBoundTests extends OpenSearchTestCase {

    public void testATakeWhoseRecordWouldNotFitIsRefusedAndLeavesNothing() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(
            new FsBlobStore(1024, createTempDir(), false),
            BlobPath.cleanPath(),
            clock::get,
            300_000L
        );
        plane.createIndex(new IndexDescriptor("one", "uuid-one-0000000000", 1, "{\"properties\":{\"msg\":{\"type\":\"text\"}}}", null));
        final Settings settings = Settings.builder()
            .put("node.name", "bound-" + randomAlphaOfLength(5).toLowerCase(java.util.Locale.ROOT))
            .put("cluster.name", "serverless-snapshot-record-bound")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.snapshots.take.capture_global_state", true)
            .build();
        final long original = SnapshotGlobalState.maxRecordBytes;
        try (ServerlessNode node = new ServerlessNode(settings)) {
            node.startup().start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("one", 0);
            loop.tick(clock.get());
            assertEquals(201, send(node, "PUT", "/one/_doc/1?refresh=true", "{\"msg\":\"doc\"}"));
            loop.tick(clock.get());
            assertEquals(200, send(node, "PUT", "/_snapshot/backups", null));
            assertEquals(200, send(node, "PUT", "/_index_template/t", "{\"index_patterns\":[\"x-*\"]}"));

            final String body = "{\"indices\":\"one\",\"include_global_state\":true}";
            assertEquals("fits at the real bound", 200, send(node, "PUT", "/_snapshot/backups/ok?wait_for_completion=true", body));

            SnapshotGlobalState.maxRecordBytes = 400;
            assertEquals("too big for the lowered bound", 400, send(node, "PUT", "/_snapshot/backups/big?wait_for_completion=true", body));
            assertTrue("no record is left", plane.snapshot("backups", "big").isEmpty());
            SnapshotGlobalState.maxRecordBytes = original;
            assertEquals("the name is free again", 200, send(node, "PUT", "/_snapshot/backups/big?wait_for_completion=true", body));
        } finally {
            SnapshotGlobalState.maxRecordBytes = original;
        }
    }

    private static int send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .method(
                    method,
                    body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)
                )
                .build();
            return client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode();
        }
    }
}
