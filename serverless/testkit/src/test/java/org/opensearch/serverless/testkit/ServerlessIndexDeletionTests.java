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
 * What deleting an index actually deletes.
 *
 * <p>Deleting an index removes its descriptor and its shard-heads, which is enough for it to stop being
 * routable. This suite asks the questions that answer does not: what happens to the node still holding a
 * writer for it, what happens to the bytes it wrote, and what happens to somebody who creates an index of
 * the same name afterwards.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessIndexDeletionTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-delete-index")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /**
     * An index recreated with a name that was used before does not inherit the old one's documents.
     *
     * <p>The sharpest question deletion raises, because getting it wrong is not a leak but a wrong answer:
     * a caller creates an empty index, searches it, and is shown somebody else's data.
     */
    public void testARecreatedIndexIsEmpty() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final java.nio.file.Path store = createTempDir();
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, store, false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("reused", "uuid-reused-first-00", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("delete-recreate"))) {
            node.start();
            node.setMetadataPlane(plane);
            BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("reused", 0);
            loop.tick(clock.get());

            assertEquals(201, send(node, "PUT", "/reused/_doc/1?refresh=true", "{\"msg\":\"the first index\"}").status());
            // Published, so the document is in the object store and not only in this node's memory.
            loop.tick(clock.get());

            assertEquals("the index is deleted", 200, send(node, "DELETE", "/reused", null).status());
            loop.tick(clock.get());

            // The same name again, with a new identity, as a caller would get from any create.
            plane.createIndex(new IndexDescriptor("reused", "uuid-reused-second-0", 1, MAPPING, null));
            loop = new BackgroundReconciler(node, plane);
            loop.want("reused", 0);
            loop.tick(clock.get());

            final Response searched = send(node, "POST", "/reused/_search", "{\"query\":{\"match_all\":{}}}");
            assertEquals(searched.body(), 200, searched.status());
            assertTrue(
                "a freshly created index must be empty, whatever its name was used for before: " + searched.body(),
                searched.body().contains("\"value\":0")
            );
        }
    }

    /**
     * An index recreated after a delete that never finished is still empty.
     *
     * <p>This is the case the create-time clear exists for, and the one the test above cannot reach: there,
     * the delete purged the data, so creation had nothing to clear and would have passed with the clear
     * removed. Here the bytes are put back after the delete — exactly the state a process killed between
     * removing the descriptor and removing the data leaves behind — and the new index must still be empty.
     *
     * <p>That is why the argument lives at creation rather than at deletion. A delete can be interrupted;
     * a create either happened or did not.
     */
    public void testAnIndexCreatedOverTheRemainsOfAnInterruptedDeleteIsEmpty() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final java.nio.file.Path store = createTempDir();
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, store, false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("halfdeleted", "uuid-half-first-0000", 1, MAPPING, null));

        final java.nio.file.Path shardData = store.resolve("segments").resolve("halfdeleted#0");
        final java.nio.file.Path saved = createTempDir().resolve("saved");

        try (ServerlessNode node = new ServerlessNode(nodeSettings("delete-interrupted"))) {
            node.start();
            node.setMetadataPlane(plane);
            BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("halfdeleted", 0);
            loop.tick(clock.get());

            assertEquals(201, send(node, "PUT", "/halfdeleted/_doc/1?refresh=true", "{\"msg\":\"left behind\"}").status());
            loop.tick(clock.get());
            assertTrue("the fixture needs published data on disk", java.nio.file.Files.isDirectory(shardData));

            // Everything the index wrote, kept aside.
            copyTree(shardData, saved);

            assertEquals(200, send(node, "DELETE", "/halfdeleted", null).status());
            loop.tick(clock.get());

            // And put back, which is what a process killed between removing the descriptor and removing
            // the data leaves on the store.
            copyTree(saved, shardData);

            plane.createIndex(new IndexDescriptor("halfdeleted", "uuid-half-second-000", 1, MAPPING, null));
            loop = new BackgroundReconciler(node, plane);
            loop.want("halfdeleted", 0);
            loop.tick(clock.get());

            final Response searched = send(node, "POST", "/halfdeleted/_search", "{\"query\":{\"match_all\":{}}}");
            assertEquals(searched.body(), 200, searched.status());
            assertTrue(
                "a new index must not inherit the remains of an interrupted delete: " + searched.body(),
                searched.body().contains("\"value\":0")
            );
        }
    }

    /** Copies a directory tree, so a test can put back what a delete removed. */
    private static void copyTree(java.nio.file.Path from, java.nio.file.Path to) throws java.io.IOException {
        try (var walk = java.nio.file.Files.walk(from)) {
            for (java.nio.file.Path source : walk.toList()) {
                final java.nio.file.Path target = to.resolve(from.relativize(source).toString());
                if (java.nio.file.Files.isDirectory(source)) {
                    java.nio.file.Files.createDirectories(target);
                } else {
                    java.nio.file.Files.createDirectories(target.getParent());
                    java.nio.file.Files.copy(source, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    /**
     * A node holding a writer for a deleted index lets it go.
     *
     * <p>Nothing told it to. The descriptor is gone and the shard-head with it, so the node has no reason
     * to keep the shard open and no way to learn that except by noticing the index is no longer there.
     */
    public void testANodeReleasesShardsOfADeletedIndex() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("doomed", "uuid-doomed-0000000", 2, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("delete-release"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("doomed", 0);
            loop.want("doomed", 1);
            loop.tick(clock.get());
            assertEquals("both shards must be open first", 2, node.reconciler().openShards().size());

            assertEquals(200, send(node, "DELETE", "/doomed", null).status());
            loop.tick(clock.get());

            assertTrue(
                "a shard whose index no longer exists must be closed, not held forever: " + node.reconciler().openShards(),
                node.reconciler().openShards().isEmpty()
            );
        }
    }

    /**
     * The bytes a deleted index wrote are reclaimed.
     *
     * <p>Otherwise every index ever created is still being paid for. The check is deliberately on the
     * object store rather than on an API: an index that no longer answers is not the same as one that no
     * longer costs anything.
     */
    public void testADeletedIndexStopsCostingStorage() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final java.nio.file.Path store = createTempDir();
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, store, false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("costly", "uuid-costly-0000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("delete-storage"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("costly", 0);
            loop.tick(clock.get());

            for (int i = 0; i < 20; i++) {
                assertEquals(201, send(node, "PUT", "/costly/_doc/c" + i + "?refresh=true", "{\"msg\":\"payload\"}").status());
            }
            loop.tick(clock.get());
            final long before = bytesUnder(store);
            assertTrue("the fixture must have written something", before > 0);

            assertEquals(200, send(node, "DELETE", "/costly", null).status());
            loop.tick(clock.get());

            final long after = bytesUnder(store);
            assertTrue("a deleted index must stop costing storage: " + before + " bytes before, " + after + " after", after < before / 2);
        }
    }

    /** How many bytes the deployment is holding, counted from the store itself. */
    private static long bytesUnder(java.nio.file.Path root) throws java.io.IOException {
        try (var files = java.nio.file.Files.walk(root)) {
            return files.filter(java.nio.file.Files::isRegularFile).mapToLong(f -> {
                try {
                    return java.nio.file.Files.size(f);
                } catch (java.io.IOException e) {
                    return 0L;
                }
            }).sum();
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
                .method(
                    method,
                    body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)
                )
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
