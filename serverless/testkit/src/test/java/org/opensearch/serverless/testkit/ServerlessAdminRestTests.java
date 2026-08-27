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
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Phase 7: the allowlisted admin surface, served from the metadata plane over real HTTP.
 *
 * <p>Every endpoint here is one or two object-store reads. None of them consults a cluster-manager,
 * because there isn't one, and none of them answers a question it cannot actually answer — which is the
 * whole of decision D2 and the reason the classic endpoints still return 501 rather than something
 * plausible.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessAdminRestTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-p7")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private record Response(int status, String body) {
    }

    private static Response send(TransportAddress address, String method, String path, String body) throws Exception {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest.BodyPublisher payload = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .method(method, payload)
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }

    public void testIndexLifecycleOverRest() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("p7-a"))) {
            node.start();
            node.setMetadataPlane(plane);
            final TransportAddress http = node.boundHttpAddress().publishAddress();

            // Absent before creation, and said so rather than answered emptily.
            final Response missing = send(http, "GET", "/alpha", null);
            assertEquals(404, missing.status());
            assertTrue(missing.body().contains("index_not_found"));

            final Response created = send(http, "PUT", "/alpha?shards=2", "{\"properties\":{\"msg\":{\"type\":\"text\"}}}");
            assertEquals(200, created.status());
            assertTrue(created.body().contains("\"acknowledged\":true"));

            // The descriptor is really in the object store, not merely in a response.
            assertTrue("the REST create did not reach the metadata plane", plane.describe("alpha").isPresent());
            assertEquals(2, plane.describe("alpha").orElseThrow().numberOfShards());

            final Response fetched = send(http, "GET", "/alpha", null);
            assertEquals(200, fetched.status());
            assertTrue(fetched.body().contains("\"shards\":2"));
            assertTrue("the mapping should have been stored", fetched.body().contains("\"has_mapping\":true"));

            // Name uniqueness, arbitrated by a put-if-absent rather than by an elected node.
            final Response duplicate = send(http, "PUT", "/alpha", null);
            assertEquals(400, duplicate.status());
            assertTrue(duplicate.body().contains("index_already_exists"));

            final Response deleted = send(http, "DELETE", "/alpha", null);
            assertEquals(200, deleted.status());
            assertTrue(plane.describe("alpha").isEmpty());

            assertEquals("deleting twice must not pretend to succeed", 404, send(http, "DELETE", "/alpha", null).status());
        }
    }

    public void testCatalogReportsIndicesShardsAndNodes() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("p7-b"))) {
            node.start();
            node.setMetadataPlane(plane);
            final TransportAddress http = node.boundHttpAddress().publishAddress();

            send(http, "PUT", "/alpha?shards=2", "{\"properties\":{\"msg\":{\"type\":\"text\"}}}");
            send(http, "PUT", "/beta", null);

            final Response indices = send(http, "GET", "/_serverless/indices", null);
            assertEquals(200, indices.status());
            assertTrue(indices.body().contains("\"count\":2"));
            assertTrue(indices.body().contains("alpha"));
            assertTrue(indices.body().contains("beta"));

            // Shards before anything is activated: a distinct, honest state.
            final Response cold = send(http, "GET", "/_serverless/shards/alpha", null);
            assertEquals(200, cold.status());
            assertTrue("a never-activated shard must say so: " + cold.body(), cold.body().contains("never_activated"));
            assertTrue(cold.body().contains("\"published\":false"));

            // Activate one and publish, then look again.
            final ShardId shardId = node.activateWriter(plane, "alpha", 0).orElseThrow();
            ShardOps.indexDoc(node.reconciler().shard(shardId), "1", "{\"msg\":\"hello\"}");
            node.reconciler().shard(shardId).refresh("p7");
            node.publishShard(shardId, plane.heads().read("alpha", 0).orElseThrow().term());

            final Response warm = send(http, "GET", "/_serverless/shards/alpha", null);
            assertTrue("an owned shard must report its owner: " + warm.body(), warm.body().contains(node.localNode().getId()));
            assertTrue(warm.body().contains("\"state\":\"owned\""));
            assertTrue(warm.body().contains("\"published\":true"));
            // Shard 1 was never activated, so both states appear in one response.
            assertTrue(warm.body().contains("never_activated"));

            assertEquals(404, send(http, "GET", "/_serverless/shards/ghost", null).status());

            // Nodes comes from live leases, and is named for that rather than for a cluster.
            plane.membership()
                .renew(
                    new org.opensearch.serverless.membership.NodeLease(
                        node.localNode().getId(),
                        node.localNode().getEphemeralId(),
                        "127.0.0.1:9300",
                        java.util.Set.of("ingest"),
                        0L
                    )
                );
            final Response nodes = send(http, "GET", "/_serverless/nodes", null);
            assertEquals(200, nodes.status());
            assertTrue(nodes.body().contains("\"count\":1"));
            assertTrue(nodes.body().contains("live_leases_observed_by_this_node"));
            assertTrue(nodes.body().contains(node.localNode().getId()));
        }
    }

    /** Registering /{index} must not have swallowed the routes that were already there. */
    public void testTheNewRoutesDoNotShadowTheExistingSurface() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("p7-c"))) {
            node.start();
            node.setMetadataPlane(plane);
            final TransportAddress http = node.boundHttpAddress().publishAddress();

            final Response root = send(http, "GET", "/", null);
            assertEquals(200, root.status());
            assertTrue(root.body().contains("\"flavour\":\"serverless\""));

            final Response health = send(http, "GET", "/_serverless/health", null);
            assertEquals(200, health.status());
            assertTrue(health.body().contains("\"status\":\"serving\""));

            // D2 still holds: the classic endpoints refuse rather than answering as an index would.
            for (String path : new String[] { "/_cluster/health", "/_cat/indices", "/_nodes", "/_cluster/state" }) {
                final Response refused = send(http, "GET", path, null);
                assertEquals("classic endpoint " + path + " stopped refusing", 501, refused.status());
                assertTrue(refused.body().contains("no cluster-wide state"));
            }
        }
    }

    /** Without a metadata plane the admin surface says so, rather than behaving as if nothing exists. */
    public void testAdminEndpointsRefuseWithoutAMetadataPlane() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("p7-d"))) {
            node.start();   // deliberately no setMetadataPlane
            final TransportAddress http = node.boundHttpAddress().publishAddress();

            for (String path : new String[] { "/alpha", "/_serverless/indices", "/_serverless/nodes" }) {
                final Response response = send(http, "GET", path, null);
                assertEquals("an unconfigured node must not answer " + path + " as though it were empty", 503, response.status());
                assertTrue(response.body().contains("no_metadata_plane"));
            }
        }
    }
}
