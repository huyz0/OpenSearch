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
 * Writing through an alias, as core does: to its {@code is_write_index} member, or to its only index; and rolling it over so
 * the new index takes the writes while the old ones stay readable through it.
 */
public class ServerlessWriteAliasTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"mappings\":{\"properties\":{\"msg\":{\"type\":\"keyword\"}}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-write-alias")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
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

    private MetadataPlane plane;

    private ServerlessNode started(String name, AtomicLong clock) throws Exception {
        plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, 30_000L);
        final ServerlessNode node = new ServerlessNode(nodeSettings(name));
        node.startup().start();
        node.setMetadataPlane(plane);
        return node;
    }

    private void hold(ServerlessNode node, AtomicLong clock, String... indices) throws Exception {
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (String index : indices) {
            loop.want(index, 0);
        }
        loop.tick(clock.get());
    }

    /** An alias created with the index, marked as its write index: reported as such, and a write through it lands there. */
    public void testCreateWithAWriteAliasAndWriteThroughIt() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = started("write-alias-create", clock)) {
            final Response created = send(
                node,
                "PUT",
                "/logs-000001",
                "{\"aliases\":{\"logs\":{\"is_write_index\":true}},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"keyword\"}}}}"
            );
            assertEquals(created.body(), 200, created.status());
            hold(node, clock, "logs-000001");
            assertTrue(send(node, "GET", "/_alias/logs", null).body().contains("\"is_write_index\":true"));
            final var inView = node.clusterService().state().metadata().index("logs-000001");
            assertNotNull("the served index is in the local view", inView);
            assertEquals(
                "the local view carries the alias and its write flag, which index management reads before a rollover",
                Boolean.TRUE,
                inView.getAliases().get("logs").writeIndex()
            );

            final Response written = send(node, "PUT", "/logs/_doc/1?refresh=true", "{\"msg\":\"a\"}");
            assertEquals(written.body(), 201, written.status());
            assertTrue(
                "the write reports the index it landed in: " + written.body(),
                written.body().contains("\"_index\":\"logs-000001\"")
            );
            assertEquals(200, send(node, "GET", "/logs-000001/_doc/1", null).status());

            final Response bulk = send(
                node,
                "POST",
                "/_bulk?refresh=true",
                "{\"index\":{\"_index\":\"logs\",\"_id\":\"2\"}}\n{\"msg\":\"b\"}\n"
            );
            assertEquals(bulk.body(), 200, bulk.status());
            assertFalse(bulk.body(), bulk.body().contains("\"errors\":true"));
            assertEquals(200, send(node, "GET", "/logs-000001/_doc/2", null).status());
        }
    }

    /** An alias of one index takes writes; of two with none marked, a write is refused; marking one makes it the target. */
    public void testWhichMemberAWriteThroughAnAliasReaches() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = started("write-alias-members", clock)) {
            assertEquals(200, send(node, "PUT", "/a-1", MAPPING).status());
            assertEquals(200, send(node, "PUT", "/a-2", MAPPING).status());
            hold(node, clock, "a-1", "a-2");
            assertEquals(200, send(node, "PUT", "/a-1/_alias/both", "{}").status());
            assertEquals(
                "an alias of one index takes writes, as in core",
                201,
                send(node, "PUT", "/both/_doc/1", "{\"msg\":\"x\"}").status()
            );

            assertEquals(200, send(node, "PUT", "/a-2/_alias/both", "{}").status());
            final Response ambiguous = send(node, "PUT", "/both/_doc/2", "{\"msg\":\"x\"}");
            assertEquals("several indices and none marked: refused " + ambiguous.body(), 404, ambiguous.status());

            final Response marked = send(
                node,
                "POST",
                "/_aliases",
                "{\"actions\":[{\"add\":{\"index\":\"a-2\",\"alias\":\"both\",\"is_write_index\":true}}]}"
            );
            assertEquals(marked.body(), 200, marked.status());
            final Response written = send(node, "PUT", "/both/_doc/3", "{\"msg\":\"x\"}");
            assertEquals(written.body(), 201, written.status());
            assertTrue(written.body(), written.body().contains("\"_index\":\"a-2\""));

            final Response twoMarked = send(
                node,
                "POST",
                "/_aliases",
                "{\"actions\":[{\"add\":{\"index\":\"a-1\",\"alias\":\"both\",\"is_write_index\":true}},"
                    + "{\"add\":{\"index\":\"a-2\",\"alias\":\"both\",\"is_write_index\":true}}]}"
            );
            assertEquals(twoMarked.body(), 400, twoMarked.status());

            // Removing the marked member leaves the alias with one index and nothing marked.
            assertEquals(200, send(node, "DELETE", "/a-2/_alias/both", null).status());
            assertFalse(send(node, "GET", "/_alias/both", null).body().contains("is_write_index"));
            assertEquals(201, send(node, "PUT", "/both/_doc/4", "{\"msg\":\"x\"}").status());
        }
    }

    /** Rolling over an alias with a write index adds the new index as the write index and keeps the old one for reads. */
    public void testRolloverMovesTheWriteIndexAndKeepsTheOldForReads() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = started("write-alias-rollover", clock)) {
            assertEquals(
                200,
                send(
                    node,
                    "PUT",
                    "/logs-000001",
                    "{\"aliases\":{\"logs\":{\"is_write_index\":true}},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"keyword\"}}}}"
                ).status()
            );
            hold(node, clock, "logs-000001");
            assertEquals(201, send(node, "PUT", "/logs/_doc/old?refresh=true", "{\"msg\":\"a\"}").status());

            final Response rolled = send(node, "POST", "/logs/_rollover", null);
            assertEquals(rolled.body(), 200, rolled.status());
            assertTrue(rolled.body(), rolled.body().contains("\"new_index\":\"logs-000002\""));
            final String aliases = send(node, "GET", "/_alias/logs", null).body();
            assertTrue(
                "the old index stays in the alias: " + aliases,
                aliases.contains("\"logs-000001\":{\"aliases\":{\"logs\":{\"is_write_index\":false}}}")
            );
            assertTrue(
                "the new one takes the writes: " + aliases,
                aliases.contains("\"logs-000002\":{\"aliases\":{\"logs\":{\"is_write_index\":true}}}")
            );

            hold(node, clock, "logs-000002");
            final Response written = send(node, "PUT", "/logs/_doc/new?refresh=true", "{\"msg\":\"b\"}");
            assertEquals(written.body(), 201, written.status());
            assertTrue(written.body(), written.body().contains("\"_index\":\"logs-000002\""));
            final Response searched = send(node, "POST", "/logs/_search", "{\"query\":{\"match_all\":{}}}");
            assertTrue("both generations are read through the alias: " + searched.body(), searched.body().contains("\"value\":2"));
        }
    }
}
