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
 * An alias: one name standing for some indices.
 *
 * <p>The design question was where alias metadata lives in a system with no cluster state, and the answer
 * is that it lives in the same registers as the index descriptors. A separate namespace would have been
 * easier and would have let an index be created with a name an alias already had — after which the alias
 * silently stops resolving, because something has to win and an index winning is the only defensible order.
 * Sharing the namespace makes the collision impossible rather than unlikely: both are created with the same
 * put-if-absent against the same key, and whichever arrives second is refused.
 *
 * <p>It also makes resolution one read, which is what keeps searching through an alias the same cost as
 * searching the index directly.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessAliasTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-alias")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** Searching an alias searches everything it names, and one index alone still means one index. */
    public void testSearchingAnAliasSearchesTheIndicesItNames() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("logs-monday", "uuid-mon-0000000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("logs-tuesday", "uuid-tue-0000000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("alias-search"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, "logs-monday", "logs-tuesday");

            assertEquals(201, send(node, "PUT", "/logs-monday/_doc/1?refresh=true", "{\"msg\":\"from monday\"}").status());
            assertEquals(201, send(node, "PUT", "/logs-tuesday/_doc/2?refresh=true", "{\"msg\":\"from tuesday\"}").status());

            final Response made = send(node, "PUT", "/_alias/logs", "{\"indices\":[\"logs-monday\",\"logs-tuesday\"]}");
            assertEquals(made.body(), 200, made.status());

            final Response searched = send(node, "POST", "/logs/_search", "{\"query\":{\"match_all\":{}}}");
            assertEquals(searched.body(), 200, searched.status());
            assertTrue("both documents: " + searched.body(), searched.body().contains("\"value\":2"));
            assertEquals("and both indices' shards counted: " + searched.body(), 2, shardTotal(searched.body()));
            // Hits name the index they came from, not the alias -- an alias is a way of naming indices and
            // not a thing documents belong to.
            assertTrue(searched.body().contains("logs-monday") && searched.body().contains("logs-tuesday"));

            assertTrue(
                "searching one index directly must still mean one index",
                send(node, "POST", "/logs-monday/_search", "{\"query\":{\"match_all\":{}}}").body().contains("\"value\":1")
            );
        }
    }

    /**
     * A name cannot be both an index and an alias, in either order.
     *
     * <p>This is what sharing the register namespace buys, and it is worth asserting from both directions
     * because a check written by hand would almost certainly cover only one.
     */
    public void testANameCannotBeBothAnIndexAndAnAlias() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("real", "uuid-real-0000000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("alias-collide"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, "real");

            final Response overIndex = send(node, "PUT", "/_alias/real", "{\"indices\":[\"real\"]}");
            assertEquals("an alias must not shadow an index: " + overIndex.body(), 400, overIndex.status());
            assertTrue(overIndex.body().contains("cannot be both"));

            assertEquals(200, send(node, "PUT", "/_alias/pointer", "{\"indices\":[\"real\"]}").status());
            final Response overAlias = send(node, "PUT", "/pointer?shards=1", MAPPING);
            assertEquals("and an index must not shadow an alias: " + overAlias.body(), 400, overAlias.status());

            // The alias still resolves, which is the property the collision would have broken.
            assertEquals(200, send(node, "GET", "/_alias/pointer", null).status());
            assertTrue(send(node, "POST", "/pointer/_search", "{\"query\":{\"match_all\":{}}}").body().contains("\"complete\":true"));
        }
    }

    /** Reading, replacing and removing an alias. */
    public void testAnAliasCanBeReadAndRemoved() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("one", "uuid-one-00000000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("two", "uuid-two-00000000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("alias-lifecycle"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, "one", "two");

            assertEquals(200, send(node, "PUT", "/_alias/both", "{\"indices\":[\"one\",\"two\"]}").status());

            final Response read = send(node, "GET", "/_alias/both", null);
            assertEquals(read.body(), 200, read.status());
            // Core's shape, keyed by index, in the order the alias names them.
            assertTrue("it must say what it names: " + read.body(), read.body().contains("\"one\":{\"aliases\":{\"both\":{}}}"));
            assertTrue("in order: " + read.body(), read.body().indexOf("\"one\"") < read.body().indexOf("\"two\""));

            // Replacing is a delete and a create, said plainly rather than offered as an update that could
            // quietly lose a concurrent one.
            assertEquals(400, send(node, "PUT", "/_alias/both", "{\"indices\":[\"one\"]}").status());
            assertEquals(200, send(node, "DELETE", "/_alias/both", null).status());
            assertEquals(200, send(node, "PUT", "/_alias/both", "{\"indices\":[\"one\"]}").status());
            // Core's shape, keyed by index: {"one":{"aliases":{"both":{}}}}.
            assertTrue(send(node, "GET", "/_alias/both", null).body().contains("\"one\":{\"aliases\":{\"both\":{}}}"));

            assertEquals("deleting it twice finds nothing the second time", 200, send(node, "DELETE", "/_alias/both", null).status());
            assertEquals(404, send(node, "DELETE", "/_alias/both", null).status());
            assertEquals("and the name resolves to nothing again", 404, send(node, "GET", "/_alias/both", null).status());

            // Removing an alias must not remove what it named.
            assertEquals(200, send(node, "POST", "/one/_search", "{\"query\":{\"match_all\":{}}}").status());
        }
    }

    /** An alias over an index that does not exist is refused, at creation and at resolution. */
    public void testAnAliasCannotNameWhatIsNotThere() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("present", "uuid-present-000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("alias-missing"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, "present");

            // A typo at creation is permanent until somebody notices, so it is caught here.
            final Response typo = send(node, "PUT", "/_alias/wrong", "{\"indices\":[\"present\",\"absnet\"]}");
            assertEquals("an alias over a typo must be refused: " + typo.body(), 404, typo.status());
            assertTrue(typo.body().contains("absnet"));
            assertEquals("and not created", 404, send(node, "GET", "/_alias/wrong", null).status());

            assertEquals(400, send(node, "PUT", "/_alias/empty", "{\"indices\":[]}").status());

            // An index deleted out from under a living alias is ordinary, and gets the same answer a named
            // index would: refused, unless the caller said to ignore what is missing.
            assertEquals(200, send(node, "PUT", "/_alias/living", "{\"indices\":[\"present\"]}").status());
            assertEquals(200, send(node, "DELETE", "/present", null).status());
            final Response stale = send(node, "POST", "/living/_search", "{\"query\":{\"match_all\":{}}}");
            assertEquals("an alias naming a deleted index must not answer emptily: " + stale.body(), 404, stale.status());
            assertTrue("and must say which one is gone: " + stale.body(), stale.body().contains("present"));
        }
    }

    private MetadataPlane plane(AtomicLong clock) throws java.io.IOException {
        return new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
    }

    private void hold(ServerlessNode node, MetadataPlane plane, AtomicLong clock, String... indices) throws Exception {
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (String index : indices) {
            loop.want(index, 0);
        }
        loop.tick(clock.get());
    }

    private static int shardTotal(String body) {
        final var matcher = java.util.regex.Pattern.compile("\"_shards\":\\{\"total\":(\\d+)").matcher(body);
        assertTrue("no shard coverage in " + body, matcher.find());
        return Integer.parseInt(matcher.group(1));
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
