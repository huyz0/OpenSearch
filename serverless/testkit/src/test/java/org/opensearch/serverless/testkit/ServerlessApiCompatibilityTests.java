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
 * M47: response shapes that cost nothing to make real-OpenSearch-compatible — field names, envelopes and
 * defaults that were shell-specific inventions with no reason to differ, as opposed to the endpoints and
 * request shapes D2 deliberately refuses (conditional writes, non-prefix wildcards, a cluster-wide surface)
 * because supporting them for real would mean building the thing this architecture exists not to need.
 *
 * <p><b>The line this milestone stays on the right side of.</b> Every fix here is a response-shape change:
 * a field renamed, a field added from a value already computed, an envelope restructured. Nothing here
 * adds a new object-store request, a new listing, or a new piece of state a node has to hold. Real
 * scalability-driven refusals — see {@code rfc-serverless-shell.md}'s D2 and §6.3 — are untouched.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessApiCompatibilityTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-api-compat")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /**
     * A deliberate refusal renders the same nested {@code error} object real OpenSearch's own uncaught
     * exceptions already did. Before this, only the caught-and-explained path used a bare string, so a
     * client that parsed errors structurally rather than only checking the HTTP status broke on precisely
     * the responses that were trying hardest to explain themselves.
     */
    public void testDeliberateRefusalsUseTheRealErrorEnvelope() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("compat-error"))) {
            node.start();
            node.setMetadataPlane(plane);

            // A deliberate refusal (conditional write), not an uncaught exception -- the case this fix is
            // specifically about, since the uncaught-exception path already rendered correctly.
            final Response refused = send(node, "PUT", "/alpha/_doc/1?version=3", "{\"msg\":\"x\"}");
            assertEquals(501, refused.status());
            assertTrue(
                "error must be a nested object with its own type and reason, matching an uncaught "
                    + "exception's own shape: "
                    + refused.body(),
                refused.body().contains("\"error\":{\"root_cause\":[{\"type\":\"unsupported_write\"")
            );
            assertTrue(
                "and the object itself must carry type and reason too, not just root_cause: " + refused.body(),
                refused.body().contains("\"type\":\"unsupported_write\",\"reason\":")
            );

            // A plain 404 renders the same shape.
            final Response missing = send(node, "GET", "/ghost", null);
            assertEquals(404, missing.status());
            assertTrue(missing.body().contains("\"error\":{\"root_cause\":[{\"type\":\"index_not_found\""));
        }
    }

    /**
     * A search response carries the fields a real OpenSearch client's own deserializer expects --
     * {@code took}, {@code timed_out}, real {@code _shards} names, {@code hits.total.relation} and
     * {@code hits.max_score} -- none of which cost anything new to report: {@code took} is measured around
     * work already being timed by nothing, the rest were values already computed under shell-specific
     * names or never rendered at all.
     */
    public void testSearchResponseCarriesRealOpenSearchFields() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("compat-search"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, "alpha", 1);
            assertEquals(201, send(node, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"hello\"}").status());

            final Response found = send(node, "POST", "/alpha/_search", "{\"query\":{\"match\":{\"msg\":\"hello\"}}}");
            assertEquals(200, found.status());
            final String body = found.body();
            assertTrue("took must be reported: " + body, body.matches("(?s).*\"took\":\\d+.*"));
            assertTrue("timed_out must be reported: " + body, body.contains("\"timed_out\":false"));
            assertTrue(
                "_shards must use real OpenSearch's own field names: " + body,
                body.contains("\"successful\":1") && body.contains("\"skipped\":0") && body.contains("\"failed\":0")
            );
            assertTrue("hits.total must carry a relation: " + body, body.contains("\"relation\":\"eq\""));
            assertTrue("a non-empty result must carry a max_score: " + body, body.matches("(?s).*\"max_score\":[0-9.].*"));
        }
    }

    /** A single-document write, delete and update all report real OpenSearch's own {@code _shards} field. */
    public void testDocumentOperationsReportShards() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("compat-shards"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, "alpha", 1);

            final Response written = send(node, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"hello\"}");
            assertEquals(201, written.status());
            assertTrue(
                "a write must report _shards: " + written.body(),
                written.body().contains("\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0}")
            );

            final Response updated = send(node, "POST", "/alpha/_update/1?refresh=true", "{\"doc\":{\"msg\":\"world\"}}");
            assertEquals(200, updated.status());
            assertTrue("an update must report _shards too: " + updated.body(), updated.body().contains("\"_shards\":{"));

            final Response deleted = send(node, "DELETE", "/alpha/_doc/1?refresh=true", null);
            assertEquals(200, deleted.status());
            assertTrue(
                "and a delete: " + deleted.body(),
                deleted.body().contains("\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0}")
            );
        }
    }

    /** Each successful item of a bulk request reports {@code _shards}, matching the single-document path. */
    public void testBulkItemsReportShards() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("compat-bulk"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, "alpha", 1);

            final Response bulked = send(
                node,
                "POST",
                "/_bulk?refresh=true",
                "{\"index\":{\"_index\":\"alpha\",\"_id\":\"1\"}}\n{\"msg\":\"hello\"}\n"
            );
            assertEquals(200, bulked.status());
            assertTrue(
                "a successful bulk item must report _shards: " + bulked.body(),
                bulked.body().contains("\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0}")
            );
        }
    }

    /** Creating an index reports {@code shards_acknowledged}, real OpenSearch's second flag. */
    public void testIndexCreateReportsShardsAcknowledged() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("compat-create"))) {
            node.start();
            node.setMetadataPlane(plane);
            final Response created = send(node, "PUT", "/alpha?shards=1", MAPPING);
            assertEquals(created.body(), 200, created.status());
            assertTrue(created.body().contains("\"acknowledged\":true"));
            assertTrue("must also report shards_acknowledged: " + created.body(), created.body().contains("\"shards_acknowledged\":true"));
        }
    }

    /**
     * {@code acknowledged} appears only on the write path -- real OpenSearch's own {@code GET
     * /_cluster/settings} has no such field, and inventing one there would be a value nothing backs.
     */
    public void testClusterSettingsAcknowledgedIsWriteOnly() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("compat-settings"))) {
            node.start();
            node.setMetadataPlane(plane);

            final Response before = send(node, "GET", "/_cluster/settings", null);
            assertEquals(200, before.status());
            assertFalse("a GET must not claim to have acknowledged anything: " + before.body(), before.body().contains("\"acknowledged\""));

            final Response written = send(node, "PUT", "/_cluster/settings", "{\"persistent\":{\"compat.probe\":\"1\"}}");
            assertEquals(written.body(), 200, written.status());
            assertTrue("a PUT must acknowledge the write: " + written.body(), written.body().contains("\"acknowledged\":true"));

            final Response after = send(node, "GET", "/_cluster/settings", null);
            assertFalse("still absent on GET after a write happened: " + after.body(), after.body().contains("\"acknowledged\""));
        }
    }

    private void hold(ServerlessNode node, MetadataPlane plane, String index, int shards) throws Exception {
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (int shard = 0; shard < shards; shard++) {
            loop.want(index, shard);
        }
        loop.tick(plane.clock().getAsLong());
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
