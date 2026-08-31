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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@code _mget} — many documents, one request.
 *
 * <p>A multi-get is worth having over a loop the caller writes for two reasons, and the tests are built
 * around both. The documents go out concurrently, so the cost is the slowest of them rather than the sum;
 * and each item answers for itself, so a document that does not exist is {@code "found": false} beside the
 * ones that do rather than an error that loses them.
 *
 * <p>What must not happen is an item quietly disappearing, so every fixture here asks for more documents
 * than exist and asserts the answer has one entry per request, in order.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessMultiGetTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-mget")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** Documents from several shards, and one that is not there, all in one answer. */
    public void testAMultiGetAnswersForEveryDocumentAsked() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 3, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("mget-basic"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, 3, "alpha");

            for (int i = 1; i <= 5; i++) {
                assertEquals(201, send(node, "PUT", "/alpha/_doc/d" + i + "?refresh=true", "{\"msg\":\"doc" + i + "\"}").status());
            }
            assertTrue("the documents must actually spread across shards", node.reconciler().openShards().size() > 1);

            final Response got = send(node, "POST", "/alpha/_mget", "{\"ids\":[\"d1\",\"missing\",\"d5\",\"d3\"]}");
            assertEquals(got.body(), 200, got.status());

            // One entry per request, in the order asked. An item that vanished would be the failure this
            // whole shape exists to prevent.
            assertEquals("four asked for, four answered: " + got.body(), List.of("d1", "missing", "d5", "d3"), idsIn(got.body()));
            assertEquals("and found where they exist: " + got.body(), List.of(true, false, true, true), foundIn(got.body()));
            assertTrue("with their sources: " + got.body(), got.body().contains("\"msg\":\"doc5\""));
            assertFalse("and nothing invented for the one that is absent", got.body().contains("\"msg\":\"missing\""));
        }
    }

    /** Documents from different indices in one request, each naming its own. */
    public void testAMultiGetSpansIndices() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("left", "uuid-left-0000000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("right", "uuid-right-000000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("mget-indices"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, 1, "left", "right");

            assertEquals(201, send(node, "PUT", "/left/_doc/1?refresh=true", "{\"msg\":\"on the left\"}").status());
            assertEquals(201, send(node, "PUT", "/right/_doc/1?refresh=true", "{\"msg\":\"on the right\"}").status());

            final Response got = send(
                node,
                "POST",
                "/_mget",
                "{\"docs\":[{\"_index\":\"left\",\"_id\":\"1\"},{\"_index\":\"right\",\"_id\":\"1\"}]}"
            );
            assertEquals(got.body(), 200, got.status());
            assertTrue("both documents, from their own indices: " + got.body(), got.body().contains("on the left"));
            assertTrue(got.body().contains("on the right"));
            assertEquals(List.of("left", "right"), indicesIn(got.body()));
        }
    }

    /**
     * An item naming an index that does not exist fails as an item, not as the request.
     *
     * <p>The distinction a multi-get exists for: one bad entry must not lose the answers to the good ones.
     */
    public void testABadItemDoesNotLoseTheGoodOnes() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("mget-partial"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, 1, "alpha");
            assertEquals(201, send(node, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"survivor\"}").status());

            final Response got = send(
                node,
                "POST",
                "/_mget",
                "{\"docs\":[{\"_index\":\"alpha\",\"_id\":\"1\"},{\"_index\":\"nowhere\",\"_id\":\"1\"}]}"
            );
            assertEquals("the request itself succeeds: " + got.body(), 200, got.status());
            assertTrue("the good document comes back: " + got.body(), got.body().contains("survivor"));
            assertTrue("and the bad one says why: " + got.body(), got.body().contains("index_not_found"));
            // Not "found: false" -- an index that does not exist is a different answer from a document
            // that is not in one, and collapsing them would tell a caller their data was deleted.
            assertEquals("two asked for, two answered", 2, idsIn(got.body()).size());
        }
    }

    /** A body that names nothing is refused rather than answered emptily. */
    public void testAnEmptyOrMalformedRequestIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("mget-refuse"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, 1, "alpha");

            assertEquals("no body at all", 400, send(node, "POST", "/alpha/_mget", null).status());
            assertEquals("a body naming nothing", 400, send(node, "POST", "/alpha/_mget", "{\"docs\":[]}").status());
            assertEquals("neither docs nor ids", 400, send(node, "POST", "/alpha/_mget", "{\"documents\":[]}").status());
            // A bare id list needs somewhere to look, and the path is the only place that can say.
            final Response noIndex = send(node, "POST", "/_mget", "{\"ids\":[\"1\"]}");
            assertEquals(400, noIndex.status());
            assertTrue("and says what is missing: " + noIndex.body(), noIndex.body().contains("index in the request path"));
        }
    }

    private MetadataPlane plane(AtomicLong clock) throws java.io.IOException {
        return new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
    }

    private void hold(ServerlessNode node, MetadataPlane plane, AtomicLong clock, int shards, String... indices) throws Exception {
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (String index : indices) {
            for (int shard = 0; shard < shards; shard++) {
                loop.want(index, shard);
            }
        }
        loop.tick(clock.get());
    }

    private static final Pattern ID = Pattern.compile("\"_id\":\"([^\"]+)\"");
    private static final Pattern INDEX = Pattern.compile("\"_index\":\"([^\"]+)\"");
    private static final Pattern FOUND = Pattern.compile("\"found\":(true|false)");

    private static List<String> idsIn(String body) {
        return all(ID, body);
    }

    private static List<String> indicesIn(String body) {
        return all(INDEX, body);
    }

    private static List<Boolean> foundIn(String body) {
        final List<Boolean> found = new ArrayList<>();
        for (String value : all(FOUND, body)) {
            found.add(Boolean.parseBoolean(value));
        }
        return found;
    }

    private static List<String> all(Pattern pattern, String body) {
        final List<String> values = new ArrayList<>();
        final Matcher matcher = pattern.matcher(body);
        while (matcher.find()) {
            values.add(matcher.group(1));
        }
        return values;
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
