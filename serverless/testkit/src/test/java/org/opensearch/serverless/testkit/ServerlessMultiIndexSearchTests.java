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
 * Searching several indices at once, and refusing to guess which ones.
 *
 * <p>A comma-separated list is resolved by looking each name up: one register read per name, no listing.
 * A pattern is refused, and the refusal is the interesting half. Resolving {@code logs-*} means enumerating
 * the deployment's indices, which is the operation &sect;6.3 declines to offer on a request path — the same
 * reason {@code /_serverless/indices} answers 501 and the same reason a plugin's index-name resolver
 * refuses. A wildcard that quietly matched only the indices this node happened to know about would be worse
 * than no wildcard at all.
 *
 * <p><b>One fan-out, not one search per index.</b> Six shards across three indices are asked at once, and
 * the window is cut once over everything — so page two of a search over three indices is the page two it
 * would be over one.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessMultiIndexSearchTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"rank\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-multi-index")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** Two indices, one search, and every hit says where it came from. */
    public void testASearchCoversEveryIndexNamed() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("logs-a", "uuid-logs-a-0000000", 2, MAPPING, null));
        plane.createIndex(new IndexDescriptor("logs-b", "uuid-logs-b-0000000", 2, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("multi-both"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, 2, "logs-a", "logs-b");

            assertEquals(201, send(node, "PUT", "/logs-a/_doc/1?refresh=true", "{\"msg\":\"shared\",\"rank\":1}").status());
            assertEquals(201, send(node, "PUT", "/logs-b/_doc/2?refresh=true", "{\"msg\":\"shared\",\"rank\":2}").status());

            final Response both = send(node, "POST", "/logs-a,logs-b/_search", "{\"size\":10,\"query\":{\"match\":{\"msg\":\"shared\"}}}");
            assertEquals(both.body(), 200, both.status());
            assertTrue("both documents must be found: " + both.body(), both.body().contains("\"value\":2"));
            assertEquals("every shard of both indices must be reported: " + both.body(), 4, shardTotal(both.body()));
            assertTrue("and all of them answered: " + both.body(), both.body().contains("\"complete\":true"));

            // Each hit names its own index, which is the difference between a multi-index search and a
            // single-index search with a longer name.
            final List<String> indices = indicesIn(both.body());
            assertTrue("a hit from each index: " + both.body(), indices.contains("logs-a") && indices.contains("logs-b"));

            // And one index alone still means one index.
            final Response one = send(node, "POST", "/logs-a/_search", "{\"size\":10,\"query\":{\"match\":{\"msg\":\"shared\"}}}");
            assertTrue("naming one index must not search the other: " + one.body(), one.body().contains("\"value\":1"));
            assertEquals(2, shardTotal(one.body()));
        }
    }

    /**
     * The page is cut once, over everything.
     *
     * <p>The assertion that catches a search which ran per index and concatenated: sorted by rank, the
     * second page of a search over both indices interleaves them.
     */
    public void testThePageIsCutAcrossTheWholeSet() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("even", "uuid-even-000000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("odd", "uuid-odd-0000000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("multi-page"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, 1, "even", "odd");

            for (int rank = 1; rank <= 10; rank++) {
                final String index = rank % 2 == 0 ? "even" : "odd";
                assertEquals(201, send(node, "PUT", "/" + index + "/_doc/d" + rank + "?refresh=true", body(rank)).status());
            }

            final Response page = send(node, "POST", "/even,odd/_search", "{\"from\":3,\"size\":4,\"sort\":[{\"rank\":\"asc\"}]}");
            assertEquals(page.body(), 200, page.status());
            assertEquals("the fourth through seventh documents overall: " + page.body(), List.of(4L, 5L, 6L, 7L), ranksIn(page.body()));
        }
    }

    /** A pattern is refused, and the refusal says why rather than returning nothing. */
    public void testAnIndexPatternIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("logs-a", "uuid-logs-a-0000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("multi-pattern"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, 1, "logs-a");

            final Response wildcard = send(node, "POST", "/logs-*/_search", "{\"query\":{\"match_all\":{}}}");
            assertEquals("a pattern must be refused, not matched against what this node knows: " + wildcard.body(), 501, wildcard.status());
            assertTrue("and say what it would have cost: " + wildcard.body(), wildcard.body().contains("enumerating every index"));
            assertTrue("and what to do instead: " + wildcard.body(), wildcard.body().contains("Name the indices"));

            // The same is true inside a list, where it would be easiest to let one through.
            assertEquals(501, send(node, "POST", "/logs-a,logs-*/_search", "{\"query\":{\"match_all\":{}}}").status());
        }
    }

    /** An index in the list that does not exist is refused, not quietly dropped. */
    public void testAMissingIndexInTheListIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("present", "uuid-present-000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("multi-missing"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, 1, "present");
            assertEquals(201, send(node, "PUT", "/present/_doc/1?refresh=true", body(1)).status());

            final Response got = send(node, "POST", "/present,absent/_search", "{\"query\":{\"match_all\":{}}}");
            // A result that looked complete while silently covering two of the three indices asked for is
            // the confident empty answer this whole surface exists to avoid.
            assertEquals("a search naming an index that does not exist must be refused: " + got.body(), 404, got.status());
            assertTrue("and name the one that is missing: " + got.body(), got.body().contains("absent"));
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

    private static String body(int rank) {
        return "{\"msg\":\"doc\",\"rank\":" + rank + "}";
    }

    private static final Pattern RANK = Pattern.compile("\"rank\":(\\d+)");
    private static final Pattern INDEX = Pattern.compile("\"_index\":\"([a-z-]+)\"");
    private static final Pattern SHARD_TOTAL = Pattern.compile("\"_shards\":\\{\"total\":(\\d+)");

    private static List<Long> ranksIn(String body) {
        final List<Long> ranks = new ArrayList<>();
        final Matcher matcher = RANK.matcher(body);
        while (matcher.find()) {
            ranks.add(Long.parseLong(matcher.group(1)));
        }
        return ranks;
    }

    private static List<String> indicesIn(String body) {
        final List<String> indices = new ArrayList<>();
        final Matcher matcher = INDEX.matcher(body);
        while (matcher.find()) {
            indices.add(matcher.group(1));
        }
        return indices;
    }

    private static int shardTotal(String body) {
        final Matcher matcher = SHARD_TOTAL.matcher(body);
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
