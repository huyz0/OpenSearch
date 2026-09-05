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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Aggregations, reduced across shards.
 *
 * <p>The last thing the search surface refused outright, and the reason it did was honest: a shard's terms
 * aggregation holds <em>that shard's</em> counts, and returning one of them would be answering with a
 * number computed over a fraction of the index. Combining them is not summing — it is merging ordered
 * buckets, deciding which terms survive, and carrying the error bounds that say how wrong the answer might
 * be. So the reduce is core's own {@code InternalAggregations#topLevelReduce}, the code a classic
 * coordinator runs, rather than a second implementation that would agree with it until it did not.
 *
 * <p><b>Every fixture here is built so that one shard cannot answer it.</b> Each term appears on more than
 * one shard, so a count that came from a single shard, or a merge that took the first answer and discarded
 * the rest, is a different and visibly wrong number.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessAggregationTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int SHARDS = 3;
    private static final String MAPPING = "{\"properties\":{"
        + "\"msg\":{\"type\":\"text\"},"
        + "\"amount\":{\"type\":\"long\"},"
        + "\"colour\":{\"type\":\"keyword\"}}}";

    /** Twelve documents: four red, four green, four blue, with amounts that sum to something checkable. */
    private static final String[] COLOURS = {
        "red",
        "green",
        "blue",
        "red",
        "green",
        "blue",
        "red",
        "green",
        "blue",
        "red",
        "green",
        "blue" };

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-aggs")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** A terms aggregation counts the whole index, not one shard of it. */
    public void testATermsAggregationCountsEveryShard() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("stock", "uuid-stock-00000000", SHARDS, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("aggs-terms"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, "stock");
            fill(node, "stock");
            assertTrue("the fixture is pointless unless the documents actually spread", node.reconciler().openShards().size() > 1);

            final Response got = send(
                node,
                "POST",
                "/stock/_search",
                "{\"size\":0,\"aggs\":{\"by_colour\":{\"terms\":{\"field\":\"colour\"}}}}"
            );
            assertEquals(got.body(), 200, got.status());
            assertTrue("every shard must have answered: " + got.body(), got.body().contains("\"complete\":true"));

            // Four of each, and the point is that no single shard holds four of anything.
            final Map<String, Long> buckets = bucketsIn(got.body());
            assertEquals(
                "the counts must be the index's, not a shard's: " + got.body(),
                Map.of("red", 4L, "green", 4L, "blue", 4L),
                buckets
            );
        }
    }

    /** A numeric aggregation sums across shards. */
    public void testANumericAggregationCombinesAcrossShards() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("totals", "uuid-totals-0000000", SHARDS, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("aggs-sum"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, "totals");
            fill(node, "totals");

            final Response got = send(
                node,
                "POST",
                "/totals/_search",
                "{\"size\":0,\"aggs\":{\"total\":{\"sum\":{\"field\":\"amount\"}}}}"
            );
            assertEquals(got.body(), 200, got.status());
            // Amounts are 1..12, which sum to 78. A single shard holds a third of that at most.
            assertTrue("the sum must be over every shard: " + got.body(), got.body().contains("\"value\":78.0"));
        }
    }

    /** {@code size: 0} returns aggregations and no hits, which is how most aggregations are asked for. */
    public void testAggregationsComeBackWithoutHits() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("quiet", "uuid-quiet-00000000", SHARDS, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("aggs-quiet"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, "quiet");
            fill(node, "quiet");

            final Response got = send(
                node,
                "POST",
                "/quiet/_search",
                "{\"size\":0,\"aggs\":{\"by_colour\":{\"terms\":{\"field\":\"colour\"}}}}"
            );
            assertEquals(got.body(), 200, got.status());
            assertTrue("the total must still be reported: " + got.body(), got.body().contains("\"value\":12"));
            assertTrue("and no hits returned: " + got.body(), got.body().contains("\"hits\":[]"));
            assertFalse("but the aggregation must be there", bucketsIn(got.body()).isEmpty());
        }
    }

    /** An aggregation combined from shards on two nodes, so the buckets have to survive the wire. */
    public void testAnAggregationSurvivesTheWire() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("split", "uuid-split-00000000", SHARDS, MAPPING, null));

        try (
            ServerlessNode first = new ServerlessNode(nodeSettings("aggs-wire-1"));
            ServerlessNode second = new ServerlessNode(nodeSettings("aggs-wire-2"))
        ) {
            first.start();
            second.start();
            first.setMetadataPlane(plane);
            second.setMetadataPlane(plane);

            final BackgroundReconciler one = new BackgroundReconciler(first, plane);
            one.want("split", 0);
            one.tick(clock.get());
            final BackgroundReconciler two = new BackgroundReconciler(second, plane);
            two.want("split", 1);
            two.want("split", 2);
            two.tick(clock.get());
            assertEquals("the first node must hold exactly one shard", 1, first.reconciler().openShards().size());
            assertEquals("and its peer the other two", 2, second.reconciler().openShards().size());

            fill(first, "split");

            final Response got = send(
                first,
                "POST",
                "/split/_search",
                "{\"size\":0,\"aggs\":{\"by_colour\":{\"terms\":{\"field\":\"colour\"}}}}"
            );
            assertEquals(got.body(), 200, got.status());
            assertTrue("every shard must have answered: " + got.body(), got.body().contains("\"complete\":true"));
            assertEquals(
                "buckets that crossed the transport must still be counted: " + got.body(),
                Map.of("red", 4L, "green", 4L, "blue", 4L),
                bucketsIn(got.body())
            );
        }
    }

    /** An aggregation over a filtered query aggregates the matches, not the index. */
    public void testAnAggregationRespectsTheQuery() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("filtered", "uuid-filtered-00000", SHARDS, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("aggs-filtered"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, "filtered");
            fill(node, "filtered");

            // Amounts 7..12 are six documents, two of each colour.
            final Response got = send(
                node,
                "POST",
                "/filtered/_search",
                "{\"size\":0,\"query\":{\"range\":{\"amount\":{\"gte\":7}}},\"aggs\":{\"by_colour\":{\"terms\":{\"field\":\"colour\"}}}}"
            );
            assertEquals(got.body(), 200, got.status());
            assertEquals(
                "an aggregation must count what the query matched: " + got.body(),
                Map.of("red", 2L, "green", 2L, "blue", 2L),
                bucketsIn(got.body())
            );
        }
    }

    private MetadataPlane plane(AtomicLong clock) throws java.io.IOException {
        return new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
    }

    private void hold(ServerlessNode node, MetadataPlane plane, AtomicLong clock, String index) throws Exception {
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (int shard = 0; shard < SHARDS; shard++) {
            loop.want(index, shard);
        }
        loop.tick(clock.get());
    }

    private void fill(ServerlessNode node, String index) throws Exception {
        for (int i = 0; i < COLOURS.length; i++) {
            final String body = "{\"msg\":\"doc\",\"amount\":" + (i + 1) + ",\"colour\":\"" + COLOURS[i] + "\"}";
            final Response written = send(node, "PUT", "/" + index + "/_doc/a" + i + "?refresh=true", body);
            assertEquals("the fixture must load: " + written.body(), 201, written.status());
        }
    }

    private static final Pattern BUCKET = Pattern.compile("\\{\"key\":\"([a-z]+)\",\"doc_count\":(\\d+)\\}");

    private static Map<String, Long> bucketsIn(String body) {
        final Map<String, Long> buckets = new LinkedHashMap<>();
        final Matcher matcher = BUCKET.matcher(body);
        while (matcher.find()) {
            buckets.put(matcher.group(1), Long.parseLong(matcher.group(2)));
        }
        return buckets;
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
