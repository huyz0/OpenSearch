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
 * Sorting a search, across shards.
 *
 * <p>Every shard could already sort itself — a shard's query goes through the same {@code SearchService} a
 * classic node uses, and comes back as Lucene's {@code TopFieldDocs}. What was missing was the other half:
 * the coordinating node merged shards by score, so a sorted search would have returned the right documents
 * in the wrong order. That is worse than refusing, which is why it used to refuse with a 501.
 *
 * <p><b>The tests are built so that a single shard cannot answer them.</b> Documents are spread across
 * three shards and the expected order interleaves them, so a merge that concatenated shards, or ranked them
 * by score, or dropped the sort keys on the way back from a remote shard, produces a visibly different
 * answer. A single-shard fixture would pass with the merge deleted.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessSortTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int SHARDS = 3;
    private static final String MAPPING = "{\"properties\":{"
        + "\"msg\":{\"type\":\"text\"},"
        + "\"rank\":{\"type\":\"long\"},"
        + "\"name\":{\"type\":\"keyword\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-sort")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** Ascending and descending on a numeric field, across three shards. */
    public void testANumericSortOrdersHitsAcrossShards() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("ranked", "uuid-ranked-0000000", SHARDS, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("sort-numeric"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, "ranked");

            // Ten documents whose ranks are deliberately unrelated to their ids, so the routing that
            // scatters them across shards cannot accidentally agree with the sort order.
            final int[] ranks = { 7, 2, 9, 4, 1, 8, 3, 10, 5, 6 };
            for (int i = 0; i < ranks.length; i++) {
                final String body = "{\"msg\":\"doc\",\"rank\":" + ranks[i] + ",\"name\":\"n" + ranks[i] + "\"}";
                assertEquals(201, send(node, "PUT", "/ranked/_doc/d" + i + "?refresh=true", body).status());
            }
            assertTrue("the fixture is pointless unless the documents actually spread", spreadAcrossShards(node));

            final Response ascending = send(node, "POST", "/ranked/_search", "{\"size\":10,\"sort\":[{\"rank\":\"asc\"}]}");
            assertEquals(ascending.body(), 200, ascending.status());
            assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), ranksIn(ascending.body()));

            final Response descending = send(node, "POST", "/ranked/_search", "{\"size\":10,\"sort\":[{\"rank\":\"desc\"}]}");
            assertEquals(descending.body(), 200, descending.status());
            assertEquals(List.of(10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L), ranksIn(descending.body()));
        }
    }

    /**
     * A sorted page is the right page, not the first page of one shard.
     *
     * <p>The window is applied after the merge, so {@code from} and {@code size} have to mean the same
     * thing they would on one shard. This is the assertion that catches a merge which sorted correctly and
     * then cut the page before combining.
     */
    public void testASortedPageIsCutAfterMerging() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("paged", "uuid-paged-00000000", SHARDS, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("sort-paged"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, "paged");

            for (int rank = 1; rank <= 12; rank++) {
                final String body = "{\"msg\":\"doc\",\"rank\":" + rank + ",\"name\":\"n\"}";
                assertEquals(201, send(node, "PUT", "/paged/_doc/p" + rank + "?refresh=true", body).status());
            }

            final Response page = send(node, "POST", "/paged/_search", "{\"from\":4,\"size\":3,\"sort\":[{\"rank\":\"asc\"}]}");
            assertEquals(page.body(), 200, page.status());
            assertEquals("the fifth, sixth and seventh documents overall: " + page.body(), List.of(5L, 6L, 7L), ranksIn(page.body()));
        }
    }

    /** A keyword sort works too, which is a different kind of sort value on the wire. */
    public void testAKeywordSortOrdersHitsAcrossShards() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("named", "uuid-named-00000000", SHARDS, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("sort-keyword"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, "named");

            for (String name : List.of("delta", "alpha", "echo", "bravo", "charlie")) {
                final String body = "{\"msg\":\"doc\",\"rank\":1,\"name\":\"" + name + "\"}";
                assertEquals(201, send(node, "PUT", "/named/_doc/" + name + "?refresh=true", body).status());
            }

            final Response sorted = send(node, "POST", "/named/_search", "{\"size\":10,\"sort\":[{\"name\":\"asc\"}]}");
            assertEquals(sorted.body(), 200, sorted.status());
            assertEquals(List.of("alpha", "bravo", "charlie", "delta", "echo"), namesIn(sorted.body()));
        }
    }

    /** Without a sort, ranking is still by score, which is what every other test in the suite assumes. */
    public void testAnUnsortedSearchIsStillRankedByScore() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("scored", "uuid-scored-0000000", SHARDS, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("sort-none"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, "scored");

            assertEquals(201, send(node, "PUT", "/scored/_doc/1?refresh=true", "{\"msg\":\"needle\",\"rank\":1,\"name\":\"a\"}").status());
            assertEquals(
                201,
                send(node, "PUT", "/scored/_doc/2?refresh=true", "{\"msg\":\"needle needle needle\",\"rank\":2,\"name\":\"b\"}").status()
            );

            final Response scored = send(node, "POST", "/scored/_search", "{\"size\":10,\"query\":{\"match\":{\"msg\":\"needle\"}}}");
            assertEquals(scored.body(), 200, scored.status());
            // The document mentioning the term three times scores higher, and comes first.
            assertEquals("the denser match must rank first: " + scored.body(), List.of(2L, 1L), ranksIn(scored.body()));
        }
    }

    /**
     * A sort survives the wire, which is the half a single node cannot test.
     *
     * <p>Sort keys are attached by the shard that produced them and have to reach the coordinating node to
     * be merged on. When the shard is on another node they travel inside a serialised {@code SearchHit},
     * and nothing in a single-node test exercises that. Here one node holds a shard and its peer holds the
     * other two, so most of the answer arrives over the transport.
     */
    public void testASortSurvivesTheWire() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("split", "uuid-split-00000000", SHARDS, MAPPING, null));

        try (
            ServerlessNode first = new ServerlessNode(nodeSettings("sort-wire-1"));
            ServerlessNode second = new ServerlessNode(nodeSettings("sort-wire-2"))
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

            final int[] ranks = { 7, 2, 9, 4, 1, 8, 3, 10, 5, 6 };
            for (int i = 0; i < ranks.length; i++) {
                final String body = "{\"msg\":\"doc\",\"rank\":" + ranks[i] + ",\"name\":\"n\"}";
                final Response written = send(first, "PUT", "/split/_doc/w" + i + "?refresh=true", body);
                assertEquals("written through the first node, forwarded where necessary: " + written.body(), 201, written.status());
            }

            final Response sorted = send(first, "POST", "/split/_search", "{\"size\":10,\"sort\":[{\"rank\":\"asc\"}]}");
            assertEquals(sorted.body(), 200, sorted.status());
            assertTrue("every shard must have answered: " + sorted.body(), sorted.body().contains("\"complete\":true"));
            assertEquals(
                "hits that crossed the transport must still carry their sort keys: " + sorted.body(),
                List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
                ranksIn(sorted.body())
            );
        }
    }

    /** Opens every shard of an index on this node. */
    private void hold(ServerlessNode node, MetadataPlane plane, AtomicLong clock, String index) throws Exception {
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (int shard = 0; shard < SHARDS; shard++) {
            loop.want(index, shard);
        }
        loop.tick(clock.get());
    }

    /** True when the documents landed on more than one shard, which the whole fixture depends on. */
    private static boolean spreadAcrossShards(ServerlessNode node) {
        return node.reconciler().openShards().size() > 1;
    }

    private static final Pattern RANK = Pattern.compile("\"rank\":(\\d+)");
    private static final Pattern NAME = Pattern.compile("\"name\":\"([a-z]+)\"");

    private static List<Long> ranksIn(String body) {
        final List<Long> ranks = new ArrayList<>();
        final Matcher matcher = RANK.matcher(body);
        while (matcher.find()) {
            ranks.add(Long.parseLong(matcher.group(1)));
        }
        return ranks;
    }

    private static List<String> namesIn(String body) {
        final List<String> names = new ArrayList<>();
        final Matcher matcher = NAME.matcher(body);
        while (matcher.find()) {
            names.add(matcher.group(1));
        }
        return names;
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
