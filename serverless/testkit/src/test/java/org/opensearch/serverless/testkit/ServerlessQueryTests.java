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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A query language, rather than one hard-coded query.
 *
 * <p>Search was {@code ?q=field:value}: one {@code matchQuery} on one field, and a {@code POST} route
 * that accepted a body and never read it. Everything needed to do better was already wired — the node
 * has a real {@code SearchService} and a real mapper — so what was missing was the parsing, the merge,
 * and an honest answer for the parts that cannot be merged.
 *
 * <p><b>The merge is the part with teeth.</b> Hits used to be appended in the order the shards were
 * visited and returned in full, so a three-shard search for {@code size=10} returned up to thirty hits
 * ordered by shard number. That is not a page of a relevance-ranked search, and no test noticed, because
 * every existing search test either fits on one shard or only counts totals.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessQueryTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING =
        "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"},\"tag\":{\"type\":\"keyword\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-query")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private MetadataPlane freshIndex(AtomicLong clock, int shards) throws Exception {
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", shards, MAPPING, null));
        return plane;
    }

    private ServerlessNode started(String name, MetadataPlane plane, int shards) throws Exception {
        final ServerlessNode node = new ServerlessNode(nodeSettings(name));
        node.start();
        node.setMetadataPlane(plane);
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (int shard = 0; shard < shards; shard++) {
            loop.want("alpha", shard);
        }
        loop.tick(1_000L);
        return node;
    }

    /**
     * A real query body is parsed and answered — the thing the {@code POST} route pretended to accept.
     *
     * <p>A {@code bool} with a {@code must} and a {@code must_not} is the smallest query that cannot be
     * expressed as {@code field:value}, so answering it correctly is the whole claim in one request.
     */
    public void testABooleanQueryBodyIsParsedAndAnswered() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, 1);
        try (ServerlessNode node = started("query-bool", plane, 1)) {
            index(node, "1", "{\"msg\":\"apple pie\",\"n\":1,\"tag\":\"keep\"}");
            index(node, "2", "{\"msg\":\"apple tart\",\"n\":2,\"tag\":\"drop\"}");
            index(node, "3", "{\"msg\":\"banana pie\",\"n\":3,\"tag\":\"keep\"}");

            final String body = "{\"query\":{\"bool\":{"
                + "\"must\":[{\"match\":{\"msg\":\"apple\"}}],"
                + "\"must_not\":[{\"term\":{\"tag\":\"drop\"}}]}}}";
            final Response got = send(node, "POST", "/alpha/_search", body);
            assertEquals("a search body must be answered: " + got.body(), 200, got.status());
            assertEquals("only the apple document that is not tagged drop matches: " + got.body(), 1, hitIds(got.body()).size());
            assertEquals("and it must be the right one: " + got.body(), List.of("1"), hitIds(got.body()));
        }
    }

    /** A range query, to show the parsing is the whole language and not a second special case. */
    public void testARangeQueryBodyIsParsedAndAnswered() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, 1);
        try (ServerlessNode node = started("query-range", plane, 1)) {
            for (int i = 1; i <= 5; i++) {
                index(node, String.valueOf(i), "{\"msg\":\"ranged\",\"n\":" + i + ",\"tag\":\"t\"}");
            }
            final Response got = send(node, "POST", "/alpha/_search", "{\"query\":{\"range\":{\"n\":{\"gte\":4}}},\"size\":10}");
            assertEquals(200, got.status());
            assertEquals("only n>=4 should match: " + got.body(), 2, hitIds(got.body()).size());
        }
    }

    /**
     * The merge: {@code size} is the whole search's, not each shard's, and the page is by score.
     *
     * <p>Documents are spread over three shards and one of them is made unambiguously the best match by
     * repetition, so relevance ordering is not a coin flip. Two things are asserted that the old code got
     * wrong in two different ways: exactly {@code size} hits come back, and the best match is first.
     */
    public void testSizeAndOrderAreTheWholeSearchesNotEachShards() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, 3);
        try (ServerlessNode node = started("query-merge", plane, 3)) {
            assertEquals(
                "this node must hold every shard for the merge to be the thing under test",
                3,
                node.reconciler().openShards().size()
            );
            for (int i = 0; i < 12; i++) {
                index(node, "d" + i, "{\"msg\":\"merge\",\"n\":" + i + ",\"tag\":\"t\"}");
            }
            // Repetition makes this the top match for "merge" under any sane scoring.
            index(node, "best", "{\"msg\":\"merge merge merge merge merge\",\"n\":99,\"tag\":\"t\"}");

            final Response got = send(node, "POST", "/alpha/_search", "{\"query\":{\"match\":{\"msg\":\"merge\"}},\"size\":5}");
            assertEquals(200, got.status());
            final List<String> ids = hitIds(got.body());
            assertEquals("size is the whole search's, not each shard's: " + got.body(), 5, ids.size());
            assertEquals("the best match must be first, across shards: " + got.body(), "best", ids.get(0));
            assertTrue("the total must count every match, not just the page: " + got.body(), got.body().contains("\"value\":13"));
        }
    }

    /**
     * {@code from} pages through the merged result, and each shard is asked for enough to fill it.
     *
     * <p><b>Every document here routes to one shard, deliberately.</b> A first version spread ten
     * documents over three shards and asserted two disjoint pages — and passed with each shard asked for
     * {@code size} instead of {@code from + size}, because no shard held more than a page's worth and
     * truncating it lost nothing. The window only matters when one shard could supply the whole page, so
     * the fixture puts every candidate on one.
     *
     * <p>Asserted as sets rather than as an order: the claim is that the second page exists and does not
     * repeat the first, which does not depend on how ten near-identical documents happen to score.
     */
    public void testFromPagesThroughTheMergedResult() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, 3);
        try (ServerlessNode node = started("query-from", plane, 3)) {
            final var descriptor = plane.describe("alpha").orElseThrow();
            final List<String> onOneShard = idsRoutingTo(descriptor, 0, 10);
            for (String id : onOneShard) {
                index(node, id, "{\"msg\":\"paged\",\"n\":1,\"tag\":\"t\"}");
            }

            final List<String> firstPage = hitIds(
                send(node, "POST", "/alpha/_search", "{\"query\":{\"match\":{\"msg\":\"paged\"}},\"size\":4}").body()
            );
            final List<String> secondPage = hitIds(
                send(node, "POST", "/alpha/_search", "{\"query\":{\"match\":{\"msg\":\"paged\"}},\"size\":4,\"from\":4}").body()
            );
            assertEquals("a page is size hits", 4, firstPage.size());
            assertEquals("and so is the next one -- an empty second page means the shards were asked for too few", 4, secondPage.size());
            final List<String> both = new ArrayList<>(firstPage);
            both.retainAll(secondPage);
            assertEquals("the second page must not repeat the first: " + firstPage + " then " + secondPage, List.of(), both);
        }
    }

    /** Ids that all route to one shard, so a test can put a whole page in one place. */
    private static List<String> idsRoutingTo(IndexDescriptor descriptor, int shard, int howMany) {
        final List<String> found = new ArrayList<>();
        for (int i = 0; found.size() < howMany; i++) {
            final String candidate = "aimed-" + i;
            if (org.opensearch.serverless.rest.DocumentRouting.shardFor(descriptor, candidate) == shard) {
                found.add(candidate);
            }
        }
        return found;
    }

    /**
     * What cannot be merged is refused, not ignored.
     *
     * <p>An aggregation parses and runs perfectly well on a single shard. Returning one shard's buckets,
     * or three shards' buckets concatenated, as if either were the index's answer would be a number that
     * is wrong and looks right — which is the failure mode D2's 501s exist to prevent.
     */
    public void testWhatCannotBeMergedIsRefusedRatherThanIgnored() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, 2);
        try (ServerlessNode node = started("query-refuse", plane, 2)) {
            index(node, "1", "{\"msg\":\"refuse\",\"n\":1,\"tag\":\"a\"}");

            final Response aggregated = send(
                node,
                "POST",
                "/alpha/_search",
                "{\"query\":{\"match_all\":{}},\"aggs\":{\"tags\":{\"terms\":{\"field\":\"tag\"}}}}"
            );
            assertEquals("an aggregation must be refused, not silently dropped: " + aggregated.body(), 501, aggregated.status());
            assertTrue("and named: " + aggregated.body(), aggregated.body().contains("unsupported_search"));
            assertTrue("and explained: " + aggregated.body(), aggregated.body().contains("aggregations"));

            final Response sorted = send(node, "POST", "/alpha/_search", "{\"query\":{\"match_all\":{}},\"sort\":[{\"n\":\"asc\"}]}");
            assertEquals("a sort must be refused while the merge is by score: " + sorted.body(), 501, sorted.status());
        }
    }

    /** A malformed body is a 400 that says so, not a 500 and not an empty result. */
    public void testAMalformedBodyIsABadRequest() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, 1);
        try (ServerlessNode node = started("query-bad", plane, 1)) {
            final Response got = send(node, "POST", "/alpha/_search", "{\"query\":{\"no_such_query\":{}}}");
            assertEquals("an unparseable query must be a 400: " + got.body(), 400, got.status());
            assertTrue("and say it could not be parsed: " + got.body(), got.body().contains("bad_query"));
        }
    }

    /** The shorthand every existing caller uses still works, and still means what it meant. */
    public void testTheQueryParameterShorthandStillWorks() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, 1);
        try (ServerlessNode node = started("query-shorthand", plane, 1)) {
            index(node, "1", "{\"msg\":\"shorthand\",\"n\":1,\"tag\":\"t\"}");
            final Response got = send(node, "GET", "/alpha/_search?q=msg:shorthand", null);
            assertEquals(200, got.status());
            assertEquals(List.of("1"), hitIds(got.body()));
        }
    }

    /**
     * A document comes back as an object, not as a string that has to be unescaped first.
     *
     * <p>The source used to be written with {@code builder.field(String, String)}, so a hit's
     * {@code _source} was the whole document as one escaped JSON string. Every client had to parse it a
     * second time, and the response was not the shape any OpenSearch client expects.
     */
    public void testAHitsSourceIsAnObject() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, 1);
        try (ServerlessNode node = started("query-source", plane, 1)) {
            index(node, "1", "{\"msg\":\"objectified\",\"n\":7,\"tag\":\"t\"}");
            final Response got = send(node, "GET", "/alpha/_search?q=msg:objectified", null);
            assertTrue("the source must be an object: " + got.body(), got.body().contains("\"_source\":{\"msg\":\"objectified\""));
            assertFalse("and not an escaped string: " + got.body(), got.body().contains("\"_source\":\"{"));
            assertTrue("and a hit should carry its score: " + got.body(), got.body().contains("\"_score\":"));
        }
    }

    private void index(ServerlessNode node, String id, String source) throws Exception {
        assertEquals(201, send(node, "PUT", "/alpha/_doc/" + id + "?refresh=true", source).status());
    }

    /** The ids in a response, in the order the response put them. */
    private static List<String> hitIds(String body) {
        final List<String> ids = new ArrayList<>();
        final Matcher matcher = Pattern.compile("\"_id\":\"([^\"]+)\"").matcher(body);
        while (matcher.find()) {
            ids.add(matcher.group(1));
        }
        return ids;
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest.BodyPublisher payload = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .method(method, payload)
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }

    /**
     * The shards of one search run at the same time, not one after another.
     *
     * <p><b>Asserted as a rendezvous, not as a stopwatch.</b> Each shard's first read blocks until every
     * shard has arrived; sequential fan-out therefore cannot get past the first one, and concurrent
     * fan-out cannot fail. There is no threshold to tune — see {@link RendezvousBlobStore} for why timing
     * this instead would have been a coin flip.
     *
     * <p>The shards have to be opened from the object store for there to be a read to meet on, so nobody
     * owns them and the searching node carries the {@code search} role, which is the scale-to-zero path.
     */
    public void testTheShardsOfOneSearchRunAtTheSameTime() throws Exception {
        final int shards = 3;
        final org.opensearch.common.blobstore.fs.FsBlobStore disk = new FsBlobStore(1024, createTempDir(), false);
        // Generous: the point is that a sequential fan-out cannot finish at all, not that it is slow.
        final RendezvousBlobStore store = new RendezvousBlobStore(disk, shards, 5_000L);
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", shards, MAPPING, null));

        final ServerlessNode writer = new ServerlessNode(nodeSettings("fanout-writer"));
        writer.start();
        writer.setMetadataPlane(plane);
        final BackgroundReconciler loop = new BackgroundReconciler(writer, plane);
        for (int shard = 0; shard < shards; shard++) {
            loop.want("alpha", shard);
        }
        loop.tick(clock.get());
        for (int i = 0; i < 30; i++) {
            index(writer, "f" + i, "{\"msg\":\"fanout\",\"n\":" + i + ",\"tag\":\"t\"}");
        }
        loop.tick(clock.get() + 1_000);   // publish, so there is something for a reader to open
        for (int shard = 0; shard < shards; shard++) {
            assertTrue(plane.heads().release("alpha", shard, writer.localNode().getId()));
        }
        writer.close();

        try (ServerlessNode reader = new ServerlessNode(searchNodeSettings("fanout-reader"))) {
            reader.start();
            reader.setMetadataPlane(plane);
            final Response got = send(reader, "POST", "/alpha/_search", "{\"query\":{\"match\":{\"msg\":\"fanout\"}},\"size\":5}");
            assertEquals("the search should have been answered: " + got.body(), 200, got.status());
            assertTrue(
                "every shard must have answered -- a sequential fan-out leaves the later ones stuck at the rendezvous: " + got.body(),
                got.body().contains("\"complete\":true")
            );
            assertTrue("and the shards must actually have met: they did not all arrive together", store.everyoneArrived());
            assertTrue("the search must still return results: " + got.body(), got.body().contains("\"value\":30"));
        }
    }

    private Settings searchNodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-query")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "search")
            .build();
    }
}
