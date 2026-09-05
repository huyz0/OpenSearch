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
 * {@code POST /{index}/_delete_by_query} — every document a query matches, removed.
 *
 * <p>See {@link org.opensearch.serverless.rest.DeleteByQueryHandler} for why matching is frozen and
 * deleting is not, and why it walks one shard at a time by {@code _doc} order rather than through the
 * cross-shard {@code search_after} merge {@code _search} uses.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessDeleteByQueryTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"},"
        + "\"keep\":{\"type\":\"boolean\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-delete-by-query")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private MetadataPlane planeOver(java.nio.file.Path dir, AtomicLong clock) throws Exception {
        return new MetadataPlane(new FsBlobStore(1024, dir, false), BlobPath.cleanPath(), clock::get, TTL);
    }

    private record Response(int status, String body) {
    }

    private static Response send(TransportAddress address, String method, String path, String body) throws Exception {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }

    private ServerlessNode started(MetadataPlane plane, String name) throws Exception {
        final ServerlessNode node = new ServerlessNode(nodeSettings(name));
        node.start();
        node.setMetadataPlane(plane);
        return node;
    }

    /**
     * Publishes every open shard of an index, so there is a commit for a point in time to freeze.
     *
     * <p>{@code refresh=true} on a write makes it searchable on the live shard; it does not publish it.
     * A point in time — which is what delete_by_query freezes — can only be taken over what has been
     * published, the same rule {@code _pit} itself enforces. Every test here has to publish explicitly for
     * exactly that reason.
     */
    private void publish(ServerlessNode node, MetadataPlane plane, String index, int shardCount) throws Exception {
        for (int shard = 0; shard < shardCount; shard++) {
            final int number = shard;
            final var shardId = node.reconciler()
                .openShards()
                .stream()
                .filter(s -> s.getIndexName().equals(index) && s.id() == number)
                .findFirst()
                .orElseThrow();
            final long term = plane.heads().read(index, shard).orElseThrow().term();
            node.publishShard(shardId, term);
        }
    }

    /**
     * Matching documents are removed, and everything else survives -- checked over several shards, which
     * is the case that actually exercises the per-shard walk rather than one shard answering everything.
     */
    public void testMatchingDocumentsAreDeletedAcrossSeveralShards() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "dbq-basic")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=3", MAPPING);
            node.activateWriter(plane, "alpha", 0);
            node.activateWriter(plane, "alpha", 1);
            node.activateWriter(plane, "alpha", 2);

            for (int i = 1; i <= 30; i++) {
                final boolean keep = i % 3 == 0;
                assertEquals(
                    201,
                    send(http, "PUT", "/alpha/_doc/d" + i + "?refresh=true", "{\"msg\":\"x\",\"n\":" + i + ",\"keep\":" + keep + "}")
                        .status()
                );
            }
            publish(node, plane, "alpha", 3);

            final Response result = send(http, "POST", "/alpha/_delete_by_query?refresh=true", "{\"query\":{\"term\":{\"keep\":false}}}");
            assertEquals(result.body(), 200, result.status());
            assertTrue("20 of 30 must have matched: " + result.body(), result.body().contains("\"matched\":20"));
            assertTrue("and all 20 deleted: " + result.body(), result.body().contains("\"deleted\":20"));

            final Response search = send(http, "POST", "/alpha/_search", "{\"size\":30,\"query\":{\"match_all\":{}}}");
            assertTrue("only the kept 10 must remain: " + search.body(), search.body().contains("\"value\":10"));
            for (int i = 1; i <= 30; i++) {
                final boolean shouldRemain = i % 3 == 0;
                assertEquals(
                    "d" + i + " must " + (shouldRemain ? "remain" : "be gone"),
                    shouldRemain ? 200 : 404,
                    send(http, "GET", "/alpha/_doc/d" + i, null).status()
                );
            }
        }
    }

    /**
     * More matches than one page, on one shard -- this is the assertion the per-shard walk stands on.
     *
     * <p>The handler's own batch size is 1,000 and is package-private; 2,137 is deliberately more than
     * twice that rather than a number derived from it, so this does not need to track the constant to stay
     * meaningful. One page's worth of matches would already prove the walk fetches at least one page; well
     * over two proves it actually continues with {@code search_after} rather than silently stopping at the
     * first page.
     */
    public void testMoreThanOnePageOfMatchesAreAllDeleted() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "dbq-paged")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activateWriter(plane, "alpha", 0);

            final int documents = 2137;
            for (int i = 1; i <= documents; i++) {
                node.index(node.reconciler().openShards().iterator().next(), "d" + i, "{\"msg\":\"x\",\"n\":" + i + ",\"keep\":false}");
            }
            node.reconciler().shard(node.reconciler().openShards().iterator().next()).refresh("dbq-paged");
            publish(node, plane, "alpha", 1);

            final Response result = send(http, "POST", "/alpha/_delete_by_query?refresh=true", "{\"query\":{\"match_all\":{}}}");
            assertEquals(result.body(), 200, result.status());
            assertTrue(
                "every one of " + documents + " matches must be walked, not just the first page: " + result.body(),
                result.body().contains("\"matched\":" + documents)
            );
            assertTrue(result.body(), result.body().contains("\"deleted\":" + documents));

            final Response search = send(http, "POST", "/alpha/_search", "{\"query\":{\"match_all\":{}}}");
            assertTrue("nothing must remain: " + search.body(), search.body().contains("\"value\":0"));
        }
    }

    /** A query matching nothing deletes nothing, and says so rather than erroring. */
    public void testAQueryMatchingNothingDeletesNothing() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "dbq-nothing")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activateWriter(plane, "alpha", 0);
            send(http, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"x\",\"n\":1,\"keep\":true}");
            publish(node, plane, "alpha", 1);

            final Response result = send(http, "POST", "/alpha/_delete_by_query", "{\"query\":{\"term\":{\"keep\":false}}}");
            assertEquals(result.body(), 200, result.status());
            assertTrue(result.body(), result.body().contains("\"matched\":0"));
            assertEquals(200, send(http, "GET", "/alpha/_doc/1", null).status());
        }
    }

    /**
     * A write acknowledged but not yet published is invisible to the frozen view, so it survives a
     * delete_by_query that would have matched it.
     *
     * <p>This is the real, specific cost of freezing what has been published rather than what has been
     * written, stated in the class javadoc and made concrete here: a document written and refreshed —
     * searchable to a live query right now — is not touched by a delete_by_query running at that same
     * moment if the writer has not yet published it. That is not a bug in the operation; it is the same
     * boundary a plain {@code search_after} export of matching ids would have, and a caller relying on
     * delete_by_query to remove everything a live search can currently find should know it is not that.
     */
    public void testAnUnpublishedWriteSurvivesEvenThoughItMatches() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "dbq-frozen")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activateWriter(plane, "alpha", 0);

            // Published: the frozen view will see this one.
            send(http, "PUT", "/alpha/_doc/published?refresh=true", "{\"msg\":\"x\",\"n\":1,\"keep\":false}");
            publish(node, plane, "alpha", 1);

            // Written and refreshed after -- searchable right now, on the live shard -- but never
            // published, so the point in time this call freezes never saw it.
            send(http, "PUT", "/alpha/_doc/unpublished?refresh=true", "{\"msg\":\"x\",\"n\":2,\"keep\":false}");
            final Response liveSearch = send(http, "POST", "/alpha/_search", "{\"query\":{\"term\":{\"keep\":false}}}");
            assertTrue(
                "this test is meaningless unless a live search can already find both: " + liveSearch.body(),
                liveSearch.body().contains("\"value\":2")
            );

            final Response result = send(http, "POST", "/alpha/_delete_by_query", "{\"query\":{\"term\":{\"keep\":false}}}");
            assertEquals(result.body(), 200, result.status());
            assertTrue("only the published document was in the frozen view: " + result.body(), result.body().contains("\"matched\":1"));

            assertEquals("the published match must be gone", 404, send(http, "GET", "/alpha/_doc/published", null).status());
            assertEquals(
                "the unpublished match must survive -- the view never saw it",
                200,
                send(http, "GET", "/alpha/_doc/unpublished", null).status()
            );
        }
    }

    /** {@code max_docs} bounds how many matches are visited, as a safety valve rather than an all-or-nothing. */
    public void testMaxDocsBoundsHowManyAreDeleted() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "dbq-maxdocs")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activateWriter(plane, "alpha", 0);
            for (int i = 1; i <= 10; i++) {
                send(http, "PUT", "/alpha/_doc/d" + i + "?refresh=true", "{\"msg\":\"x\",\"n\":" + i + ",\"keep\":false}");
            }
            publish(node, plane, "alpha", 1);

            final Response result = send(http, "POST", "/alpha/_delete_by_query?max_docs=4&refresh=true", "{\"query\":{\"match_all\":{}}}");
            assertEquals(result.body(), 200, result.status());
            assertTrue(result.body(), result.body().contains("\"matched\":4"));

            final Response search = send(http, "POST", "/alpha/_search", "{\"size\":10,\"query\":{\"match_all\":{}}}");
            assertTrue("6 of the 10 must remain: " + search.body(), search.body().contains("\"value\":6"));
        }
    }

    /** No query at all is refused; there is no implicit match_all. */
    public void testAMissingQueryIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "dbq-noquery")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activateWriter(plane, "alpha", 0);
            send(http, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"x\",\"n\":1,\"keep\":true}");

            assertEquals(400, send(http, "POST", "/alpha/_delete_by_query", "{}").status());
            assertEquals(200, send(http, "GET", "/alpha/_doc/1", null).status());
        }
    }

    /** A sort in the body is refused, not silently overridden by the internal one. */
    public void testASortInTheBodyIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "dbq-sort")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activateWriter(plane, "alpha", 0);

            final Response refused = send(
                http,
                "POST",
                "/alpha/_delete_by_query",
                "{\"query\":{\"match_all\":{}},\"sort\":[{\"n\":\"asc\"}]}"
            );
            assertEquals(refused.body(), 501, refused.status());
            assertTrue(refused.body(), refused.body().contains("sort"));
        }
    }

    /** No index at all is a plain miss, not an error. */
    public void testAMissingIndexIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "dbq-missing-index")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            final Response refused = send(http, "POST", "/ghost/_delete_by_query", "{\"query\":{\"match_all\":{}}}");
            assertEquals(404, refused.status());
        }
    }

    /**
     * The frozen shards are released once the operation finishes, not left open.
     *
     * <p>Every point-in-time this node opens for the operation must be closed by the end of it, win or
     * lose -- the same discipline {@code PointInTimeHandler}'s own release path follows.
     */
    public void testTheFrozenViewIsReleasedWhenDone() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "dbq-release")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activateWriter(plane, "alpha", 0);
            send(http, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"x\",\"n\":1,\"keep\":false}");
            publish(node, plane, "alpha", 1);

            send(http, "POST", "/alpha/_delete_by_query", "{\"query\":{\"match_all\":{}}}");

            assertTrue("no frozen views must be left open on this node", node.reconciler().frozenShards().isEmpty());
            assertTrue("and the record itself must be gone", plane.livePointsInTime(clock.get()).isEmpty());
        }
    }
}
