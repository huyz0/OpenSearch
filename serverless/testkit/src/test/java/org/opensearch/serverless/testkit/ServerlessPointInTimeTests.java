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
import org.opensearch.serverless.reconcile.GarbageCollector;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A frozen view of an index, and the promise that its bytes survive.
 *
 * <p>{@code search_after} pages through a result set and the index moves between pages. A point in time
 * makes both pages read the same commits, which is the difference between exporting a result set and
 * exporting whatever happened to be there each time you asked.
 *
 * <p><b>The hard half is not freezing, it is the garbage collector.</b> The sweep deletes a blob when the
 * live commit does not name it and it belongs to a dead term — which is exactly what a frozen view's files
 * become a moment after the writer publishes again. A view whose files were swept would dissolve under a
 * paging caller and look like corruption. So the test that matters here is the one that publishes over a
 * view, sweeps, and then reads it anyway.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessPointInTimeTests extends OpenSearchTestCase {

    private static final long TTL = 300_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"rank\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-pit")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** A view keeps showing what it froze while the index moves on. */
    public void testAFrozenViewDoesNotSeeLaterWrites() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("frozen", "uuid-frozen-0000000", 2, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("pit-basic"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "frozen", 2);

            for (int i = 1; i <= 4; i++) {
                assertEquals(201, send(node, "PUT", "/frozen/_doc/a" + i + "?refresh=true", body(i)).status());
            }
            loop.tick(clock.get());   // publish, so there is a commit to freeze

            final Response taken = send(node, "POST", "/frozen/_pit?keep_alive=10m", null);
            assertEquals(taken.body(), 200, taken.status());
            final String pit = field(taken.body(), "pit_id");

            // The index moves on, and is published, so the frozen commit is genuinely superseded.
            for (int i = 5; i <= 8; i++) {
                assertEquals(201, send(node, "PUT", "/frozen/_doc/a" + i + "?refresh=true", body(i)).status());
            }
            loop.tick(clock.get() + 1);

            final Response live = send(node, "POST", "/frozen/_search", "{\"size\":20,\"query\":{\"match_all\":{}}}");
            assertTrue("a live search must see everything: " + live.body(), live.body().contains("\"value\":8"));

            final Response frozen = send(node, "POST", "/frozen/_search?pit=" + pit, "{\"size\":20,\"query\":{\"match_all\":{}}}");
            assertEquals(frozen.body(), 200, frozen.status());
            assertTrue("and the view only what it froze: " + frozen.body(), frozen.body().contains("\"value\":4"));
            assertTrue("over every shard it froze: " + frozen.body(), frozen.body().contains("\"complete\":true"));
        }
    }

    /**
     * The collector does not delete what a view is holding.
     *
     * <p>The assertion this whole feature stands on. Without it a frozen view is a promise the sweep never
     * heard, and a caller paging through one would find it dissolving halfway — a failure that would look
     * like corruption rather than like a deletion.
     */
    public void testASweepDoesNotCollectWhatAViewIsHolding() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("swept", "uuid-swept-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("pit-sweep"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "swept", 1);

            for (int i = 1; i <= 4; i++) {
                assertEquals(201, send(node, "PUT", "/swept/_doc/s" + i + "?refresh=true", body(i)).status());
            }
            loop.tick(clock.get());

            final String pit = field(send(node, "POST", "/swept/_pit?keep_alive=10m", null).body(), "pit_id");

            // Move the shard to a new term and publish there, so the frozen commit's files are in a dead
            // term container and unreferenced by the live manifest -- the exact condition the sweep deletes.
            assertTrue(plane.heads().release("swept", 0, node.localNode().getId()));
            node.activateWriter(plane, "swept", 0);
            for (int i = 5; i <= 12; i++) {
                assertEquals(201, send(node, "PUT", "/swept/_doc/s" + i + "?refresh=true", body(i)).status());
            }
            loop.tick(clock.get() + 1);

            final var swept = new GarbageCollector(store, BlobPath.cleanPath()).collectShard(plane, "swept", 0);
            logger.info("pit: the sweep collected {} blobs with a view held", swept.size());

            // And the view still reads.
            final Response frozen = send(node, "POST", "/swept/_search?pit=" + pit, "{\"size\":20,\"query\":{\"match_all\":{}}}");
            assertEquals("the view must still be readable after a sweep: " + frozen.body(), 200, frozen.status());
            assertTrue("and still show what it froze: " + frozen.body(), frozen.body().contains("\"value\":4"));
        }
    }

    /** Paging a frozen view sees a consistent result set, which is what it is for. */
    public void testPagingAFrozenViewIsConsistent() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("paged", "uuid-paged-00000000", 2, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("pit-paging"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "paged", 2);

            for (int rank = 1; rank <= 6; rank++) {
                assertEquals(201, send(node, "PUT", "/paged/_doc/p" + rank + "?refresh=true", body(rank)).status());
            }
            loop.tick(clock.get());
            final String pit = field(send(node, "POST", "/paged/_pit?keep_alive=10m", null).body(), "pit_id");

            final Response first = send(node, "POST", "/paged/_search?pit=" + pit, "{\"size\":3,\"sort\":[{\"rank\":\"asc\"}]}");
            assertEquals(java.util.List.of(1L, 2L, 3L), ranksIn(first.body()));

            // A view has the name of the index it is a view of, and every lookup on the write path finds a
            // shard by name and number. So the two must not be in the same set: the node is serving two
            // shards of "paged" and holding two views of them, and a write asking for shard 0 of "paged"
            // must find the shard it has been writing to rather than a reader that happens to share the
            // name. Asserted rather than left to the writes below, which caught it only when a set
            // iterated the wrong one first.
            assertEquals("the views must not be counted among the shards this node serves", 2, node.reconciler().openShards().size());
            for (var shardId : node.reconciler().openShards()) {
                assertEquals("uuid-paged-00000000", shardId.getIndex().getUUID());
            }
            assertEquals("and both views are held", 2, node.reconciler().frozenShards().size());

            // Documents arrive between the pages, and are published. A live paged search would shift; this
            // must not.
            for (int rank = 7; rank <= 12; rank++) {
                final Response written = send(node, "PUT", "/paged/_doc/p" + rank + "?refresh=true", body(rank));
                assertEquals(written.body(), 201, written.status());
            }
            loop.tick(clock.get() + 1);

            final Response second = send(
                node,
                "POST",
                "/paged/_search?pit=" + pit,
                "{\"size\":3,\"sort\":[{\"rank\":\"asc\"}],\"search_after\":[3]}"
            );
            assertEquals("page two must continue the page one the view showed", java.util.List.of(4L, 5L, 6L), ranksIn(second.body()));

            final Response third = send(
                node,
                "POST",
                "/paged/_search?pit=" + pit,
                "{\"size\":3,\"sort\":[{\"rank\":\"asc\"}],\"search_after\":[6]}"
            );
            assertTrue("and the view ends where it was frozen: " + third.body(), ranksIn(third.body()).isEmpty());
        }
    }

    /** A released or expired view is gone, and says so rather than answering emptily. */
    public void testAViewCanBeReleasedAndExpires() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("brief", "uuid-brief-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("pit-release"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "brief", 1);
            assertEquals(201, send(node, "PUT", "/brief/_doc/1?refresh=true", body(1)).status());
            loop.tick(clock.get());

            final String released = field(send(node, "POST", "/brief/_pit?keep_alive=10m", null).body(), "pit_id");
            assertEquals(200, send(node, "POST", "/brief/_search?pit=" + released, "{\"query\":{\"match_all\":{}}}").status());
            assertEquals(200, send(node, "DELETE", "/_pit/" + released, null).status());
            final Response afterRelease = send(node, "POST", "/brief/_search?pit=" + released, "{\"query\":{\"match_all\":{}}}");
            assertEquals("a released view must not answer: " + afterRelease.body(), 404, afterRelease.status());
            assertEquals("and releasing it twice finds nothing", 404, send(node, "DELETE", "/_pit/" + released, null).status());

            // Expiry is by the clock, and a search does not extend it.
            final String expiring = field(send(node, "POST", "/brief/_pit?keep_alive=1m", null).body(), "pit_id");
            assertEquals(200, send(node, "POST", "/brief/_search?pit=" + expiring, "{\"query\":{\"match_all\":{}}}").status());
            clock.addAndGet(120_000L);
            final Response expired = send(node, "POST", "/brief/_search?pit=" + expiring, "{\"query\":{\"match_all\":{}}}");
            assertEquals("an expired view must not answer either: " + expired.body(), 404, expired.status());

            // And the collector reaps it, so its files stop being held.
            assertEquals("the expired view must be reaped", 1, plane.reapPointsInTime(clock.get()));
            assertTrue("and nothing is held any more", plane.livePointsInTime(clock.get()).isEmpty());
        }
    }

    /** A shard that has published nothing cannot be frozen, because a partial view is a wrong one. */
    public void testAnIndexWithNothingPublishedCannotBeFrozen() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("fresh", "uuid-fresh-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("pit-fresh"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, "fresh", 1);

            final Response tooSoon = send(node, "POST", "/fresh/_pit", null);
            assertEquals("nothing published, nothing to freeze: " + tooSoon.body(), 409, tooSoon.status());
            assertTrue(tooSoon.body().contains("published nothing yet"));
            assertEquals(404, send(node, "POST", "/absent/_pit", null).status());
        }
    }

    private BackgroundReconciler hold(ServerlessNode node, MetadataPlane plane, AtomicLong clock, String index, int shards)
        throws Exception {
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (int shard = 0; shard < shards; shard++) {
            loop.want(index, shard);
        }
        loop.tick(clock.get());
        return loop;
    }

    private static String body(int rank) {
        return "{\"msg\":\"doc\",\"rank\":" + rank + "}";
    }

    private static final Pattern RANK = Pattern.compile("\"rank\":(\\d+)");

    private static java.util.List<Long> ranksIn(String body) {
        final java.util.List<Long> ranks = new java.util.ArrayList<>();
        final Matcher matcher = RANK.matcher(body);
        while (matcher.find()) {
            ranks.add(Long.parseLong(matcher.group(1)));
        }
        return ranks;
    }

    private static String field(String body, String name) {
        final Matcher matcher = Pattern.compile("\"" + name + "\":\"([^\"]+)\"").matcher(body);
        assertTrue("no " + name + " in " + body, matcher.find());
        return java.net.URLEncoder.encode(matcher.group(1), StandardCharsets.UTF_8);
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
