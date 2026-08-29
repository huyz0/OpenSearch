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
import org.opensearch.core.index.shard.ShardId;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@code GET /{index}/_doc/{id}} — reading back what was written.
 *
 * <p>The tests that matter here are about <b>which copy answers</b>. A get is routed to the shard's
 * owner, the opposite of a search, because a write is acknowledged before it is searchable and long
 * before it is published; and when nobody owns the shard, a published commit is served instead, because
 * with no writer there is nothing newer than the commit. The interesting case is the third one — an owner
 * that exists and cannot be reached — where serving the commit would be a wrong answer that looks right.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessGetTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-get")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private MetadataPlane freshIndex(AtomicLong clock, java.nio.file.Path dir, int shards) throws Exception {
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, dir, false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", shards, MAPPING, null));
        return plane;
    }

    /**
     * The property that decides the whole design: a get sees a write that a search cannot.
     *
     * <p>Written without {@code refresh}, so the document is in the log and in the engine and in no
     * segment anybody can search. A get routed like a search would answer "not found" for a document the
     * caller was just told was written, which from outside is indistinguishable from data loss. The
     * search is asserted alongside it, because "the get found it" only means something next to "and the
     * search did not".
     */
    public void testAGetSeesAWriteThatASearchCannotYet() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("get-realtime"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            // Note the absence of ?refresh=true. That absence is the test.
            assertEquals(201, send(node, "PUT", "/alpha/_doc/1", "{\"msg\":\"realtime\",\"n\":1}").status());

            final Response search = send(node, "GET", "/alpha/_search?q=msg:realtime", null);
            assertEquals(200, search.status());
            assertTrue(
                "the fixture is meaningless unless the write really is unsearchable: " + search.body(),
                search.body().contains("\"value\":0")
            );

            final Response got = send(node, "GET", "/alpha/_doc/1", null);
            assertEquals("a get must find an acknowledged write before it is searchable: " + got.body(), 200, got.status());
            assertTrue("and report it as found: " + got.body(), got.body().contains("\"found\":true"));
            assertTrue("and return its source: " + got.body(), got.body().contains("\"msg\":\"realtime\""));
            assertTrue("and say it answered in realtime: " + got.body(), got.body().contains("\"realtime\":true"));
        }
    }

    /** A document that was never written is a 404 that says so, not an empty 200. */
    public void testAMissingDocumentIsNotFound() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("get-missing"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            final Response got = send(node, "GET", "/alpha/_doc/nothing-here", null);
            assertEquals("absent must be 404, never an empty 200: " + got.body(), 404, got.status());
            assertTrue("and say found:false rather than nothing at all: " + got.body(), got.body().contains("\"found\":false"));

            final Response head = send(node, "HEAD", "/alpha/_doc/nothing-here", null);
            assertEquals("HEAD must agree with GET", 404, head.status());
        }
    }

    /** A deleted document stops being found, immediately, without waiting for a refresh. */
    public void testADeletedDocumentIsNoLongerFound() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("get-deleted"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            assertEquals(201, send(node, "PUT", "/alpha/_doc/1", "{\"msg\":\"gone\",\"n\":1}").status());
            assertEquals(200, send(node, "GET", "/alpha/_doc/1", null).status());
            assertEquals(200, send(node, "DELETE", "/alpha/_doc/1", null).status());

            final Response got = send(node, "GET", "/alpha/_doc/1", null);
            assertEquals("a deleted document must not be returned: " + got.body(), 404, got.status());
        }
    }

    /**
     * A get sent to a node that does not own the shard is answered by the node that does.
     *
     * <p>Not refreshed, again deliberately: the forwarded read has to be realtime on the far side too, or
     * forwarding it was pointless.
     */
    public void testAGetIsAnsweredByTheShardOwner() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 2);

        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("get-fwd-a"));
            ServerlessNode b = new ServerlessNode(nodeSettings("get-fwd-b"))
        ) {
            a.start();
            b.start();
            a.setMetadataPlane(plane);
            b.setMetadataPlane(plane);
            final BackgroundReconciler loopA = new BackgroundReconciler(a, plane);
            loopA.want("alpha", 0);
            loopA.tick(clock.get());
            final BackgroundReconciler loopB = new BackgroundReconciler(b, plane);
            loopB.want("alpha", 1);
            loopB.tick(clock.get());

            final var descriptor = plane.describe("alpha").orElseThrow();
            final String remote = idRoutingTo(descriptor, 1);
            assertTrue(
                "the fixture must target a shard node a does not hold",
                a.reconciler().openShards().stream().noneMatch(shard -> shard.id() == 1)
            );

            // Written through a, which forwards it to b; then read through a, which must do the same.
            assertEquals(201, send(a, "PUT", "/alpha/_doc/" + remote, "{\"msg\":\"forwarded\",\"n\":7}").status());

            final Response got = send(a, "GET", "/alpha/_doc/" + remote, null);
            assertEquals("a get for a remote shard must be forwarded, not refused: " + got.body(), 200, got.status());
            assertTrue("and find the document: " + got.body(), got.body().contains("\"msg\":\"forwarded\""));
            assertTrue("and name the owner that answered: " + got.body(), got.body().contains(b.localNode().getId()));
            assertTrue("and still be realtime on the far side: " + got.body(), got.body().contains("\"realtime\":true"));
        }
    }

    /**
     * With nobody owning the shard, a get is served from the published commit — which is what makes it
     * work against an index that has scaled to zero.
     *
     * <p>The writer publishes, shuts down cleanly and releases its lease, so the head names nobody. A
     * fresh node with its own empty data directory can only answer from the object store.
     */
    public void testAGetServesThePublishedCommitWhenNobodyOwnsTheShard() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final java.nio.file.Path objectStore = createTempDir();
        final MetadataPlane plane = freshIndex(clock, objectStore, 1);

        final ServerlessNode writer = new ServerlessNode(nodeSettings("get-zero-writer"));
        writer.start();
        writer.setMetadataPlane(plane);
        final BackgroundReconciler loop = new BackgroundReconciler(writer, plane);
        loop.want("alpha", 0);
        loop.tick(clock.get());
        assertEquals(201, send(writer, "PUT", "/alpha/_doc/1", "{\"msg\":\"published\",\"n\":1}").status());
        assertEquals(
            "the document must actually be published, or there is nothing to serve",
            1,
            loop.tick(clock.get() + 1_000).published().size()
        );
        final ShardId held = writer.reconciler().openShards().iterator().next();
        // A clean handover: the head is released, so nothing names an owner.
        assertTrue("the writer should be able to release the head it holds", plane.heads().release("alpha", 0, writer.localNode().getId()));
        writer.releaseShard(held, "test: scaling to zero");
        writer.close();

        assertTrue("nobody should own the shard now", plane.heads().read("alpha", 0).map(h -> h.ownerNodeId()).isEmpty());

        try (ServerlessNode reader = new ServerlessNode(nodeSettings("get-zero-reader"))) {
            reader.start();
            reader.setMetadataPlane(plane);
            final Response got = send(reader, "GET", "/alpha/_doc/1", null);
            assertEquals("a get against an unowned shard must be served, not refused: " + got.body(), 200, got.status());
            assertTrue("and find the published document: " + got.body(), got.body().contains("\"msg\":\"published\""));
            // Said plainly, because it is the one case where the answer excludes unpublished writes -- and
            // here that set is empty, which is exactly why the answer is still correct.
            assertTrue("and admit it is not realtime: " + got.body(), got.body().contains("\"realtime\":false"));
        }
    }

    /**
     * When the shard has an owner that cannot be reached, a get refuses rather than serving a stale copy.
     *
     * <p><b>This is the case the design is actually about.</b> A published commit is sitting right there
     * and would answer instantly — and would be wrong, because a live writer holds writes the commit does
     * not, so the answer would be a stale document, or a 404 for a document that exists, reported as
     * success. A 503 is the true answer.
     *
     * <p>Staged so that the fallback would visibly produce the wrong result: the document is written and
     * <em>not</em> published, and the owner is then made unreachable. Serving the commit finds nothing.
     */
    public void testAGetRefusesRatherThanServeStaleDataWhenTheOwnerIsUnreachable() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final java.nio.file.Path objectStore = createTempDir();
        final MetadataPlane plane = freshIndex(clock, objectStore, 1);

        final ServerlessNode owner = new ServerlessNode(nodeSettings("get-stale-owner"));
        owner.start();
        owner.setMetadataPlane(plane);
        final BackgroundReconciler loop = new BackgroundReconciler(owner, plane);
        loop.want("alpha", 0);
        loop.tick(clock.get());

        // Published once, so a commit exists for a fallback to find...
        assertEquals(201, send(owner, "PUT", "/alpha/_doc/old", "{\"msg\":\"stale\",\"n\":1}").status());
        assertEquals(1, loop.tick(clock.get() + 1_000).published().size());
        // ...and then a write that only the owner knows about.
        assertEquals(201, send(owner, "PUT", "/alpha/_doc/fresh", "{\"msg\":\"stale\",\"n\":2}").status());

        // The owner dies without releasing: the head still names it, and its lease has not yet expired,
        // but nothing answers on its transport address.
        owner.close();

        try (ServerlessNode other = new ServerlessNode(nodeSettings("get-stale-other"))) {
            other.start();
            other.setMetadataPlane(plane);
            assertEquals(
                "the head must still name the dead owner, or this tests nothing",
                true,
                plane.heads().read("alpha", 0).map(h -> h.ownerNodeId()).isPresent()
            );

            // There are TWO ways an owner can be out of reach, they take different branches, and a test
            // that produced only one of them left the other free to serve stale data unnoticed -- which is
            // what a planted fallback proved, by passing, before this was split.

            // One: the lease is still inside its TTL, so the owner looks alive and the connection fails.
            final Response justDied = send(other, "GET", "/alpha/_doc/fresh", null);
            assertNotEquals(
                "a document the owner holds and the commit does not must never be reported absent: " + justDied.body(),
                404,
                justDied.status()
            );
            assertEquals("an owner that cannot be reached is a retry, not an answer: " + justDied.body(), 503, justDied.status());
            assertTrue("and should say the forward failed: " + justDied.body(), justDied.body().contains("forward_failed"));

            // Two: the lease has expired, so there is no address to try at all. Same answer required.
            clock.set(1_000L + TTL + 1);
            final Response leaseGone = send(other, "GET", "/alpha/_doc/fresh", null);
            assertEquals(
                "an owner with no live lease is still not an excuse to serve a commit: " + leaseGone.body(),
                503,
                leaseGone.status()
            );
            assertTrue("and should say the owner is unreachable: " + leaseGone.body(), leaseGone.body().contains("owner_unreachable"));

            // And the same for a document the commit DOES have: answering it from the commit would look
            // perfectly correct here, which is exactly why the rule cannot be "fall back when it works".
            final Response stale = send(other, "GET", "/alpha/_doc/old", null);
            assertEquals(
                "a stale copy must not be served just because it happens to have the document: " + stale.body(),
                503,
                stale.status()
            );
        }
    }

    private static String idRoutingTo(IndexDescriptor descriptor, int shard) {
        for (int i = 0;; i++) {
            final String candidate = "aimed-" + i;
            if (org.opensearch.serverless.rest.DocumentRouting.shardFor(descriptor, candidate) == shard) {
                return candidate;
            }
        }
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
}
