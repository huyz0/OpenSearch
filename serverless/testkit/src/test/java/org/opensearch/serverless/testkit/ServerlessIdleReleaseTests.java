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
 * A node that lets go of what nobody is asking about.
 *
 * <p>Before this, a shard was released only when ownership was taken away — fenced, reassigned, or the
 * lease lost. Nothing ever released one voluntarily, so a node that answered a single query for an index
 * held that shard for the life of the process and {@code maxShardsHeld} was a refusal rather than an
 * eviction. Scale-to-zero was an operator killing the process.
 *
 * <p><b>The negative test is the important one.</b> A controller that releases shards is one bad
 * predicate away from being an availability bug, so "a shard in use is not released" carries more weight
 * here than "an idle one is".
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessIdleReleaseTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final long IDLE_AFTER = 60_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, String roles) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-idle")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .build();
    }

    private MetadataPlane freshIndex(AtomicLong clock, int shards) throws Exception {
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", shards, MAPPING, null));
        return plane;
    }

    /**
     * A shard nobody has touched is let go, its head with it, and the data is intact when it comes back.
     *
     * <p>Released and then re-acquired by the same node, which is the honest end-to-end shape: the point
     * of releasing is that taking it again is possible and cheap, not that it is gone.
     */
    public void testAnIdleShardIsReleasedAndItsDataSurvives() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("idle-one", "ingest"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setIdleAfterMillis(IDLE_AFTER);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();

            assertEquals(201, send(node, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"idle\",\"n\":1}").status());
            assertEquals(
                "the head must name this node before anything is released",
                node.localNode().getId(),
                plane.heads().read("alpha", 0).orElseThrow().ownerNodeId()
            );

            // Long enough that nothing has been asked of it.
            clock.addAndGet(IDLE_AFTER + 1);
            final var result = loop.tick(clock.get());
            assertTrue("the idle shard should have been released: " + result, result.released().contains(shardId));
            assertTrue("and closed here", node.reconciler().openShards().isEmpty());
            assertTrue(
                "and its head given up, or the node is refusing writes it will not serve",
                plane.heads().read("alpha", 0).map(h -> h.ownerNodeId()).isEmpty()
            );

            // Taking it back is the other half of the claim.
            loop.tick(clock.get());
            assertEquals("the shard should come back", 1, node.reconciler().openShards().size());
            final Response found = send(node, "GET", "/alpha/_search?q=msg:idle", null);
            assertEquals(200, found.status());
            assertTrue("the document must have survived the release: " + found.body(), found.body().contains("\"value\":1"));
        }
    }

    /**
     * A shard being used is not released, however long the node has been running.
     *
     * <p>The predicate is the whole controller. Ticked repeatedly past the idle threshold with a request
     * between each one, so anything that released on elapsed time rather than on time-since-used fails.
     */
    public void testAShardInUseIsNeverReleased() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("idle-busy", "ingest"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setIdleAfterMillis(IDLE_AFTER);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();

            for (int round = 0; round < 4; round++) {
                assertEquals(201, send(node, "PUT", "/alpha/_doc/r" + round + "?refresh=true", "{\"msg\":\"busy\",\"n\":1}").status());
                clock.addAndGet(IDLE_AFTER - 1_000);
                final var result = loop.tick(clock.get());
                assertFalse(
                    "a shard used " + (IDLE_AFTER - 1_000) + "ms ago must not be released: " + result,
                    result.released().contains(shardId)
                );
                assertEquals("and it must still be open", 1, node.reconciler().openShards().size());
            }

            // A search counts as use too, not only a write.
            for (int round = 0; round < 3; round++) {
                assertEquals(200, send(node, "GET", "/alpha/_search?q=msg:busy", null).status());
                clock.addAndGet(IDLE_AFTER - 1_000);
                assertFalse("a shard being searched must not be released", loop.tick(clock.get()).released().contains(shardId));
            }
            assertEquals("nothing should have been released at any point", 1, node.reconciler().openShards().size());
        }
    }

    /**
     * A reader is released too, and a query afterwards opens it again and answers the same.
     *
     * <p>Readers are the case that makes idle release worth having: they are what a search node
     * accumulates, they hold no head, and nothing else in the system ever gives one back.
     */
    public void testAnIdleReaderIsReleasedAndReopensOnDemand() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, 1);

        final ServerlessNode writer = new ServerlessNode(nodeSettings("idle-reader-writer", "ingest"));
        writer.start();
        writer.setMetadataPlane(plane);
        final BackgroundReconciler writing = new BackgroundReconciler(writer, plane);
        writing.want("alpha", 0);
        writing.tick(clock.get());
        assertEquals(201, send(writer, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"reader\",\"n\":1}").status());
        writing.tick(clock.get() + 1_000);   // publish
        assertTrue(plane.heads().release("alpha", 0, writer.localNode().getId()));
        writer.close();

        try (ServerlessNode reader = new ServerlessNode(nodeSettings("idle-reader", "search"))) {
            reader.start();
            reader.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(reader, plane).setIdleAfterMillis(IDLE_AFTER);

            final Response first = send(reader, "GET", "/alpha/_search?q=msg:reader", null);
            assertEquals(200, first.status());
            assertTrue("the reader must have opened the shard: " + first.body(), first.body().contains("\"value\":1"));
            assertEquals("which means it is holding it", 1, reader.reconciler().openShards().size());

            clock.addAndGet(IDLE_AFTER + 1);
            final var result = loop.tick(clock.get());
            assertEquals("the idle reader should have been let go: " + result, 0, reader.reconciler().openShards().size());
            assertFalse("and it should be reported: " + result, result.released().isEmpty());

            final Response again = send(reader, "GET", "/alpha/_search?q=msg:reader", null);
            assertEquals(200, again.status());
            assertTrue("re-opening must answer the same: " + again.body(), again.body().contains("\"value\":1"));
        }
    }

    /**
     * A writer publishes before it lets go, so it does not hand its successor a log to replay.
     *
     * <p>Not a durability claim — the log already makes the data safe, and a successor would replay it.
     * This is about the release being clean: an idle release that skipped the publish would leave records
     * behind for no reason, and the next node to take the shard would pay to replay writes that were
     * already finished.
     *
     * <p><b>{@code releaseIdle} is called directly rather than through a tick</b>, and it has to be. A tick
     * publishes everything dirty before it releases anything, so within a tick the release's own publish
     * is a no-op and removing it changes nothing — which is exactly what happened: the first version of
     * this test went through {@code tick} and passed with the publish deleted. Calling the method alone is
     * what makes it answerable for its own behaviour.
     */
    public void testAnIdleWriterPublishesBeforeItLetsGo() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("idle-publish", "ingest"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setIdleAfterMillis(IDLE_AFTER);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();
            final var wal = node.reconciler().wal(shardId);

            assertEquals(201, send(node, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"flushed\",\"n\":1}").status());
            assertFalse("the write should be sitting in the log, unpublished", wal.replayable().isEmpty());

            clock.addAndGet(IDLE_AFTER + 1);
            assertTrue("the shard should have been released", loop.releaseIdle(clock.get()).contains(shardId));

            // The commit is the proof: a successor that starts from it needs nothing from the log.
            assertEquals(
                "the released writer's data must be in the published commit",
                1L,
                documentsInThePublishedCommit(plane, "idle-verify")
            );
        }
    }

    /** Opens the shard fresh from the object store alone and counts what is there. */
    private long documentsInThePublishedCommit(MetadataPlane plane, String name) throws Exception {
        try (ServerlessNode verifier = new ServerlessNode(nodeSettings(name, "search"))) {
            verifier.start();
            verifier.setMetadataPlane(plane);
            final ShardId shardId = verifier.serveAsReader(plane, "alpha", 0);
            return org.opensearch.serverless.shard.ShardQuery.execute(verifier.searchService(), shardId, "msg", "flushed", 10).total();
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
