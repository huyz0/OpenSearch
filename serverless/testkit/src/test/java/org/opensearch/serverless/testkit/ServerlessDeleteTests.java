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
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Deletes, and the reason they had to be in the log.
 *
 * <p>Until now the durability promise ran one way: every acknowledged write survives its node dying. There
 * was no matching claim for deletions because there were no deletions, and the moment there are, the
 * existing machinery becomes actively dangerous rather than merely incomplete.
 *
 * <p>Replay is an idempotent redo of state. A successor rebuilds a shard by applying every record in the
 * log to whatever the last published commit contained. A deletion that was acknowledged and not logged is
 * absent from that reconstruction, so the document it removed <b>comes back</b> — during a recovery that
 * reports success, to a caller that was told the delete worked. That is the failure this suite exists to
 * make impossible, and the canary for it is planted rather than assumed.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessDeleteTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-delete")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private MetadataPlane freshIndex(AtomicLong clock, Path dir) throws Exception {
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, dir, false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        return plane;
    }

    /** The record round-trips, and a deletion is distinguishable from a write of an empty document. */
    public void testADeletionSurvivesSerialisation() throws Exception {
        final byte[] bytes = org.opensearch.core.common.bytes.BytesReference.toBytes(WalRecord.deletion("7").toBytes());
        logger.info("delete record bytes: {}", new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
        final WalRecord parsed = WalRecord.fromStream(new java.io.ByteArrayInputStream(bytes));
        assertTrue("a deletion must parse back as a deletion", parsed.isDeletion());
        assertEquals("7", parsed.id());

        // And the other direction: an ordinary write must not be mistaken for one.
        final byte[] write = org.opensearch.core.common.bytes.BytesReference.toBytes(new WalRecord("7", "{}").toBytes());
        assertFalse(
            "an indexed document must not parse as a deletion",
            WalRecord.fromStream(new java.io.ByteArrayInputStream(write)).isDeletion()
        );
    }

    /**
     * A log written before deletions existed must keep meaning what it meant.
     *
     * <p>The marker is only written for deletions, so an old record has no {@code deleted} field at all —
     * and a reader that has never heard of the field reads such a record as an index operation, which is
     * exactly what it is. Worth pinning: a format change that quietly reinterpreted old records would turn
     * every previously-written document into an ambiguous one.
     */
    public void testALogWrittenBeforeDeletesExistedStillReadsAsWrites() throws Exception {
        final String legacy = "{\"id\":\"5\",\"source\":\"{\\\"msg\\\":\\\"old\\\"}\"}";
        final WalRecord parsed = WalRecord.fromStream(
            new java.io.ByteArrayInputStream(legacy.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        );
        assertFalse("a record with no deletion marker is a write", parsed.isDeletion());
        assertEquals("5", parsed.id());
    }

    /** The ordinary path: a deleted document stops being found, and deleting nothing says so. */
    public void testADeletedDocumentIsGoneAndDeletingNothingSaysSo() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir());

        try (ServerlessNode node = new ServerlessNode(nodeSettings("del-basic"))) {
            node.start();
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();

            node.index(shardId, "1", "{\"msg\":\"here\",\"n\":1}");
            node.reconciler().shard(shardId).refresh("test");
            assertEquals(1, hits(node, shardId, "msg", "here"));

            assertTrue("deleting a document that exists must report that it did", node.delete(shardId, "1"));
            node.reconciler().shard(shardId).refresh("test");
            assertEquals("the document must be gone", 0, hits(node, shardId, "msg", "here"));

            assertFalse("deleting a document that does not exist must say so, not throw", node.delete(shardId, "nonexistent"));
        }
    }

    /**
     * The property this whole thing exists for: a node dies after acknowledging a delete, and the
     * successor does not bring the document back.
     *
     * <p>Nothing is published between the delete and the death, so the successor's only source for it is
     * the write-ahead log. Remove deletions from that log and this test reports a resurrected document,
     * which is exactly what the canary confirms.
     */
    public void testASuccessorDoesNotResurrectADeletedDocument() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Path objectStore = createTempDir();
        final MetadataPlane plane = freshIndex(clock, objectStore);

        final ServerlessNode a = new ServerlessNode(nodeSettings("del-writer"));
        a.start();
        final BackgroundReconciler loopA = new BackgroundReconciler(a, plane);
        loopA.want("alpha", 0);
        loopA.tick(clock.get());
        final ShardId shardId = a.reconciler().openShards().iterator().next();

        for (int i = 1; i <= 5; i++) {
            a.index(shardId, String.valueOf(i), "{\"msg\":\"kept\",\"n\":" + i + "}");
        }
        // Published, so the successor recovers these from a commit rather than from the log.
        loopA.tick(clock.get() + 1_000);

        // Now write one more and delete two, none of it published. The log is the only record.
        a.index(shardId, "6", "{\"msg\":\"kept\",\"n\":6}");
        assertTrue(a.delete(shardId, "2"));
        assertTrue(a.delete(shardId, "4"));

        // The writer dies without publishing and without releasing anything.
        a.close();
        clock.set(clock.get() + TTL + 1);

        try (ServerlessNode b = new ServerlessNode(nodeSettings("del-successor"))) {
            b.start();
            final BackgroundReconciler loopB = new BackgroundReconciler(b, plane);
            loopB.want("alpha", 0);
            loopB.tick(clock.get());
            final ShardId recovered = b.reconciler().openShards().iterator().next();
            b.reconciler().shard(recovered).refresh("test");

            final int found = hits(b, recovered, "msg", "kept");
            logger.info("delete failover: successor sees {} documents (expected 4: 1,3,5,6)", found);
            assertEquals("documents 2 and 4 were deleted and must not come back: found " + found, 4, found);
        }
    }

    /**
     * Order matters, and replaying writes before deletes would get this wrong.
     *
     * <p>A document written, deleted, then written again must end up present. Replay applies records in
     * log order for exactly this reason; a reconstruction that grouped operations by kind would delete the
     * document it had just been told to re-create.
     */
    public void testAReWrittenDocumentSurvivesItsOwnEarlierDeletion() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir());

        final ServerlessNode a = new ServerlessNode(nodeSettings("del-order"));
        a.start();
        final BackgroundReconciler loopA = new BackgroundReconciler(a, plane);
        loopA.want("alpha", 0);
        loopA.tick(clock.get());
        final ShardId shardId = a.reconciler().openShards().iterator().next();

        a.index(shardId, "1", "{\"msg\":\"first\",\"n\":1}");
        assertTrue(a.delete(shardId, "1"));
        a.index(shardId, "1", "{\"msg\":\"second\",\"n\":2}");
        a.close();
        clock.set(clock.get() + TTL + 1);

        try (ServerlessNode b = new ServerlessNode(nodeSettings("del-order-successor"))) {
            b.start();
            final BackgroundReconciler loopB = new BackgroundReconciler(b, plane);
            loopB.want("alpha", 0);
            loopB.tick(clock.get());
            final ShardId recovered = b.reconciler().openShards().iterator().next();
            b.reconciler().shard(recovered).refresh("test");

            assertEquals("the re-written document must survive its earlier deletion", 1, hits(b, recovered, "msg", "second"));
            assertEquals("and the deleted version must not", 0, hits(b, recovered, "msg", "first"));
        }
    }

    /** Over HTTP, including the status a delete of something absent should carry. */
    public void testDeleteOverHttp() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir());

        try (ServerlessNode node = new ServerlessNode(nodeSettings("del-rest"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            final var http = node.boundHttpAddress().publishAddress();
            assertEquals(201, send(http, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"rest\",\"n\":1}").status());

            final Response deleted = send(http, "DELETE", "/alpha/_doc/1?refresh=true", null);
            assertEquals("deleting an existing document: " + deleted.body(), 200, deleted.status());
            assertTrue("the result should say deleted: " + deleted.body(), deleted.body().contains("\"result\":\"deleted\""));

            final Response missing = send(http, "DELETE", "/alpha/_doc/1?refresh=true", null);
            assertEquals("deleting it again must not claim success: " + missing.body(), 404, missing.status());
            assertTrue("and should say why: " + missing.body(), missing.body().contains("\"result\":\"not_found\""));

            final Response search = send(http, "GET", "/alpha/_search?q=msg:rest", null);
            assertEquals(200, search.status());
            assertFalse("the deleted document must not be searchable: " + search.body(), search.body().contains("\"_id\":\"1\""));
        }
    }

    private int hits(ServerlessNode node, ShardId shardId, String field, String value) throws Exception {
        return (int) org.opensearch.serverless.shard.ShardQuery.execute(node.searchService(), shardId, field, value, 100).total();
    }

    private record Response(int status, String body) {
    }

    private static Response send(org.opensearch.core.common.transport.TransportAddress address, String method, String path, String body)
        throws Exception {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest.BodyPublisher payload = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .method(method, payload)
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
