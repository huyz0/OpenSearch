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

            assertTrue("deleting a document that exists must report that it did", node.delete(shardId, "1").found());
            node.reconciler().shard(shardId).refresh("test");
            assertEquals("the document must be gone", 0, hits(node, shardId, "msg", "here"));

            assertFalse("deleting a document that does not exist must say so, not throw", node.delete(shardId, "nonexistent").found());
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
        assertTrue(a.delete(shardId, "2").found());
        assertTrue(a.delete(shardId, "4").found());

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
        assertTrue(a.delete(shardId, "1").found());
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

    /**
     * A deletion survives a garbage collection sweep that has something to sweep.
     *
     * <p><b>The failover is the test.</b> A first version of this published twice under one term and swept;
     * it passed, and it passed with the collector's manifest check deleted, because every live file sat in
     * the <em>current</em> term container and the sweep skips that container entirely. It exercised
     * nothing.
     *
     * <p>Publication after a failover is what creates the dangerous shape: a successor inherits its
     * predecessor's files rather than re-uploading them, so the live commit at term 2 names files that
     * live under {@code t=1} — collectable by term, and saved only by the manifest naming them. A
     * tombstone is the least obvious of those files, and if the deletion were not part of the commit the
     * sweep would be free to reclaim what carries it.
     */
    public void testADeletionSurvivesASweepOfInheritedFiles() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Path objectStore = createTempDir();
        final MetadataPlane plane = freshIndex(clock, objectStore);
        final var store = new FsBlobStore(1024, objectStore, false);
        final var gc = new org.opensearch.serverless.reconcile.GarbageCollector(store, BlobPath.cleanPath());

        final ServerlessNode a = new ServerlessNode(nodeSettings("del-gc"));
        a.start();
        final BackgroundReconciler loopA = new BackgroundReconciler(a, plane);
        loopA.want("alpha", 0);
        loopA.tick(clock.get());
        final ShardId shardId = a.reconciler().openShards().iterator().next();
        for (int i = 1; i <= 6; i++) {
            a.index(shardId, String.valueOf(i), "{\"msg\":\"swept\",\"n\":" + i + "}");
        }
        loopA.tick(clock.get() + 1_000);
        final long firstTerm = plane.segmentPublisher("alpha", 0).readManifest().orElseThrow().term();
        a.close();
        clock.set(clock.get() + TTL + 1);

        try (ServerlessNode b = new ServerlessNode(nodeSettings("del-gc-successor"))) {
            b.start();
            final BackgroundReconciler loopB = new BackgroundReconciler(b, plane);
            loopB.want("alpha", 0);
            loopB.tick(clock.get());
            final ShardId recovered = b.reconciler().openShards().iterator().next();

            assertTrue("the successor should delete a document it inherited", b.delete(recovered, "3").found());
            b.reconciler().shard(recovered).refresh("test");
            loopB.tick(clock.get() + 1_000);   // publish at the new term, inheriting the old files

            final var manifest = plane.segmentPublisher("alpha", 0).readManifest().orElseThrow();
            assertTrue("the successor must have published at a higher term", manifest.term() > firstTerm);
            final long inherited = manifest.files()
                .values()
                .stream()
                .filter(container -> container.equals(org.opensearch.serverless.store.SegmentPublisher.termSegment(firstTerm)))
                .count();
            logger.info(
                "delete + gc: live commit at term {} names {} file(s) inherited from term {}",
                manifest.term(),
                inherited,
                firstTerm
            );
            assertTrue("the live commit must inherit files from the older term, or the sweep has nothing dangerous to do", inherited > 0);

            final var collected = gc.collectShard(plane, "alpha", 0);
            logger.info("delete + gc: swept {} orphaned blobs", collected.size());
            for (var file : manifest.files().entrySet()) {
                assertTrue(
                    "the sweep deleted a file the live commit depends on: " + file.getValue() + "/" + file.getKey(),
                    store.blobContainer(plane.shardData("alpha", 0).add(file.getValue())).blobExists(file.getKey())
                );
            }
            b.reconciler().shard(recovered).refresh("test");
            assertEquals("and the deletion must have survived it", 5, hits(b, recovered, "msg", "swept"));
        }
    }

    /**
     * A deletion written under one term still applies after two more handovers.
     *
     * <p>Records are replayed across every term a shard has ever had, in term order — the log is trimmed
     * per term, and a dead predecessor's records are never trimmed by anyone, so a successor replays them
     * for as long as they exist. That makes cross-term ordering load-bearing: a write at term 1 and a
     * delete at term 2 replayed in the wrong order resurrects the document, and nothing else in the suite
     * exercises a delete that outlives the writer that made it.
     */
    public void testADeletionOutlivesTheTermItWasMadeIn() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir());

        final ServerlessNode a = new ServerlessNode(nodeSettings("del-t1"));
        a.start();
        final BackgroundReconciler loopA = new BackgroundReconciler(a, plane);
        loopA.want("alpha", 0);
        loopA.tick(clock.get());
        a.index(a.reconciler().openShards().iterator().next(), "x", "{\"msg\":\"chain\",\"n\":1}");
        a.index(a.reconciler().openShards().iterator().next(), "y", "{\"msg\":\"chain\",\"n\":2}");
        a.close();
        clock.set(clock.get() + TTL + 1);

        final ServerlessNode b = new ServerlessNode(nodeSettings("del-t2"));
        b.start();
        final BackgroundReconciler loopB = new BackgroundReconciler(b, plane);
        loopB.want("alpha", 0);
        loopB.tick(clock.get());
        final ShardId atB = b.reconciler().openShards().iterator().next();
        assertTrue("b should have recovered x before deleting it", b.delete(atB, "x").found());
        b.close();
        clock.set(clock.get() + TTL + 1);

        try (ServerlessNode c = new ServerlessNode(nodeSettings("del-t3"))) {
            c.start();
            final BackgroundReconciler loopC = new BackgroundReconciler(c, plane);
            loopC.want("alpha", 0);
            loopC.tick(clock.get());
            final ShardId atC = c.reconciler().openShards().iterator().next();
            c.reconciler().shard(atC).refresh("test");

            final long term = plane.heads().read("alpha", 0).orElseThrow().term();
            logger.info("delete across terms: third owner at term {} sees {} documents", term, hits(c, atC, "msg", "chain"));
            assertEquals("a deletion made at an earlier term must still apply two handovers later", 1, hits(c, atC, "msg", "chain"));
        }
    }

    /**
     * Replay order holds past ten records, where a naive ordinal would stop sorting correctly.
     *
     * <p>Record names are zero-padded to twenty digits precisely so string order is numeric order. Nothing
     * asserted that, and every other test in this suite writes fewer than ten records to a term — which is
     * exactly the range where {@code "10" < "2"} does not bite. A delete replayed before the write it
     * removes would resurrect the document, and only a repeated document id can show it.
     */
    public void testReplayOrderHoldsPastTenRecords() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir());

        final ServerlessNode a = new ServerlessNode(nodeSettings("del-ordinal"));
        a.start();
        final BackgroundReconciler loopA = new BackgroundReconciler(a, plane);
        loopA.want("alpha", 0);
        loopA.tick(clock.get());
        final ShardId shardId = a.reconciler().openShards().iterator().next();

        // Eight fillers take ordinals 1-8, so the write lands on 9 and the delete on 10. That straddle is
        // the whole point and it is easy to miss by one: with ordinals 10 and 11 an unpadded name still
        // sorts "10" before "11", the delete still follows the write, and the test passes while proving
        // nothing. It has to be 9 and 10, where "10" sorts before "9" and the delete arrives first.
        for (int i = 0; i < 8; i++) {
            a.index(shardId, "filler-" + i, "{\"msg\":\"ordinal\",\"n\":" + i + "}");
        }
        a.index(shardId, "target", "{\"msg\":\"ordinal\",\"n\":100}");   // ordinal 9
        assertTrue(a.delete(shardId, "target").found());                             // ordinal 10
        a.close();
        clock.set(clock.get() + TTL + 1);

        try (ServerlessNode b = new ServerlessNode(nodeSettings("del-ordinal-successor"))) {
            b.start();
            final BackgroundReconciler loopB = new BackgroundReconciler(b, plane);
            loopB.want("alpha", 0);
            loopB.tick(clock.get());
            final ShardId recovered = b.reconciler().openShards().iterator().next();
            b.reconciler().shard(recovered).refresh("test");

            assertEquals("the eight filler documents must survive", 8, hits(b, recovered, "msg", "ordinal"));
            assertEquals("and the target, deleted at ordinal 10, must not come back", 0, hits(b, recovered, "n", "100"));
        }
    }

    /**
     * A sweep must not eat the log: reclaiming records is the publish path's job, on the publish path's rule.
     *
     * <p><b>This is a boundary check, and deliberately not a durability claim.</b> The first version of it
     * asserted the log was non-empty afterwards, which stopped meaning anything once a publish at a higher
     * term began reclaiming older terms — the log was legitimately empty, and the assertion could no longer
     * tell that from a sweep having eaten it. It also asserted that no collected name contained
     * {@code "wal"}, which never could: the collector names what it deletes {@code t=N/blob}.
     *
     * <p>Made honest, it says less than it looks like it says. The sweep skips any container whose term is
     * at or above the live manifest's, so the <em>only</em> log records it could ever reach are ones under
     * a term a publish has already superseded — exactly the ones {@code WalStore} itself deletes, for the
     * same reason. Collecting them would lose nothing. What is defended here is therefore ownership rather
     * than data: two components must not both be reclaiming the log, because only one of them states a rule
     * for when that is safe. No canary can turn this into a lost write, and it should not be read as one.
     */
    public void testAGarbageCollectionSweepDoesNotTouchTheWriteAheadLog() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Path objectStore = createTempDir();
        final MetadataPlane plane = freshIndex(clock, objectStore);
        final var store = new FsBlobStore(1024, objectStore, false);
        final var gc = new org.opensearch.serverless.reconcile.GarbageCollector(store, BlobPath.cleanPath());

        final ServerlessNode a = new ServerlessNode(nodeSettings("del-wal-gc"));
        a.start();
        final BackgroundReconciler loopA = new BackgroundReconciler(a, plane);
        loopA.want("alpha", 0);
        loopA.tick(clock.get());
        final ShardId shardId = a.reconciler().openShards().iterator().next();

        a.index(shardId, "published", "{\"msg\":\"waltest\",\"n\":1}");
        loopA.tick(clock.get() + 1_000);   // publish, so a later term exists to sweep against

        // Unpublished, and deliberately so: these exist only in the log.
        a.index(shardId, "unpublished", "{\"msg\":\"waltest\",\"n\":2}");
        assertTrue(a.delete(shardId, "published").found());
        a.close();
        clock.set(clock.get() + TTL + 1);

        try (ServerlessNode b = new ServerlessNode(nodeSettings("del-wal-gc-successor"))) {
            b.start();
            final BackgroundReconciler loopB = new BackgroundReconciler(b, plane);
            loopB.want("alpha", 0);
            loopB.tick(clock.get());
            // Sweep after the successor took the shard and bumped the term, which is when older term
            // containers become collectable and the log is at its most vulnerable.
            loopB.tick(clock.get() + 1_000);

            // Two records, both of which must survive, and they are here for different reasons.
            //
            // The first is under the successor's own term: without it the log is legitimately empty by
            // now, because a publish at a higher term reclaims every older term, and "the log is empty"
            // would satisfy any assertion about the sweep sparing it.
            b.index(b.reconciler().openShards().iterator().next(), "postsweep", "{\"msg\":\"postsweep\",\"n\":3}");
            // The second is under the DEAD writer's term, appended after the successor published --
            // what a zombie that has not noticed it lost the shard would write. It is the only kind of
            // log record the sweep's term guard would let it reach at all, so it is the only one that
            // can distinguish a sweep that stays out of wal/ from one that does not.
            plane.walStore("alpha", 0).append(1, new org.opensearch.serverless.store.WalRecord("zombie", "{\"msg\":\"zombie\"}"));

            final long recordsBefore = walRecords(store, plane);
            assertEquals("the log should hold one live and one stale record before the sweep", 2, recordsBefore);

            final var collected = gc.collectShard(plane, "alpha", 0);
            logger.info("wal + gc: swept {} blobs after failover", collected.size());
            assertFalse(
                "the sweep collected something named like a log record: " + collected,
                collected.stream().anyMatch(name -> name.substring(name.indexOf('/') + 1).matches("\\d{20}"))
            );

            // Directly observable, rather than trusting the returned list. The log lives at wal/t=N,
            // one level deeper than the sweep looks, so it is protected by nesting as well as by the
            // term-name check -- and a sweep later made recursive would sail past both. This is what
            // notices that. Counted before and after, because a count that is merely non-zero is also
            // satisfied by a sweep that took some of them.
            final long recordsLeft = walRecords(store, plane);
            logger.info("wal + gc: {} of {} log record(s) survive the sweep", recordsLeft, recordsBefore);
            assertEquals("the sweep collected write-ahead log records", recordsBefore, recordsLeft);

            final ShardId recovered = b.reconciler().openShards().iterator().next();
            b.reconciler().shard(recovered).refresh("test");
            assertEquals("the unpublished write survived the sweep, and the deletion applied", 1, hits(b, recovered, "msg", "waltest"));
        }
    }

    /** How many write-ahead log records exist for alpha/0, across every term. */
    private static long walRecords(FsBlobStore store, MetadataPlane plane) throws Exception {
        final var walRoot = store.blobContainer(plane.shardData("alpha", 0).add("wal"));
        long records = 0;
        for (var termDir : walRoot.children().values()) {
            records += termDir.listBlobs().keySet().stream().filter(n -> n.matches("\\d{20}")).count();
        }
        return records;
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
