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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * M48: {@code _seq_no}, {@code _primary_term} and {@code _version} — assigned, survived, and conditioned on.
 *
 * <p><b>The fields were never missing; their durability was.</b> This shell has always run core's own
 * engine, so every write has always been assigned a sequence number — {@code BackgroundReconciler} has
 * depended on one for its publish decision since M10. What was missing is that the numbers did not survive
 * a failover: the write-ahead log recorded only an id and a source, so a successor re-applied every record
 * as a fresh primary operation and the engine minted new numbers for all of them. A token a client held
 * across a failover therefore meant nothing, which is exactly why conditional writes were refused rather
 * than merely unimplemented.
 *
 * <p><b>The test that matters is the one that crosses a failover.</b> Anything can report a sequence number
 * for a write it just performed. The claim worth proving is that the number a client was given before its
 * writer died is still the number that document has afterwards, and still works as a compare-and-swap
 * token against the successor. That is
 * {@link #testAConditionalWriteTokenMintedBeforeAFailoverStillWorksAfterIt}.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessSequenceNumberTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-seqno")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private MetadataPlane freshIndex(AtomicLong clock, Path objectStore) throws Exception {
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, objectStore, false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        return plane;
    }

    /** Every write reports the sequence identity the engine gave it, and it advances by one each time. */
    public void testWritesReportTheSequenceIdentityTheEngineAssigned() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir());

        try (ServerlessNode node = new ServerlessNode(nodeSettings("seq-basic"))) {
            node.start();
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();

            final var first = node.index(shardId, "1", "{\"msg\":\"a\",\"n\":1}");
            final var second = node.index(shardId, "2", "{\"msg\":\"b\",\"n\":2}");
            assertEquals("the first write of a shard's life is sequence number 0", 0L, first.seqNo());
            assertEquals("and the next is 1", 1L, second.seqNo());
            assertTrue("a write must report a positive primary term", first.primaryTerm() > 0);
            assertEquals("the primary term is the shard-head's term, so both writes share it", first.primaryTerm(), second.primaryTerm());
            assertEquals("a first write of a document is version 1", 1L, first.version());

            // Overwriting the same document advances its version but not the other document's.
            final var overwrite = node.index(shardId, "1", "{\"msg\":\"a2\",\"n\":1}");
            assertEquals("an overwrite is version 2 of that document", 2L, overwrite.version());
            assertEquals("and takes the next sequence number", 2L, overwrite.seqNo());
            assertFalse("an overwrite is not a creation", overwrite.created());
            assertTrue("the first write of that document was", first.created());
        }
    }

    /**
     * The sequence numbers a predecessor assigned are the sequence numbers the successor serves — for
     * documents that were published, and for documents that only ever reached the log.
     *
     * <p>This is the property the whole milestone exists for. Before it, a successor re-applied the log as
     * fresh primary operations and every replayed document came back with a different number.
     */
    public void testSequenceNumbersSurviveAFailover() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Path objectStore = createTempDir();
        final MetadataPlane plane = freshIndex(clock, objectStore);

        final Map<String, Long> before = new LinkedHashMap<>();
        final long termBefore;

        final ServerlessNode a = new ServerlessNode(nodeSettings("seq-a"));
        a.start();
        final BackgroundReconciler loopA = new BackgroundReconciler(a, plane);
        loopA.want("alpha", 0);
        loopA.tick(clock.get());
        final ShardId onA = a.reconciler().openShards().iterator().next();

        // Two documents that get published, so they arrive at the successor inside the commit.
        before.put("published-1", a.index(onA, "published-1", "{\"msg\":\"p1\",\"n\":1}").seqNo());
        before.put("published-2", a.index(onA, "published-2", "{\"msg\":\"p2\",\"n\":2}").seqNo());
        loopA.tick(clock.get() + 1_000);

        // And two that never are, so they arrive only by replay -- the half that used to be renumbered.
        before.put("logged-1", a.index(onA, "logged-1", "{\"msg\":\"l1\",\"n\":3}").seqNo());
        before.put("logged-2", a.index(onA, "logged-2", "{\"msg\":\"l2\",\"n\":4}").seqNo());
        termBefore = a.get(onA, "logged-2").primaryTerm();

        a.close();
        clock.set(clock.get() + TTL + 1);

        try (ServerlessNode b = new ServerlessNode(nodeSettings("seq-b"))) {
            b.start();
            final BackgroundReconciler loopB = new BackgroundReconciler(b, plane);
            loopB.want("alpha", 0);
            loopB.tick(clock.get());
            final ShardId onB = b.reconciler().openShards().iterator().next();

            for (Map.Entry<String, Long> entry : before.entrySet()) {
                final var recovered = b.get(onB, entry.getKey());
                assertTrue(entry.getKey() + " must have survived the failover", recovered.found());
                assertEquals(
                    "the sequence number of " + entry.getKey() + " must be the one its writer assigned, not a new one",
                    entry.getValue().longValue(),
                    recovered.seqNo()
                );
            }

            // The successor's own writes continue above the inherited history rather than colliding with
            // it -- the property that makes the numbers usable at all.
            final var next = b.index(onB, "after-failover", "{\"msg\":\"n\",\"n\":5}");
            for (Long inherited : before.values()) {
                assertTrue(
                    "a new write must take a sequence number above everything inherited: " + next.seqNo() + " vs " + inherited,
                    next.seqNo() > inherited
                );
            }
            assertTrue("and must run at a higher primary term, because ownership changed", next.primaryTerm() > termBefore);
        }
    }

    /**
     * A token minted before a failover still works as a compare-and-swap against the successor.
     *
     * <p>The end-to-end version of the claim: not merely that the number is stable, but that the guarantee
     * built on it holds across the event that used to break it. A stale token must still be refused, or
     * "it works" would be indistinguishable from "the condition is ignored".
     */
    public void testAConditionalWriteTokenMintedBeforeAFailoverStillWorksAfterIt() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Path objectStore = createTempDir();
        final MetadataPlane plane = freshIndex(clock, objectStore);

        final ServerlessNode a = new ServerlessNode(nodeSettings("seq-cas-a"));
        a.start();
        final BackgroundReconciler loopA = new BackgroundReconciler(a, plane);
        loopA.want("alpha", 0);
        loopA.tick(clock.get());
        final ShardId onA = a.reconciler().openShards().iterator().next();

        final var minted = a.index(onA, "doc", "{\"msg\":\"original\",\"n\":1}");
        final long tokenSeqNo = minted.seqNo();
        final long tokenTerm = minted.primaryTerm();

        a.close();
        clock.set(clock.get() + TTL + 1);

        try (ServerlessNode b = new ServerlessNode(nodeSettings("seq-cas-b"))) {
            b.start();
            final BackgroundReconciler loopB = new BackgroundReconciler(b, plane);
            loopB.want("alpha", 0);
            loopB.tick(clock.get());
            final ShardId onB = b.reconciler().openShards().iterator().next();

            // A stale token is refused by the successor. Asserted first: if the condition were being
            // ignored, the success below would prove nothing at all.
            expectThrows(
                org.opensearch.index.engine.VersionConflictEngineException.class,
                () -> b.index(onB, "doc", "{\"msg\":\"stale\",\"n\":9}", tokenSeqNo + 500, tokenTerm)
            );

            // And the token minted on the node that has since died is still honoured.
            final var conditional = b.index(onB, "doc", "{\"msg\":\"updated\",\"n\":2}", tokenSeqNo, tokenTerm);
            assertTrue("the conditional write must have been applied", conditional.seqNo() > tokenSeqNo);
            assertEquals("and it overwrote rather than created", false, conditional.created());
            assertEquals("the document must now hold the new source", "updated", messageOf(b.get(onB, "doc").source()));
        }
    }

    /** Over HTTP: the fields are rendered, and a stale condition is a 409 rather than a silent overwrite. */
    public void testTheRestSurfaceRendersAndEnforcesTheCondition() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir());

        try (ServerlessNode node = new ServerlessNode(nodeSettings("seq-rest"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            final Response written = send(node, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"one\",\"n\":1}");
            assertEquals(written.body(), 201, written.status());
            assertTrue("a write must report _seq_no: " + written.body(), written.body().contains("\"_seq_no\":"));
            assertTrue("and _primary_term: " + written.body(), written.body().contains("\"_primary_term\":"));
            assertTrue("and _version: " + written.body(), written.body().contains("\"_version\":1"));
            assertTrue("a first write is a creation: " + written.body(), written.body().contains("\"result\":\"created\""));

            final long seqNo = numberField(written.body(), "_seq_no");
            final long term = numberField(written.body(), "_primary_term");

            // A get hands back the same token, which is how a client obtains one without having written.
            final Response got = send(node, "GET", "/alpha/_doc/1", null);
            assertEquals(seqNo, numberField(got.body(), "_seq_no"));
            assertEquals(term, numberField(got.body(), "_primary_term"));

            // A stale condition is refused with a conflict, and does not write.
            final Response stale = send(
                node,
                "PUT",
                "/alpha/_doc/1?refresh=true&if_seq_no=" + (seqNo + 500) + "&if_primary_term=" + term,
                "{\"msg\":\"stale\",\"n\":9}"
            );
            assertEquals("a stale condition must be a conflict: " + stale.body(), 409, stale.status());
            assertTrue(
                "still the original document: " + send(node, "GET", "/alpha/_doc/1", null).body(),
                send(node, "GET", "/alpha/_doc/1", null).body().contains("one")
            );

            // The current condition succeeds, and reports an overwrite honestly rather than as a creation.
            final Response conditional = send(
                node,
                "PUT",
                "/alpha/_doc/1?refresh=true&if_seq_no=" + seqNo + "&if_primary_term=" + term,
                "{\"msg\":\"two\",\"n\":2}"
            );
            assertEquals(conditional.body(), 200, conditional.status());
            assertTrue(
                "an overwrite is 'updated', not 'created': " + conditional.body(),
                conditional.body().contains("\"result\":\"updated\"")
            );
            assertTrue("and version 2: " + conditional.body(), conditional.body().contains("\"_version\":2"));
        }
    }

    /** {@code _update} accepts the same condition and reports the identity a retry would use. */
    public void testUpdateAcceptsAConditionAndReportsTheIdentity() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir());

        try (ServerlessNode node = new ServerlessNode(nodeSettings("seq-update"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            final Response written = send(node, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"one\",\"n\":1}");
            final long seqNo = numberField(written.body(), "_seq_no");
            final long term = numberField(written.body(), "_primary_term");

            final Response stale = send(
                node,
                "POST",
                "/alpha/_update/1?refresh=true&if_seq_no=" + (seqNo + 500) + "&if_primary_term=" + term,
                "{\"doc\":{\"msg\":\"stale\"}}"
            );
            assertEquals("a stale condition must be a conflict here too: " + stale.body(), 409, stale.status());

            final Response updated = send(
                node,
                "POST",
                "/alpha/_update/1?refresh=true&if_seq_no=" + seqNo + "&if_primary_term=" + term,
                "{\"doc\":{\"msg\":\"two\"}}"
            );
            assertEquals(updated.body(), 200, updated.status());
            assertTrue("an update must report the new identity: " + updated.body(), updated.body().contains("\"_seq_no\":"));
            assertTrue(
                "and the sequence number must have moved past the token it was conditioned on: " + updated.body(),
                numberField(updated.body(), "_seq_no") > seqNo
            );
        }
    }

    /** Each bulk item reports its own sequence identity, and an overwrite says so. */
    public void testBulkItemsReportTheirOwnSequenceIdentity() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir());

        try (ServerlessNode node = new ServerlessNode(nodeSettings("seq-bulk"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            final Response first = send(
                node,
                "POST",
                "/_bulk?refresh=true",
                "{\"index\":{\"_index\":\"alpha\",\"_id\":\"1\"}}\n{\"msg\":\"a\",\"n\":1}\n"
                    + "{\"index\":{\"_index\":\"alpha\",\"_id\":\"2\"}}\n{\"msg\":\"b\",\"n\":2}\n"
            );
            assertEquals(first.body(), 200, first.status());
            assertTrue("each item must carry a sequence number: " + first.body(), first.body().contains("\"_seq_no\":0"));
            assertTrue("and they must differ: " + first.body(), first.body().contains("\"_seq_no\":1"));
            assertFalse("a first write is not an update: " + first.body(), first.body().contains("\"result\":\"updated\""));

            final Response again = send(
                node,
                "POST",
                "/_bulk?refresh=true",
                "{\"index\":{\"_index\":\"alpha\",\"_id\":\"1\"}}\n{\"msg\":\"a2\",\"n\":1}\n"
            );
            assertTrue(
                "rewriting a document is an update, not a creation: " + again.body(),
                again.body().contains("\"result\":\"updated\"")
            );
        }
    }

    /**
     * A hole in the log does not leave a hole in the sequence space, and the shard stays readable.
     *
     * <p><b>What makes a hole possible.</b> The engine assigns a sequence number before it applies, so an
     * operation that fails inside Lucene — or one whose log append fails after it was already applied —
     * burns a number that no record accounts for. Replay then reconstructs a history with a gap in it.
     *
     * <p><b>Why this shell does not fill the hole itself, and does not need to.</b>
     * {@code StoreRecovery#internalRecoverFromStore} ends with {@code fillSeqNoGaps} — inside
     * {@code IndexShard#recoverFromStore}, which is the only way this shell ever opens a writer. Replayed
     * operations are already in the engine by then, because {@code ServerlessWriterEngine} feeds them in
     * during recovery, so the gaps core fills are the gaps the log left. M48's notes claimed this shell had
     * no analogue of {@code fillSeqNoGaps} because the method is unreachable from outside {@code IndexShard};
     * it does not need to reach it, because the recovery path it already uses calls it.
     *
     * <p><b>What this test is.</b> A characterization test, not a test of shell code: the filling happens in
     * core. Its value is that it fails if activation ever stops being a promotion — if the head term stops
     * being threaded through {@code updateShardState}, or activation stops going through that path at all.
     * The property it pins is the one downstream code would care about: the published commit's local
     * checkpoint keeps up with its max sequence number, so the commit stays coherent for anything that
     * later demands a complete history.
     *
     * <p>The hole is planted rather than provoked. Producing one for real needs a Lucene failure or a
     * failing object store injected at one exact instant; the property worth pinning is "if a hole exists it
     * does not survive activation", and planting one states that without building a fault-injection harness.
     */
    public void testAHoleInTheLogDoesNotSurviveActivation() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Path objectStore = createTempDir();
        final MetadataPlane plane = freshIndex(clock, objectStore);
        final String uuid = plane.describe("alpha").orElseThrow().uuid();

        // Sequence numbers 0 and 2, with nothing at 1: the shape a burned-but-unlogged number leaves.
        final var wal = plane.walStore("alpha", uuid, 0);
        wal.append(
            1L,
            java.util.List.of(
                new org.opensearch.serverless.store.WalRecord("a", "{\"msg\":\"first\",\"n\":1}", 0L, 1L, 1L),
                new org.opensearch.serverless.store.WalRecord("c", "{\"msg\":\"third\",\"n\":3}", 2L, 1L, 1L)
            )
        );

        try (ServerlessNode writer = new ServerlessNode(nodeSettings("gap-writer"))) {
            writer.start();
            final ShardId shardId = writer.activateWriter(plane, "alpha", 0).orElseThrow();

            // Both logged documents are there, each keeping the number it was logged with: the hole cost
            // no data and did not renumber what surrounded it.
            assertTrue("the record before the hole must have replayed", writer.get(shardId, "a").found());
            assertTrue("and the one after it", writer.get(shardId, "c").found());
            assertEquals("the surviving records keep their own sequence numbers", 0L, writer.get(shardId, "a").seqNo());
            assertEquals(2L, writer.get(shardId, "c").seqNo());

            // The assertion that matters. Without gap-filling the checkpoint stalls at 0 -- one below the
            // hole -- for the rest of this shard's life and every successor's, because a checkpoint cannot
            // advance past a number it has never seen.
            final var stats = writer.reconciler().shard(shardId).seqNoStats();
            assertEquals("the hole must be accounted for, not left open", 2L, stats.getMaxSeqNo());
            assertEquals("the local checkpoint must have advanced past the hole", 2L, stats.getLocalCheckpoint());
            assertEquals("and the global checkpoint with it", 2L, stats.getGlobalCheckpoint());

            writer.reconciler().shard(shardId).refresh("test");
            writer.publishShard(shardId, plane.heads().read("alpha", 0).orElseThrow().term());
        }

        // A commit whose max sequence number sits above its global checkpoint is the shape ReadOnlyEngine
        // refuses when it is asked for a complete history. This shell does not ask for one today, so this
        // assertion is a guard on the option staying open rather than on a live failure.
        final Settings searchOnly = Settings.builder()
            .put("node.name", "gap-reader")
            .put("cluster.name", "serverless-seqno")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "search")
            .build();
        try (ServerlessNode reader = new ServerlessNode(searchOnly)) {
            reader.start();
            final ShardId asReader = reader.serveAsReader(plane, "alpha", 0);
            assertNotNull("a reader must be able to open a shard whose log had a hole in it", asReader);
            assertTrue("and serve what was published", reader.get(asReader, "a").found());
        }
    }

    private static String messageOf(String source) {
        final Matcher matcher = Pattern.compile("\"msg\"\\s*:\\s*\"([^\"]*)\"").matcher(source);
        assertTrue("no msg in " + source, matcher.find());
        return matcher.group(1);
    }

    private static long numberField(String body, String name) {
        final Matcher matcher = Pattern.compile("\"" + name + "\"\\s*:\\s*(-?\\d+)").matcher(body);
        assertTrue("no " + name + " in " + body, matcher.find());
        return Long.parseLong(matcher.group(1));
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
