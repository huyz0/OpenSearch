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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@code _bulk}: the write path priced per request rather than per document.
 *
 * <p>Every other property here is in service of one number. Before this, a document cost one
 * object-store PUT, measured and asserted in {@link ServerlessCostTests}; the log's own documentation
 * said group commit belonged there and was not built. So the test that matters is the one that counts
 * blob writes, and the rest exist because batching is only worth having if it does not quietly weaken
 * ordering, durability, or the refusal to reinterpret a request.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only. The cost claim is re-measured on a bucket in
 * {@link ServerlessCostTests}, where an operation is a billed request rather than a system call.
 */
public class ServerlessBulkTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-bulk")
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

    /** An index-action line and its source, as a client would send them. */
    private static String indexLine(String id, int n) {
        return "{\"index\":{\"_id\":\"" + id + "\"}}\n{\"msg\":\"bulk\",\"n\":" + n + "}\n";
    }

    private static String deleteLine(String id) {
        return "{\"delete\":{\"_id\":\"" + id + "\"}}\n";
    }

    /**
     * The claim the whole feature exists for: fifty documents, one object-store write.
     *
     * <p>{@link ServerlessCostTests} asserts that a single-document write costs exactly one PUT, which was
     * the honest measurement of a write path priced per document. This is the same counter, and it must
     * now report one for the whole batch — measured before publication, because the segment files a
     * publish uploads are not the cost of the write and counting them together is the mistake that test
     * records having made.
     */
    public void testFiftyDocumentsInOneBatchCostOneObjectStoreWrite() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("bulk-cost"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            final int documents = 50;
            final StringBuilder body = new StringBuilder();
            for (int i = 0; i < documents; i++) {
                body.append(indexLine(String.valueOf(i), i));
            }

            store.reset();
            final Response response = send(node, "POST", "/alpha/_bulk?refresh=true", body.toString());
            final long writes = store.blobWrites();

            assertEquals("the batch should be accepted: " + response.body(), 200, response.status());
            assertTrue("no item should have failed: " + response.body(), response.body().contains("\"errors\":false"));
            logger.info("bulk: {} documents cost {} object-store write(s)", documents, writes);
            assertEquals("a batch on one shard must be one log append, not one per document", 1, writes);

            assertEquals("every document must be searchable", documents, hits(node, "msg", "bulk"));
        }
    }

    /**
     * A batch spanning shards costs one write per shard, which is the bound a group commit can actually
     * offer: a shard's log is its own, so two shards cannot share an append.
     */
    public void testABatchSpanningShardsCostsOneWritePerShard() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        final int shards = 3;
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", shards, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("bulk-shards"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            for (int shard = 0; shard < shards; shard++) {
                loop.want("alpha", shard);
            }
            loop.tick(clock.get());
            assertEquals(
                "this node must hold every shard for the count to mean what it says",
                shards,
                node.reconciler().openShards().size()
            );

            final int documents = 60;
            final StringBuilder body = new StringBuilder();
            final var descriptor = plane.describe("alpha").orElseThrow();
            final java.util.Set<Integer> touched = new java.util.HashSet<>();
            for (int i = 0; i < documents; i++) {
                body.append(indexLine(String.valueOf(i), i));
                touched.add(org.opensearch.serverless.rest.DocumentRouting.shardFor(descriptor, String.valueOf(i)));
            }
            // Sixty ids over three shards will hit all three, but asserting against a computed set rather
            // than against 3 keeps the test honest if the hash ever changes.
            assertEquals("the fixture is meant to span every shard", shards, touched.size());

            store.reset();
            final Response response = send(node, "POST", "/alpha/_bulk?refresh=true", body.toString());
            final long writes = store.blobWrites();

            assertEquals("the batch should be accepted: " + response.body(), 200, response.status());
            logger.info("bulk: {} documents over {} shards cost {} object-store write(s)", documents, touched.size(), writes);
            assertEquals("one append per shard touched, regardless of how many documents landed on it", touched.size(), writes);
            assertEquals("every document must be searchable", documents, hits(node, "msg", "bulk"));
        }
    }

    /**
     * Order inside a batch is preserved, and it survives being replayed out of one blob.
     *
     * <p>Batching put several records in a single blob, so ordering is no longer "one blob per record,
     * sorted by name" — it is (ordinal, position within the blob). A container that lost position would
     * still pass every count-based test here while turning a write-then-delete into a delete-then-write,
     * which is the difference between a document being gone and being present.
     *
     * <p>Checked twice: once against the live shard, and once against a successor that rebuilt from the
     * log alone, because only the second exercises the parsing.
     */
    public void testOrderWithinABatchSurvivesReplay() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final java.nio.file.Path objectStore = createTempDir();
        final MetadataPlane plane = freshIndex(clock, objectStore, 1);

        final ServerlessNode a = new ServerlessNode(nodeSettings("bulk-order"));
        a.start();
        a.setMetadataPlane(plane);
        final BackgroundReconciler loop = new BackgroundReconciler(a, plane);
        loop.want("alpha", 0);
        loop.tick(clock.get());

        // "gone" is written and then deleted; "present" is deleted and then written. Both in one batch,
        // so both live in one blob and only their position separates them.
        final String body = indexLine("gone", 1) + indexLine("present", 2) + deleteLine("gone") + deleteLine("present") + indexLine(
            "present",
            3
        );
        final Response response = send(a, "POST", "/alpha/_bulk?refresh=true", body);
        assertEquals("the batch should be accepted: " + response.body(), 200, response.status());

        assertEquals("written then deleted must end up gone", 0, hits(a, "n", "1"));
        assertEquals("deleted then rewritten must end up present", 1, hits(a, "n", "3"));

        // Nothing published, so the successor has only the log -- and the log is one blob.
        a.close();
        clock.set(1_000L + TTL);
        try (ServerlessNode b = new ServerlessNode(nodeSettings("bulk-order-successor"))) {
            b.start();
            final ShardId onB = b.activateWriter(plane, "alpha", 0).orElseThrow();
            b.reconciler().shard(onB).refresh("test");
            assertEquals("replay resurrected a document the batch had deleted", 0, hits(b, "n", "1"));
            assertEquals("replay lost a document the batch had rewritten", 1, hits(b, "n", "3"));
        }
    }

    /**
     * A batch is durable before it is acknowledged, which is the promise batching must not weaken.
     *
     * <p>The whole batch reaches the log in one append <em>before</em> any of it is applied, so a writer
     * that dies with nothing published still loses nothing.
     */
    public void testABatchIsDurableBeforeItIsAcknowledged() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 1);

        final ServerlessNode a = new ServerlessNode(nodeSettings("bulk-durable"));
        a.start();
        a.setMetadataPlane(plane);
        final BackgroundReconciler loop = new BackgroundReconciler(a, plane);
        loop.want("alpha", 0);
        loop.tick(clock.get());

        final int documents = 25;
        final StringBuilder body = new StringBuilder();
        for (int i = 0; i < documents; i++) {
            body.append(indexLine("d" + i, i));
        }
        assertEquals(200, send(a, "POST", "/alpha/_bulk", body.toString()).status());

        // No publish, no release, no clean shutdown.
        a.close();
        clock.set(1_000L + TTL);
        try (ServerlessNode b = new ServerlessNode(nodeSettings("bulk-durable-successor"))) {
            b.start();
            final ShardId onB = b.activateWriter(plane, "alpha", 0).orElseThrow();
            b.reconciler().shard(onB).refresh("test");
            assertEquals("an acknowledged batch was lost when the writer died", documents, hits(b, "msg", "bulk"));
        }
    }

    /**
     * {@code create} and {@code update} are refused per item rather than quietly treated as {@code index}.
     *
     * <p>Both mean something this system cannot do — fail-if-exists and a partial merge, each needing
     * version-conditional writes. Accepting them as ordinary writes would be a request that succeeds and
     * means something other than what it said, which is exactly what D2's 501s exist to prevent. Refusing
     * them per item rather than per request is what keeps the surrounding documents useful.
     */
    public void testUnsupportedActionsAreRefusedPerItemAndTheRestOfTheBatchLands() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("bulk-unsupported"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            final String body = indexLine("ok-1", 1)
                + "{\"create\":{\"_id\":\"nope-1\"}}\n{\"msg\":\"bulk\",\"n\":9}\n"
                + "{\"update\":{\"_id\":\"nope-2\"}}\n{\"doc\":{\"n\":9}}\n"
                + indexLine("ok-2", 2);

            final Response response = send(node, "POST", "/alpha/_bulk?refresh=true", body);
            assertEquals("the request itself is fine; the items are not: " + response.body(), 200, response.status());
            assertTrue("the response must own up to the failures: " + response.body(), response.body().contains("\"errors\":true"));
            assertTrue("and say why: " + response.body(), response.body().contains("\"unsupported_action\""));
            assertEquals("an unsupported action must be refused, not reinterpreted as a write", 0, hits(node, "n", "9"));
            assertEquals("the supported items in the same batch must still land", 2, hits(node, "msg", "bulk"));
        }
    }

    /**
     * A batch spanning two nodes is one forwarded request per shard, not one per document.
     *
     * <p>Forwarding item by item would have been simpler and would have put the per-document cost back
     * the moment a shard was not local — so the far side's log append count is the assertion, not just
     * that the documents arrived.
     */
    public void testABatchForwardedToAnotherNodeStaysABatch() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 2, MAPPING, null));

        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("bulk-fwd-a"));
            ServerlessNode b = new ServerlessNode(nodeSettings("bulk-fwd-b"))
        ) {
            a.start();
            b.start();
            a.setMetadataPlane(plane);
            b.setMetadataPlane(plane);
            // One shard each, so any batch touching both must cross the network.
            final BackgroundReconciler loopA = new BackgroundReconciler(a, plane);
            loopA.want("alpha", 0);
            loopA.tick(clock.get());
            final BackgroundReconciler loopB = new BackgroundReconciler(b, plane);
            loopB.want("alpha", 1);
            loopB.tick(clock.get());
            assertEquals("node a should hold exactly one shard", 1, a.reconciler().openShards().size());
            assertEquals("node b should hold exactly one shard", 1, b.reconciler().openShards().size());

            final var descriptor = plane.describe("alpha").orElseThrow();
            final int documents = 40;
            final StringBuilder body = new StringBuilder();
            final java.util.Set<Integer> touched = new java.util.HashSet<>();
            for (int i = 0; i < documents; i++) {
                body.append(indexLine("f" + i, i));
                touched.add(org.opensearch.serverless.rest.DocumentRouting.shardFor(descriptor, "f" + i));
            }
            assertEquals("the fixture must span both shards, or nothing is forwarded", 2, touched.size());

            store.reset();
            final Response response = send(a, "POST", "/alpha/_bulk?refresh=true", body.toString());
            final long writes = store.blobWrites();

            assertEquals("the batch should be accepted: " + response.body(), 200, response.status());
            assertTrue("no item should have failed: " + response.body(), response.body().contains("\"errors\":false"));
            logger.info("bulk: {} documents across two nodes cost {} object-store write(s)", documents, writes);
            assertEquals("one append per shard, even when a shard is on another node", 2, writes);

            // Both nodes appear as writers, which is the visible proof the batch was split rather than
            // being applied wherever it happened to arrive.
            assertTrue("the response should name node a as a writer: " + response.body(), response.body().contains(a.localNode().getId()));
            assertTrue("the response should name node b as a writer: " + response.body(), response.body().contains(b.localNode().getId()));
            assertEquals("every document must be searchable across the pair", documents, hits(a, "msg", "bulk") + hits(b, "msg", "bulk"));

            // Now a batch whose items have DIFFERENT outcomes, all bound for the remote shard. Everything
            // above sent nothing but writes, so every outcome was identical and a response that paired
            // them with the wrong documents would have looked perfect. Two deletes -- one of a document
            // that exists, one of a document that never did -- cannot be swapped without saying so.
            // Shard 1, named outright. Deriving it as "the shard f0 did not land on" was how the first
            // version of this aimed the batch at node a's own shard half the time, forwarded nothing, and
            // could not have noticed the wire mispairing outcomes with documents.
            final int remoteShard = 1;
            assertTrue(
                "the batch must be aimed at a shard this node does not hold, or nothing is forwarded",
                a.reconciler().openShards().stream().noneMatch(shard -> shard.id() == remoteShard)
            );
            final List<String> remote = idsRoutingTo(descriptor, remoteShard, 2);
            final String present = remote.get(0);
            // Never written, and chosen by routing rather than by decorating a written id -- prefixing one
            // changes its hash, which is how the first version of this landed the two ids on different
            // shards and asserted nothing about pairing at all.
            final String absent = remote.get(1);
            assertEquals("the fixture needs both ids on one shard", shardOf(descriptor, present), shardOf(descriptor, absent));

            assertEquals(200, send(a, "POST", "/alpha/_bulk?refresh=true", indexLine(present, 99)).status());
            final Response mixed = send(a, "POST", "/alpha/_bulk?refresh=true", deleteLine(present) + deleteLine(absent));
            assertEquals("the mixed batch should be accepted: " + mixed.body(), 200, mixed.status());

            final var results = resultsById(mixed.body());
            assertEquals("both items should be reported: " + mixed.body(), 2, results.size());
            assertEquals("the document that existed must be reported as deleted", "deleted", results.get(present));
            assertEquals("the one that never existed must not be reported as deleted", "not_found", results.get(absent));
        }
    }

    private static int shardOf(IndexDescriptor descriptor, String id) {
        return org.opensearch.serverless.rest.DocumentRouting.shardFor(descriptor, id);
    }

    /** Finds ids that route to one particular shard, so a batch can be aimed at a chosen node. */
    private static List<String> idsRoutingTo(IndexDescriptor descriptor, int shard, int howMany) {
        final List<String> found = new ArrayList<>();
        for (int i = 0; found.size() < howMany; i++) {
            final String candidate = "aimed-" + i;
            if (shardOf(descriptor, candidate) == shard) {
                found.add(candidate);
            }
        }
        return found;
    }

    /**
     * Pairs each item's id with its result, straight out of the response body.
     *
     * <p>Positional: the point is that the id and the result travelled together, so reading them as a
     * pair is the only reading that can catch them being separated.
     */
    private static java.util.Map<String, String> resultsById(String body) {
        final var pattern = java.util.regex.Pattern.compile("\"_id\":\"([^\"]+)\"[^}]*?\"result\":\"([^\"]+)\"");
        final var matcher = pattern.matcher(body);
        final var results = new java.util.LinkedHashMap<String, String>();
        while (matcher.find()) {
            results.put(matcher.group(1), matcher.group(2));
        }
        return results;
    }

    /**
     * A one-record append is byte-identical to what was written before batching existed.
     *
     * <p>The batch container is newline-delimited, so a single record is a single line with no delimiter
     * in it — which is exactly the blob the pre-batching writer produced. That is what makes a log written
     * by an older node readable by this one with no version field and no migration, and it is a property
     * of the encoding rather than of any code path, so it is checked directly on the bytes.
     */
    public void testASingleRecordBlobIsUnchangedByBatching() throws Exception {
        final java.nio.file.Path objectStore = createTempDir();
        final var store = new FsBlobStore(1024, objectStore, false);
        final var wal = new org.opensearch.serverless.store.WalStore(store, BlobPath.cleanPath().add("shard"));

        wal.append(1L, new org.opensearch.serverless.store.WalRecord("solo", "{\"msg\":\"one\"}"));
        wal.append(
            1L,
            List.of(
                new org.opensearch.serverless.store.WalRecord("a", "{\"msg\":\"two\"}"),
                new org.opensearch.serverless.store.WalRecord("b", "{\"msg\":\"two\"}")
            )
        );

        final var container = store.blobContainer(BlobPath.cleanPath().add("shard").add("wal").add("t=1"));
        final List<String> names = new ArrayList<>(container.listBlobs().keySet());
        // Only records. The test framework plants foreign files in temp directories at random -- this
        // assertion caught an `extra0` on roughly one run in ten -- which is precisely the case
        // WalStore filters for and says it filters for: a foreign object sharing the container is not
        // ours to interpret. A test that assumed otherwise was asserting something the production code
        // deliberately does not promise.
        names.removeIf(name -> name.matches("\\d{20}") == false);
        names.sort(java.util.Comparator.naturalOrder());
        assertEquals("two appends, two blobs, saw " + names, 2, names.size());

        final byte[] single;
        try (var in = container.readBlob(names.get(0))) {
            single = in.readAllBytes();
        }
        final byte[] batched;
        try (var in = container.readBlob(names.get(1))) {
            batched = in.readAllBytes();
        }
        assertEquals("a one-record blob must carry no delimiter at all", -1, indexOfNewline(single));
        assertTrue("a two-record blob must carry exactly one delimiter", indexOfNewline(batched) > 0);

        final var replayed = wal.replayable();
        assertEquals("all three records must come back", 3, replayed.size());
        assertEquals("and in order", "solo", replayed.get(0).id());
        assertEquals("and in order", "a", replayed.get(1).id());
        assertEquals("and in order", "b", replayed.get(2).id());
    }

    private static int indexOfNewline(byte[] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == '\n') {
                return i;
            }
        }
        return -1;
    }

    private int hits(ServerlessNode node, String field, String value) throws Exception {
        int total = 0;
        for (ShardId shardId : node.reconciler().openShards()) {
            total += (int) org.opensearch.serverless.shard.ShardQuery.execute(node.searchService(), shardId, field, value, 200).total();
        }
        return total;
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
     * The shard groups of one batch run at the same time, not one after another.
     *
     * <p>The class documentation said they ran sequentially long after they had stopped doing so, and
     * nothing asserted either version — so the code and its description could disagree indefinitely. A
     * batch spanning three shards on three nodes is three round trips if the groups are serialised, which
     * is paying per shard instead of per document: the same mistake batching exists to remove, one level
     * up.
     *
     * <p><b>Asserted as a rendezvous, not as a stopwatch.</b> Each shard's first log append blocks until
     * every shard has arrived, and fails if they do not. Run one at a time, the first group waits for peers
     * that cannot arrive until it returns, times out, and its items fail. There is no threshold to tune:
     * sequential cannot pass this, and concurrent cannot fail it.
     *
     * <p>The rendezvous is armed after the fixture is in place, because the fixture goes through the same
     * store — without that, the setup's writes are the ones that meet and the batch never rendezvouses at
     * all, which would pass whatever the dispatch did.
     */
    public void testTheShardGroupsOfOneBatchRunAtTheSameTime() throws Exception {
        final int shards = 3;
        final FsBlobStore disk = new FsBlobStore(1024, createTempDir(), false);
        // Generous: the point is that a sequential dispatch cannot finish at all, not that it is slow.
        final RendezvousBlobStore store = new RendezvousBlobStore(disk, shards, 5_000L, RendezvousBlobStore.Meet.WRITES);
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        final IndexDescriptor descriptor = new IndexDescriptor("alpha", "uuid-alpha-00000000", shards, MAPPING, null);
        plane.createIndex(descriptor);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("bulk-rendezvous"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            for (int shard = 0; shard < shards; shard++) {
                loop.want("alpha", shard);
            }
            loop.tick(clock.get());
            assertEquals("the node must hold every shard", shards, node.reconciler().openShards().size());

            // One id per shard, chosen rather than hoped for: a batch that happened to miss a shard could
            // never complete the rendezvous, and the test would fail for a reason that is not the one it is
            // about.
            final String[] idForShard = new String[shards];
            for (int candidate = 0; idForShard[0] == null || idForShard[1] == null || idForShard[2] == null; candidate++) {
                final String id = "r" + candidate;
                idForShard[org.opensearch.serverless.rest.DocumentRouting.shardFor(descriptor, id)] = id;
                assertTrue("no ids route to three shards, which cannot happen with this hash", candidate < 1_000);
            }

            final StringBuilder batch = new StringBuilder();
            for (String id : idForShard) {
                batch.append("{\"index\":{\"_index\":\"alpha\",\"_id\":\"").append(id).append("\"}}\n");
                batch.append("{\"msg\":\"rendezvous\"}\n");
            }

            store.arm();
            final Response got = send(node, "POST", "/_bulk?refresh=true", batch.toString());
            assertEquals("the batch should have been accepted: " + got.body(), 200, got.status());
            assertFalse("and every item should have landed: " + got.body(), got.body().contains("\"error\""));
            assertTrue(
                "every shard group must have reached the rendezvous, which only concurrent dispatch can do",
                store.everyoneArrived()
            );
        }
    }
}
