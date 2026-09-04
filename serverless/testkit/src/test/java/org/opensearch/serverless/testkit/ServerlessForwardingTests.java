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
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SystemIndexPlugin;
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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * What a forwarded request must carry back, and what the far side must refuse.
 *
 * <p>Every fixture here has two nodes and aims a request at the shard the receiving node does not hold,
 * because the bugs these pin all had the same shape: the same request answered one way through the owner
 * and another way through any other node. A conflict that was 409 locally and 400 forwarded; a get that
 * carried a sequence number locally and the unassigned sentinel forwarded; a refusal the owner phrased in
 * words the sender did not recognise. A client cannot see which node it reached, so it cannot code
 * against any of those.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessForwardingTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-forwarding")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private MetadataPlane plane(AtomicLong clock) throws Exception {
        return new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
    }

    /** Two nodes, one shard each, so anything aimed at shard 1 through node a crosses the wire. */
    private static final class Pair implements AutoCloseable {
        final ServerlessNode a;
        final ServerlessNode b;
        final BackgroundReconciler loopA;
        final BackgroundReconciler loopB;

        Pair(ServerlessNode a, ServerlessNode b, MetadataPlane plane, AtomicLong clock) throws Exception {
            this.a = a;
            this.b = b;
            a.start();
            b.start();
            a.setMetadataPlane(plane);
            b.setMetadataPlane(plane);
            loopA = new BackgroundReconciler(a, plane);
            loopA.want("alpha", 0);
            loopA.tick(clock.get());
            loopB = new BackgroundReconciler(b, plane);
            loopB.want("alpha", 1);
            loopB.tick(clock.get());
            assertTrue(
                "node a must not hold shard 1, or nothing is forwarded",
                a.reconciler().openShards().stream().noneMatch(s -> s.id() == 1)
            );
            assertTrue("node b must hold shard 1", b.reconciler().openShards().stream().anyMatch(s -> s.id() == 1));
        }

        @Override
        public void close() throws Exception {
            a.close();
            b.close();
        }
    }

    /**
     * A {@code create} of an id that exists, forwarded, is a 409 -- the same 409 the owner answers.
     *
     * <p>The owner always knew: its outcome carried the conflict flag. The flag simply did not travel, so
     * every forwarded failure was rebuilt on the sender as an ordinary one and rendered 400
     * {@code operation_failed}. A client library that stops retrying a compare-and-swap on 409 and keeps
     * retrying on 400 would have looped on it through one node and stopped through the other.
     */
    public void testAForwardedCreateOfAnExistingIdIsAVersionConflict() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 2, MAPPING, null));

        try (
            Pair pair = new Pair(
                new ServerlessNode(nodeSettings("fwd-conflict-a")),
                new ServerlessNode(nodeSettings("fwd-conflict-b")),
                plane,
                clock
            )
        ) {
            final IndexDescriptor descriptor = plane.describe("alpha").orElseThrow();
            final String remote = idRoutingTo(descriptor, 1);
            final String create = "{\"create\":{\"_index\":\"alpha\",\"_id\":\"" + remote + "\"}}\n{\"msg\":\"once\",\"n\":1}\n";

            final Response first = send(pair.a, "POST", "/_bulk?refresh=true", create);
            assertEquals(first.body(), 200, first.status());
            assertTrue("the first create lands: " + first.body(), first.body().contains("\"status\":201"));
            assertTrue("and was applied by the owner: " + first.body(), first.body().contains(pair.b.localNode().getId()));

            final Response second = send(pair.a, "POST", "/_bulk?refresh=true", create);
            assertEquals(second.body(), 200, second.status());
            assertTrue(
                "the second create must be a conflict, not a malformed operation: " + second.body(),
                second.body().contains("\"status\":409")
            );
            assertTrue("with core's type: " + second.body(), second.body().contains("version_conflict_engine_exception"));
            assertFalse("and never a 400: " + second.body(), second.body().contains("\"status\":400"));

            // The same request through the owner says the same thing, which is the whole point.
            final Response local = send(pair.b, "POST", "/_bulk?refresh=true", create);
            assertTrue("the owner agrees: " + local.body(), local.body().contains("\"status\":409"));
        }
    }

    /**
     * A forwarded get carries the document's sequence identity, and a conditional write built from it holds.
     *
     * <p>The owner read the numbers out of the same lookup that found the source; the response simply did
     * not serialise them, and the sender rebuilt the document with the unassigned sentinels. So every
     * forwarded get answered {@code _seq_no: -2}, and the {@code if_seq_no=-2} a caller sent back lost.
     */
    public void testAForwardedGetCarriesTheSequenceIdentityACallerCanConditionOn() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 2, MAPPING, null));

        try (
            Pair pair = new Pair(new ServerlessNode(nodeSettings("fwd-get-a")), new ServerlessNode(nodeSettings("fwd-get-b")), plane, clock)
        ) {
            final IndexDescriptor descriptor = plane.describe("alpha").orElseThrow();
            final String remote = idRoutingTo(descriptor, 1);
            final Response written = send(pair.a, "PUT", "/alpha/_doc/" + remote, "{\"msg\":\"forwarded\",\"n\":1}");
            assertEquals(written.body(), 201, written.status());

            final Response got = send(pair.a, "GET", "/alpha/_doc/" + remote, null);
            assertEquals(got.body(), 200, got.status());
            assertTrue("answered by the owner: " + got.body(), got.body().contains(pair.b.localNode().getId()));
            final long seqNo = number(got.body(), "_seq_no");
            final long primaryTerm = number(got.body(), "_primary_term");
            final long version = number(got.body(), "_version");
            assertTrue("a forwarded get must carry a real sequence number, not the sentinel: " + got.body(), seqNo >= 0);
            assertTrue("and a real primary term: " + got.body(), primaryTerm >= 1);
            assertTrue("and a real version: " + got.body(), version >= 1);
            assertEquals("the same identity the write reported", number(written.body(), "_seq_no"), seqNo);

            // The token round-trips: a write conditioned on what the get reported succeeds...
            final Response conditioned = send(
                pair.a,
                "PUT",
                "/alpha/_doc/" + remote + "?if_seq_no=" + seqNo + "&if_primary_term=" + primaryTerm,
                "{\"msg\":\"forwarded\",\"n\":2}"
            );
            assertEquals("a conditional write built from a forwarded get must hold: " + conditioned.body(), 200, conditioned.status());
            // ...and the same condition, now stale, loses -- so the comparison is real and not a pass-through.
            final Response stale = send(
                pair.a,
                "PUT",
                "/alpha/_doc/" + remote + "?if_seq_no=" + seqNo + "&if_primary_term=" + primaryTerm,
                "{\"msg\":\"forwarded\",\"n\":3}"
            );
            assertEquals("the used-up token must lose: " + stale.body(), 409, stale.status());

            // _mget through the same node reports the same numbers.
            final Response multi = send(pair.a, "POST", "/_mget", "{\"docs\":[{\"_index\":\"alpha\",\"_id\":\"" + remote + "\"}]}");
            assertEquals(multi.body(), 200, multi.status());
            assertTrue("a forwarded multi-get carries the identity too: " + multi.body(), number(multi.body(), "_seq_no") > seqNo);
        }
    }

    /**
     * A write forwarded, on a stale hint, to a node that holds the shard only as a search reader is
     * refused as "not the owner" and re-routed to the node that is.
     *
     * <p>The receiving side used to resolve the shard by name, which found the reader, and the write then
     * failed inside the node with "open as a reader" -- a message the sender did not recognise as a stale
     * hint, so the hint stayed and every later write through the sender failed the same way until the
     * reader was evicted.
     */
    public void testAWriteForwardedToANodeHoldingTheShardAsAReaderIsReRoutedToTheOwner() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode coordinator = new ServerlessNode(nodeSettings("fwd-reader-c"));
            ServerlessNode first = new ServerlessNode(nodeSettings("fwd-reader-a"));
            ServerlessNode second = new ServerlessNode(nodeSettings("fwd-reader-b"))
        ) {
            coordinator.start();
            first.start();
            second.start();
            coordinator.setMetadataPlane(plane);
            first.setMetadataPlane(plane);
            second.setMetadataPlane(plane);
            final BackgroundReconciler loopFirst = new BackgroundReconciler(first, plane);
            loopFirst.want("alpha", 0);
            loopFirst.tick(clock.get());
            assertEquals(first.localNode().getId(), plane.heads().read("alpha", 0).orElseThrow().ownerNodeId());

            // One write through the coordinator, so it remembers the first node as the owner, and one
            // publish, so a reader has a commit to open later.
            assertEquals(201, send(coordinator, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"first\",\"n\":1}").status());
            assertEquals(first.localNode().getId(), coordinator.ownerHint("alpha", 0).orElseThrow());
            final var headNamingFirst = plane.heads().read("alpha", 0).orElseThrow();
            assertEquals(1, loopFirst.tick(clock.get() + 1_000).published().size());

            // The first node's lease lapses, the second takes the shard, and the first notices and lets
            // go on its next heartbeat -- which also renews its own lease, so it is reachable again.
            clock.set(clock.get() + TTL + 1_000);
            final ShardId onSecond = second.activateWriter(plane, "alpha", 0).orElseThrow();
            // A hint older than a lease is dropped by age, which is right and is not the case under test.
            // The case is a hint refreshed just before the takeover -- the coordinator's own hint rebuild
            // reading the head a moment before the first node lost it -- so it is re-noted here, fresh,
            // still naming the first node.
            coordinator.noteHead("alpha", 0, headNamingFirst);
            assertEquals(1, first.heartbeat(plane).size());
            assertTrue("the first node must have released the writer", first.reconciler().openShards().isEmpty());
            // ...and then opens the same shard as a search reader, which is the state the bug needs.
            final ShardId asReader = first.serveAsReader(plane, "alpha", 0);
            assertTrue(
                "the fixture needs the first node to hold the shard as a reader",
                first.reconciler().readerShards().contains(asReader)
            );

            // The coordinator still believes the first node owns the shard.
            assertEquals(first.localNode().getId(), coordinator.ownerHint("alpha", 0).orElseThrow());
            final Response written = send(coordinator, "PUT", "/alpha/_doc/2?refresh=true", "{\"msg\":\"second\",\"n\":2}");
            assertEquals("the write must be re-routed to the owner, not fail on the reader: " + written.body(), 201, written.status());
            assertTrue("and applied by the second node: " + written.body(), written.body().contains(second.localNode().getId()));
            assertEquals("the hint now names the owner", second.localNode().getId(), coordinator.ownerHint("alpha", 0).orElseThrow());
            assertEquals(
                "the document is where the owner can see it",
                1L,
                ShardOps.hits(second.searchService(), onSecond, "msg", "second")
            );
        }
    }

    /** A plugin that owns one system index, so the node has something to guard. */
    public static final class ProbeStatePlugin extends Plugin implements SystemIndexPlugin {
        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(new SystemIndexDescriptor(".probe_state", "a plugin's own records"));
        }
    }

    /**
     * {@code _mget} cannot reach a system index by naming it in the body.
     *
     * <p>The registration-time guard reads the index in the request path, and {@code POST /_mget} has
     * none -- so {@code {"docs":[{"_index":".serverless_auth","_id":"admin"}]}} returned the stored
     * credential record to anyone who could spell it. Both spellings are tried, and the source must not
     * appear in either answer.
     */
    public void testAMultiGetCannotReachASystemIndexThroughTheBody() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor(".probe_state", "uuid-probe-000000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("mget-system"), List.of(new ProbeStatePlugin()))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want(".probe_state", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();
            // Written from inside the node, as the plugin would write it: REST cannot, and must not, put it there.
            node.index(shardId, "secret", "{\"msg\":\"the-record-nobody-may-read\",\"n\":42}");

            final Response bodyAddressed = send(node, "POST", "/_mget", "{\"docs\":[{\"_index\":\".probe_state\",\"_id\":\"secret\"}]}");
            assertFalse("the record must not be disclosed: " + bodyAddressed.body(), bodyAddressed.body().contains("nobody-may-read"));
            assertTrue("and the item must say why: " + bodyAddressed.body(), bodyAddressed.body().contains("system_index"));
            assertTrue(
                "in the words every other refusal uses: " + bodyAddressed.body(),
                bodyAddressed.body().contains("belongs to a plugin")
            );

            final Response pathAddressed = send(node, "POST", "/.probe_state/_mget", "{\"ids\":[\"secret\"]}");
            assertEquals("the path form is refused outright: " + pathAddressed.body(), 403, pathAddressed.status());
            assertFalse(pathAddressed.body(), pathAddressed.body().contains("nobody-may-read"));

            // An ordinary index in the same request still answers, so the refusal is per item.
            plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
            final BackgroundReconciler loopAlpha = new BackgroundReconciler(node, plane);
            loopAlpha.want("alpha", 0);
            loopAlpha.tick(clock.get());
            assertEquals(201, send(node, "PUT", "/alpha/_doc/ok", "{\"msg\":\"public\",\"n\":1}").status());
            final Response mixed = send(
                node,
                "POST",
                "/_mget",
                "{\"docs\":[{\"_index\":\"alpha\",\"_id\":\"ok\"},{\"_index\":\".probe_state\",\"_id\":\"secret\"}]}"
            );
            assertEquals(mixed.body(), 200, mixed.status());
            assertTrue("the public document answers: " + mixed.body(), mixed.body().contains("\"public\""));
            assertFalse("the guarded one does not: " + mixed.body(), mixed.body().contains("nobody-may-read"));
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

    /** The first occurrence of a numeric field in a JSON body, without a parser: the bodies here are small and flat. */
    private static long number(String body, String field) {
        final Matcher matcher = Pattern.compile("\"" + field + "\":(-?\\d+)").matcher(body);
        assertTrue("expected " + field + " in " + body, matcher.find());
        return Long.parseLong(matcher.group(1));
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
