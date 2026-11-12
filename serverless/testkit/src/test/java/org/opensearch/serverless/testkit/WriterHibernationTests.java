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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.ShardHead;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.reconcile.ReconcileScheduler;
import org.opensearch.serverless.rest.DocumentRouting;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A writer that hibernates loses nothing, wakes at the term it slept at, and gives way to anyone who took its head
 * while it slept.
 *
 * <p>The negative cases carry the weight, as for idle release: a writer in use must never hibernate, and a head that
 * moved while the writer slept must never be woken into.
 */
public class WriterHibernationTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final long HIBERNATE_AFTER = 30_000L;
    private static final long IDLE_AFTER = 600_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-hibernation")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private MetadataPlane plane(AtomicLong clock, int shards) throws Exception {
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", shards, MAPPING, null));
        return plane;
    }

    private static BackgroundReconciler loop(ServerlessNode node, MetadataPlane plane) {
        return new BackgroundReconciler(node, plane).setDemandDrivenActivation(true)
            .setHibernateAfterMillis(HIBERNATE_AFTER)
            .setIdleAfterMillis(IDLE_AFTER);
    }

    private static long count(ServerlessNode node, ShardId shardId) throws Exception {
        final var shard = node.reconciler().shard(shardId);
        shard.refresh("hibernation-test");
        try (var searcher = shard.acquireSearcher("hibernation-test")) {
            return searcher.getIndexReader().numDocs();
        }
    }

    private static ShardId writeSome(ServerlessNode node, MetadataPlane plane, int shard, int from, int count) throws Exception {
        final ShardId shardId = node.activation().activateWriter(plane, "alpha", shard).orElseThrow();
        for (int i = from; i < from + count; i++) {
            node.writes().index(shardId, "d" + i, "{\"msg\":\"hello\",\"n\":" + i + "}");
        }
        return shardId;
    }

    /** The whole cycle: hibernate with the head kept, wake at the same term, and carry on numbering where it stopped. */
    public void testAnIdleWriterHibernatesKeepsItsHeadAndWakesAtTheSameTerm() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, 1);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("sleeper"))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = loop(node, plane);
            final ShardId first = writeSome(node, plane, 0, 0, 5);
            final long term = node.reconciler().shard(first).getOperationPrimaryTerm();
            final long maxSeqNo = node.reconciler().shard(first).seqNoStats().getMaxSeqNo();
            loop.tick(clock.get());
            assertEquals("not idle yet", 1, node.reconciler().openShards().size());

            clock.addAndGet(HIBERNATE_AFTER + 1);
            loop.tick(clock.get());
            assertTrue("hibernated: closed here", node.reconciler().openShards().isEmpty());
            assertTrue(node.hibernation().isHibernated("alpha", 0));
            final ShardHead head = plane.heads().read("alpha", 0).orElseThrow();
            assertEquals("and the head kept", node.localNode().getId(), head.ownerNodeId());
            assertEquals("at the same term", term, head.term());

            final ShardId woken = writeSome(node, plane, 0, 5, 1);
            assertEquals("woken at the term it slept at", term, node.reconciler().shard(woken).getOperationPrimaryTerm());
            assertEquals("and the head never moved", term, plane.heads().read("alpha", 0).orElseThrow().term());
            assertEquals("numbering carries on where it stopped", maxSeqNo + 1, node.reconciler().shard(woken).seqNoStats().getMaxSeqNo());
            assertEquals("every write is there", 6, count(node, woken));
            assertEquals(1, node.hibernation().wakes());
            assertEquals(0, node.hibernation().missedWakes());
            assertFalse(node.hibernation().isHibernated("alpha", 0));

            // And again: a second sleep and wake is the same as the first.
            clock.addAndGet(HIBERNATE_AFTER + 1);
            loop.tick(clock.get());
            assertTrue(node.hibernation().isHibernated("alpha", 0));
            final ShardId again = writeSome(node, plane, 0, 6, 1);
            assertEquals(term, node.reconciler().shard(again).getOperationPrimaryTerm());
            assertEquals(7, count(node, again));
        }
    }

    /** A writer used more often than the threshold never hibernates, by writes or by searches. */
    public void testAWriterInUseNeverHibernates() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, 1);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("busy"))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = loop(node, plane);
            ShardId shardId = writeSome(node, plane, 0, 0, 1);
            for (int round = 0; round < 6; round++) {
                clock.addAndGet(HIBERNATE_AFTER - 1_000);
                node.writes().index(shardId, "r" + round, "{\"msg\":\"busy\",\"n\":1}");
                node.shards().markUsed(shardId);
                loop.tick(clock.get());
                assertFalse(
                    "a writer used " + (HIBERNATE_AFTER - 1_000) + "ms ago must not hibernate",
                    node.hibernation().isHibernated("alpha", 0)
                );
                assertEquals(1, node.reconciler().openShards().size());
            }
            assertEquals(0, node.hibernation().hibernations());
        }
    }

    /** Past the idle threshold the head is given back, as idle release gives back an open writer's. */
    public void testAHibernatedWriterGivesItsHeadBackAfterTheIdleThreshold() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, 1);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("giver"))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = loop(node, plane);
            writeSome(node, plane, 0, 0, 3);
            clock.addAndGet(HIBERNATE_AFTER + 1);
            loop.tick(clock.get());
            assertTrue(node.hibernation().isHibernated("alpha", 0));

            clock.addAndGet(IDLE_AFTER);
            final var result = loop.tick(clock.get());
            assertFalse(node.hibernation().isHibernated("alpha", 0));
            assertTrue("reported as released: " + result, result.released().stream().anyMatch(s -> s.getIndexName().equals("alpha")));
            assertTrue("the head given back", plane.heads().read("alpha", 0).map(ShardHead::ownerNodeId).isEmpty());

            // Taken back the ordinary way, at a new term, with everything there.
            final ShardId back = writeSome(node, plane, 0, 3, 1);
            assertEquals(4, count(node, back));
            assertEquals(0, node.hibernation().wakes());
        }
    }

    /**
     * A head that moved while the writer slept is never woken into: the shard is taken the ordinary way, which finds
     * out who holds it now.
     */
    public void testAHeadThatMovedWhileAsleepIsNotWokenInto() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, 1);
        try (ServerlessNode a = new ServerlessNode(nodeSettings("asleep")); ServerlessNode b = new ServerlessNode(nodeSettings("taker"))) {
            a.startup().start();
            a.setMetadataPlane(plane);
            b.startup().start();
            b.setMetadataPlane(plane);
            final BackgroundReconciler loopA = loop(a, plane);
            final ShardId slept = writeSome(a, plane, 0, 0, 3);
            final long term = a.reconciler().shard(slept).getOperationPrimaryTerm();
            clock.addAndGet(HIBERNATE_AFTER + 1);
            a.leases().renewLease(plane);
            loopA.tick(clock.get());
            assertTrue(a.hibernation().isHibernated("alpha", 0));

            // The head is given away behind the sleeper's back -- as an operator, or a lapse it never noticed, would.
            assertTrue(
                plane.heads().release("alpha", 0, a.localNode().getId(), term, slept.getIndex().getUUID(), a.localNode().getEphemeralId())
            );
            final ShardId taken = writeSome(b, plane, 0, 3, 2);
            assertTrue("taken at a newer term", b.reconciler().shard(taken).getOperationPrimaryTerm() > term);

            final Optional<ShardId> woken = a.activation().activateWriter(plane, "alpha", 0);
            assertTrue("the sleeper must not open a shard somebody else holds", woken.isEmpty());
            assertEquals(1, a.hibernation().missedWakes());
            assertEquals(0, a.hibernation().wakes());
            assertFalse(a.hibernation().isHibernated("alpha", 0));
            assertEquals("the taker has everything", 5, count(b, taken));
        }
    }

    /** A node that dies with writers asleep loses nothing: a successor takes them from their published commits. */
    public void testANodeThatDiesWithWritersAsleepLosesNothing() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, 2);
        final ServerlessNode a = new ServerlessNode(nodeSettings("dies-asleep"));
        try (ServerlessNode b = new ServerlessNode(nodeSettings("successor"))) {
            a.startup().start();
            a.setMetadataPlane(plane);
            final BackgroundReconciler loopA = loop(a, plane);
            writeSome(a, plane, 0, 0, 4);
            writeSome(a, plane, 1, 0, 6);
            clock.addAndGet(HIBERNATE_AFTER + 1);
            a.leases().renewLease(plane);
            loopA.tick(clock.get());
            assertEquals(2, a.hibernation().size());
            // Dies: no handover, the lease simply runs out.
            a.close();
            clock.addAndGet(TTL * 3);

            b.startup().start();
            b.setMetadataPlane(plane);
            final ShardId zero = b.activation().activateWriter(plane, "alpha", 0).orElseThrow();
            final ShardId one = b.activation().activateWriter(plane, "alpha", 1).orElseThrow();
            assertEquals(4, count(b, zero));
            assertEquals(6, count(b, one));
        } finally {
            a.close();
        }
    }

    /** A clean shutdown gives back the heads of writers asleep, as it hands over open ones. */
    public void testAShutdownGivesBackTheHeadsOfWritersAsleep() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, 1);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("shutdown"))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = loop(node, plane);
            writeSome(node, plane, 0, 0, 2);
            clock.addAndGet(HIBERNATE_AFTER + 1);
            loop.tick(clock.get());
            assertTrue(node.hibernation().isHibernated("alpha", 0));
            assertEquals(1, loop.handOverAll());
            assertTrue(plane.heads().read("alpha", 0).map(ShardHead::ownerNodeId).isEmpty());
        }
    }

    /**
     * A write that arrives at another node is forwarded to the sleeper, because the head names it, and is applied
     * there: the sleeper wakes for it rather than refusing it.
     */
    public void testAForwardedWriteWakesTheSleeperAndIsApplied() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, 2);
        final IndexDescriptor descriptor = plane.describe("alpha").orElseThrow();
        String id = null;
        String localId = null;
        for (int i = 0; localId == null; i++) {
            if (DocumentRouting.shardFor(descriptor, "doc-" + i, null) == 1) {
                if (id == null) {
                    id = "doc-" + i;
                } else {
                    localId = "doc-" + i;
                }
            }
        }
        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("front"));
            ServerlessNode b = new ServerlessNode(nodeSettings("sleeper-b"))
        ) {
            a.startup().start();
            a.setMetadataPlane(plane);
            b.startup().start();
            b.setMetadataPlane(plane);
            final BackgroundReconciler loopB = loop(b, plane);
            try (
                ReconcileScheduler scheduler = new ReconcileScheduler(
                    loopB,
                    b.threadPool(),
                    plane.clock(),
                    TimeValue.timeValueHours(1),
                    TimeValue.timeValueMillis(20),
                    null
                )
            ) {
                b.setSignals(scheduler);
                scheduler.start();
                writeSome(b, plane, 1, 0, 2);
                clock.addAndGet(HIBERNATE_AFTER + 1);
                b.leases().renewLease(plane);
                a.leases().renewLease(plane);
                plane.membership().refresh();
                loopB.tick(clock.get());
                assertTrue(b.hibernation().isHibernated("alpha", 1));

                final int status = put(a, "/alpha/_doc/" + id, "{\"msg\":\"forwarded\",\"n\":9}");
                assertEquals(201, status);
                assertEquals("woken on the sleeper, not taken by the front node", 1, b.hibernation().wakes());
                assertTrue(a.reconciler().openShards().stream().noneMatch(s -> s.id() == 1));
                final ShardId shardId = b.reconciler().openShards().stream().filter(s -> s.id() == 1).findFirst().orElseThrow();
                assertEquals(3, count(b, shardId));

                // Asleep again: a write to the sleeper itself, a bulk through the front node, and a search through either.
                clock.addAndGet(HIBERNATE_AFTER + 1);
                b.leases().renewLease(plane);
                a.leases().renewLease(plane);
                loopB.tick(clock.get());
                assertTrue(b.hibernation().isHibernated("alpha", 1));
                assertEquals(201, send(b, "PUT", "/alpha/_doc/" + localId, "{\"msg\":\"local\",\"n\":10}"));
                clock.addAndGet(HIBERNATE_AFTER + 1);
                b.leases().renewLease(plane);
                a.leases().renewLease(plane);
                loopB.tick(clock.get());
                assertTrue(b.hibernation().isHibernated("alpha", 1));
                final String bulk = "{\"index\":{\"_index\":\"alpha\",\"_id\":\"" + id + "\"}}\n{\"msg\":\"bulk\",\"n\":11}\n";
                assertEquals(200, send(a, "POST", "/_bulk?refresh=true", bulk));
                assertEquals(3, b.hibernation().wakes());
                clock.addAndGet(HIBERNATE_AFTER + 1);
                b.leases().renewLease(plane);
                a.leases().renewLease(plane);
                loopB.tick(clock.get());
                assertTrue(b.hibernation().isHibernated("alpha", 1));
                for (ServerlessNode via : new ServerlessNode[] { a, b }) {
                    final HttpResponse<String> found = request(
                        via,
                        "GET",
                        "/alpha/_search?q=msg:local%20msg:forwarded%20msg:bulk&size=0",
                        null
                    );
                    assertEquals(found.body(), 200, found.statusCode());
                    assertTrue("every write to the sleeper is found: " + found.body(), found.body().contains("\"value\":2"));
                }
            }
        }
    }

    private static int put(ServerlessNode node, String path, String body) throws Exception {
        return send(node, "PUT", path, body);
    }

    private static int send(ServerlessNode node, String method, String path, String body) throws Exception {
        return request(node, method, path, body).statusCode();
    }

    private static HttpResponse<String> request(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
            return client.send(request, HttpResponse.BodyHandlers.ofString());
        }
    }
}
