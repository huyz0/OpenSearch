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
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A node that joins a fleet at its cap takes work from it: the full nodes say how full they are, hand idle shards off
 * when someone has room, and send writes for shards nobody holds to the member with room rather than evict for them.
 *
 * <p>Without this a new node took only what happened to arrive at it, and the full ones stayed full until their
 * shards went idle five minutes later.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessScaleOutTests extends OpenSearchTestCase {

    private static final long TTL = 300_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings settings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-scale-out")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** A lease carries its node's load, and one written before it did reads as not saying. */
    public void testALeaseCarriesItsLoad() throws Exception {
        final NodeLease lease = new NodeLease("n", "e", "a", Set.of("ingest"), 10L, "name", "v").withLoad(7, 40);
        final NodeLease read = NodeLease.fromStream(
            new ByteArrayInputStream(org.opensearch.core.common.bytes.BytesReference.toBytes(lease.toBytes()))
        );
        assertEquals(7, read.held());
        assertEquals(40, read.cap());
        assertEquals("renewal keeps it", 7, read.renewedUntil(20L).held());
        final NodeLease old = new NodeLease("n", "e", "a", Set.of("ingest"), 10L);
        final NodeLease oldRead = NodeLease.fromStream(
            new ByteArrayInputStream(org.opensearch.core.common.bytes.BytesReference.toBytes(old.toBytes()))
        );
        assertEquals(-1, oldRead.held());
        assertEquals(-1, oldRead.cap());
    }

    public void testAFullNodeHandsOffAndSteersToAMemberWithRoom() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final List<String> indices = List.of("i0", "i1", "i2", "i3", "i4", "fresh");
        for (String index : indices) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index, 1, MAPPING, null));
        }

        try (ServerlessNode full = new ServerlessNode(settings("full")); ServerlessNode roomy = new ServerlessNode(settings("roomy"))) {
            full.start();
            full.setMetadataPlane(plane);
            roomy.start();
            roomy.setMetadataPlane(plane);
            final BackgroundReconciler fullLoop = new BackgroundReconciler(full, plane).setDemandDrivenActivation(true)
                .setMaxShardsHeld(4)
                .setEvictAfterMillis(30_000L);
            final BackgroundReconciler roomyLoop = new BackgroundReconciler(roomy, plane).setDemandDrivenActivation(true)
                .setMaxShardsHeld(4)
                .setEvictAfterMillis(30_000L);
            // Activations on demand need a scheduler to run them; the long intervals keep it from doing anything else.
            final org.opensearch.serverless.reconcile.ReconcileScheduler fullScheduler = scheduler(fullLoop, full, clock);
            final org.opensearch.serverless.reconcile.ReconcileScheduler roomyScheduler = scheduler(roomyLoop, roomy, clock);
            full.setSignals(fullScheduler);
            roomy.setSignals(roomyScheduler);
            fullScheduler.start();
            roomyScheduler.start();
            full.renewLease(plane);
            roomy.renewLease(plane);
            for (String index : List.of("i0", "i1", "i2", "i3")) {
                fullLoop.activateOnDemand(List.of(Map.entry(index, 0)));
            }
            assertEquals(4, full.capacity().held());

            // A minute on, every shard on the full node is past its grace, and the other member holds nothing.
            clock.addAndGet(60_000L);
            full.renewLease(plane);
            roomy.renewLease(plane);
            plane.membership().refresh();
            assertTrue("the empty member has room", full.memberWithRoom(plane).isPresent());
            assertEquals("a tenth of the cap a pass, at least one", 1, fullLoop.handOffToMemberWithRoom());
            assertEquals(1, full.capacity().handedOff());
            assertEquals(3, full.capacity().held());

            // Full again, a write for a shard nobody holds is taken by the member with room.
            fullLoop.activateOnDemand(List.of(Map.entry("i4", 0)));
            full.renewLease(plane);
            plane.membership().refresh();
            assertTrue(full.nearlyFull());
            final int status = send(full, "PUT", "/fresh/_doc/1", "{\"msg\":\"x\"}");
            assertEquals(201, status);
            assertEquals("the write went to the member with room", 1, full.capacity().writesSteered());
            assertEquals(roomy.localNode().getId(), plane.heads().read("fresh", 0).orElseThrow().ownerNodeId());
            assertFalse(full.reconciler().openShards().stream().anyMatch(s -> s.getIndexName().equals("fresh")));
            fullScheduler.close();
            roomyScheduler.close();
        }
    }

    private static org.opensearch.serverless.reconcile.ReconcileScheduler scheduler(
        BackgroundReconciler loop,
        ServerlessNode node,
        AtomicLong clock
    ) {
        return new org.opensearch.serverless.reconcile.ReconcileScheduler(
            loop,
            node.threadPool(),
            clock::get,
            org.opensearch.common.unit.TimeValue.timeValueHours(1),
            org.opensearch.common.unit.TimeValue.timeValueHours(1),
            null
        );
    }

    /**
     * A full node asked for a shard another live node already holds evicts nothing for it. Survivors of a dead node all
     * queue all of its shards; the ones another survivor took first each cost an eviction for nothing.
     */
    public void testAFullNodeDoesNotEvictForAShardSomeoneElseHolds() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        for (String index : List.of("mine", "theirs")) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index, 1, MAPPING, null));
        }
        try (ServerlessNode a = new ServerlessNode(settings("a")); ServerlessNode b = new ServerlessNode(settings("b"))) {
            a.start();
            a.setMetadataPlane(plane);
            b.start();
            b.setMetadataPlane(plane);
            final BackgroundReconciler aLoop = new BackgroundReconciler(a, plane).setDemandDrivenActivation(true)
                .setMaxShardsHeld(1)
                .setEvictAfterMillis(30_000L);
            final BackgroundReconciler bLoop = new BackgroundReconciler(b, plane).setDemandDrivenActivation(true);
            a.renewLease(plane);
            b.renewLease(plane);
            aLoop.activateOnDemand(List.of(Map.entry("mine", 0)));
            bLoop.activateOnDemand(List.of(Map.entry("theirs", 0)));
            clock.addAndGet(60_000L);   // "mine" is past its grace: evictable
            a.renewLease(plane);
            b.renewLease(plane);

            assertTrue("not taken", aLoop.activateOnDemand(List.of(Map.entry("theirs", 0))).isEmpty());
            assertEquals("and nothing evicted for it", 0, a.capacity().evicted());
            assertTrue(a.reconciler().openShards().stream().anyMatch(s -> s.getIndexName().equals("mine")));
        }
    }

    /**
     * An activation on a node whose lease lapsed does not wait behind a re-read of its held heads that another thread is
     * already running: it declines, and is asked again once that re-read is done. Waiting put every activation thread
     * behind the lease thread's scan on one lock: with the store slowed to seconds a request, every activation on every
     * node waited an hour.
     */
    public void testAnActivationDoesNotWaitBehindAHeadReRead() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        final HookedBlobStore store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        for (String index : List.of("held", "wanted")) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index, 1, MAPPING, null));
        }
        try (ServerlessNode node = new ServerlessNode(settings("lapsed"))) {
            node.start();
            node.setMetadataPlane(plane);
            node.renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true);
            loop.activateOnDemand(List.of(Map.entry("held", 0)));

            clock.addAndGet(2 * TTL);   // the lease has lapsed
            // The lease pass renews and re-reads the held heads; the read of one is held in flight.
            final java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
            final java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
            store.onNextRegisterRead("", org.opensearch.serverless.metadata.RegisterMap.shardHeadBlob("held", 0), () -> {
                entered.countDown();
                try {
                    release.await(30, java.util.concurrent.TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            final Thread leasePass = new Thread(() -> {
                try {
                    node.renewLease(plane);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            leasePass.start();
            assertTrue(entered.await(30, java.util.concurrent.TimeUnit.SECONDS));

            final long started = System.nanoTime();
            assertTrue(
                "not taken while another thread re-reads the held heads",
                loop.activateOnDemand(List.of(Map.entry("wanted", 0))).isEmpty()
            );
            assertTrue("and without waiting for it", System.nanoTime() - started < java.util.concurrent.TimeUnit.SECONDS.toNanos(10));

            release.countDown();
            leasePass.join(30_000L);
            assertEquals("taken once the re-read is done", 1, loop.activateOnDemand(List.of(Map.entry("wanted", 0))).size());
        }
    }

    /**
     * The fleet's size, from its members' leases: demand -- shards used in the last minute, and turned away at a cap -- over
     * the average cap at the target utilisation. A fleet turning shards away asks for more nodes; one full only of idle
     * shards asks for fewer.
     */
    public void testTheFleetSaysHowManyNodesItWants() throws Exception {
        assertEquals("busy and turning work away: more than the two it has", 4, wanted(true));
        assertEquals("full of idle shards and turning nothing away: fewer", 1, wanted(false));
    }

    private int wanted(boolean busy) throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        for (String index : List.of("a0", "a1", "a2", "b0", "b1", "b2")) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index, 1, MAPPING, null));
        }
        try (ServerlessNode a = new ServerlessNode(settings("a")); ServerlessNode b = new ServerlessNode(settings("b"))) {
            final List<ServerlessNode> nodes = List.of(a, b);
            final List<String> prefixes = List.of("a", "b");
            for (int n = 0; n < 2; n++) {
                final ServerlessNode node = nodes.get(n);
                node.start();
                node.setMetadataPlane(plane);
                node.renewLease(plane);
                final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true)
                    .setMaxShardsHeld(2)
                    .setEvictAfterMillis(0L);   // refuse rather than evict
                for (int i = 0; i < 2; i++) {
                    final String index = prefixes.get(n) + i;
                    loop.activateOnDemand(List.of(Map.entry(index, 0)));
                    node.index(
                        node.reconciler().openShards().stream().filter(s -> s.getIndexName().equals(index)).findFirst().orElseThrow(),
                        "doc",
                        "{\"msg\":\"x\"}"
                    );
                }
                if (busy) {
                    assertTrue("full: refused", loop.activateOnDemand(List.of(Map.entry(prefixes.get(n) + "2", 0))).isEmpty());
                }
            }
            if (busy == false) {
                clock.addAndGet(120_000L);   // nothing used for two minutes
            }
            for (ServerlessNode node : nodes) {
                node.renewLease(plane);
            }
            plane.membership().refresh();
            final ServerlessNode.FleetSize size = a.fleetSize(plane);
            assertEquals(2, size.members());
            return size.wanted();
        }
    }

    /** With every member as full as this one, nothing is handed off and nothing steered. */
    public void testNothingMovesWhenNobodyHasRoom() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        for (String index : List.of("a0", "a1", "b0", "b1")) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index, 1, MAPPING, null));
        }
        try (ServerlessNode a = new ServerlessNode(settings("a")); ServerlessNode b = new ServerlessNode(settings("b"))) {
            a.start();
            a.setMetadataPlane(plane);
            b.start();
            b.setMetadataPlane(plane);
            final BackgroundReconciler aLoop = new BackgroundReconciler(a, plane).setDemandDrivenActivation(true)
                .setMaxShardsHeld(2)
                .setEvictAfterMillis(30_000L);
            final BackgroundReconciler bLoop = new BackgroundReconciler(b, plane).setDemandDrivenActivation(true)
                .setMaxShardsHeld(2)
                .setEvictAfterMillis(30_000L);
            a.renewLease(plane);
            b.renewLease(plane);
            aLoop.activateOnDemand(List.of(Map.entry("a0", 0), Map.entry("a1", 0)));
            bLoop.activateOnDemand(List.of(Map.entry("b0", 0), Map.entry("b1", 0)));
            clock.addAndGet(60_000L);
            a.renewLease(plane);
            b.renewLease(plane);
            plane.membership().refresh();
            assertTrue(a.memberWithRoom(plane).isEmpty());
            assertEquals(0, aLoop.handOffToMemberWithRoom());
            assertEquals(2, a.capacity().held());
        }
    }

    private static int send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .method(method, HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
            return client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode();
        }
    }
}
