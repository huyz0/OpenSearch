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
