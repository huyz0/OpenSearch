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
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A writer publishing a shard it holds reads the shard's head once, not once per publish, while its own lease holds:
 * a head cannot move off a node whose lease holds, and the manifest's compare-and-swap fences a publish either way. A
 * successor's takeover is still caught: the zombie's publish is refused and its shard let go.
 */
public class PublishHeadReadTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"keyword\"}}}";

    private Settings nodeSettings(String name, boolean trust) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-publish-head")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.publish.trust_own_head", trust)
            .build();
    }

    /** Head reads by publishes over ten publishes, each a renewal and a half after the last. */
    private long headReadsByPublish(boolean trust) throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            plane.createIndex(new IndexDescriptor("heads", "uuid-heads", 1, MAPPING, null));
            try (ServerlessNode node = new ServerlessNode(nodeSettings("publish-head-" + trust, trust))) {
                node.startup().start();
                node.setMetadataPlane(plane);
                node.leases().renewLease(plane);
                final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
                loop.want("heads", 0);
                for (int pass = 0; pass < 20 && node.reconciler().openShards().isEmpty(); pass++) {
                    loop.tick(clock.get());
                }
                final ShardId shardId = node.reconciler().openShards().iterator().next();
                store.reset();
                store.traceCallers(3);
                for (int i = 0; i < 10; i++) {
                    clock.addAndGet(15_000L);
                    node.leases().renewLease(plane);
                    node.writes().index(shardId, "d" + i, "{\"msg\":\"m\"}");
                    assertEquals(Set.of(shardId), loop.publishAll());
                }
                long reads = 0;
                for (String line : store.byCaller().split("\n")) {
                    if (line.contains("REGISTER_GET") && line.contains("DirtyPublishing")) {
                        reads += Long.parseLong(line.trim().split(" ")[0]);
                    }
                }
                logger.info("trust_own_head={}: {} head reads by 10 publishes\n{}", trust, reads, store.byCaller());
                return reads;
            }
        }
    }

    public void testAPublishReadsTheHeadOnlyWhenItCannotTrustItsOwnSighting() throws Exception {
        assertEquals("one publish in ten reads its head without trust", 10L, headReadsByPublish(false));
        assertTrue("with trust only the first publish may read it", headReadsByPublish(true) <= 1L);
    }

    /** A node whose lease lapsed reads the head again, and a takeover it missed fences its publish. */
    public void testALapsedWriterReadsTheHeadAgainAndAZombiePublishIsFenced() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            plane.createIndex(new IndexDescriptor("lapsed", "uuid-lapsed", 1, MAPPING, null));
            try (ServerlessNode node = new ServerlessNode(nodeSettings("publish-lapse", true))) {
                node.startup().start();
                node.setMetadataPlane(plane);
                node.leases().renewLease(plane);
                final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
                loop.want("lapsed", 0);
                for (int pass = 0; pass < 20 && node.reconciler().openShards().isEmpty(); pass++) {
                    loop.tick(clock.get());
                }
                final ShardId shardId = node.reconciler().openShards().iterator().next();
                node.writes().index(shardId, "a", "{\"msg\":\"a\"}");
                assertEquals(Set.of(shardId), loop.publishAll());
                final long term = plane.heads().read("lapsed", 0).orElseThrow().term();

                // The lease runs out unrenewed; by its own clock this node may not trust what it saw before.
                clock.addAndGet(2 * TTL);
                assertTrue("a lapsed lease trusts no sighting", node.leases().ownHeadUnderUnbrokenLease(plane, shardId, term).isEmpty());

                // A successor takes the shard at a newer term while this node is paused, and publishes.
                final MetadataPlane otherPlane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
                try (ServerlessNode successor = new ServerlessNode(nodeSettings("publish-successor", true))) {
                    successor.startup().start();
                    successor.setMetadataPlane(otherPlane);
                    successor.leases().renewLease(otherPlane);
                    final NodeLease self = plane.membership()
                        .current()
                        .stream()
                        .filter(l -> l.nodeId().equals(node.localNode().getId()))
                        .findFirst()
                        .orElse(null);
                    assertNull("this node's lease has lapsed", self);
                    final ShardId taken = successor.activation().activateWriter(otherPlane, "lapsed", 0).orElseThrow();
                    successor.writes().index(taken, "b", "{\"msg\":\"b\"}");
                    successor.shards().publishShard(taken, otherPlane.heads().read("lapsed", 0).orElseThrow().term());
                    assertTrue(otherPlane.heads().read("lapsed", 0).orElseThrow().term() > term);

                    // The zombie writes into its engine and tries to publish: refused, by the head or the manifest.
                    try {
                        node.writes().index(shardId, "z", "{\"msg\":\"zombie\"}");
                    } catch (Exception refused) {
                        // A lapsed lease refuses the write itself, which is as good.
                    }
                    loop.publishAll();
                    assertEquals(
                        "the successor's commit stands",
                        successor.localNode().getId(),
                        plane.heads().read("lapsed", 0).orElseThrow().ownerNodeId()
                    );
                    assertTrue(
                        "the manifest is the successor's term",
                        plane.segmentPublisher("lapsed", "uuid-lapsed", 0).readManifest().orElseThrow().term() > term
                    );
                }
            }
        }
    }
}
