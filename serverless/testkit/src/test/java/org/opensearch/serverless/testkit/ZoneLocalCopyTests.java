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
import org.opensearch.serverless.cluster.Zones;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.SearchReadiness;
import org.opensearch.serverless.shell.SearchWarmup;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.transport.CacheBlocksRequest;
import org.opensearch.serverless.transport.CacheWarmCopy;
import org.opensearch.serverless.transport.ZoneTraffic;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Warm blocks are copied only from peers in the joiner's zone, the rest are read from the store, and a dead writer's
 * shards go to a survivor in its zone.
 *
 * <p>Runs against {@code FsBlobContainer} only.
 */
public class ZoneLocalCopyTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int SHARDS = 6;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings settings(String name, String roles, String zone, Settings extra) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-zone-copy")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put("serverless.block_cache.block_size", 4096)
            .put("node.attr.zone", zone)
            .put(extra)
            .build();
    }

    public void testAJoinerCopiesNoBlocksAcrossZonesAndReadsThemFromTheStore() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("hot", "uuid-hot-0000000000", SHARDS, MAPPING, null));
        try (ServerlessNode writer = new ServerlessNode(settings("zc-w", "ingest", "zone-a", Settings.EMPTY))) {
            writer.startup().start();
            for (int s = 0; s < SHARDS; s++) {
                final ShardId shardId = writer.activation().activateWriter(plane, "hot", s).orElseThrow();
                for (int i = 0; i < 300; i++) {
                    ShardOps.indexDoc(
                        writer.reconciler().shard(shardId),
                        s + "-" + i,
                        "{\"msg\":\"warm block " + i + "\",\"n\":" + i + "}"
                    );
                }
                writer.reconciler().shard(shardId).refresh("copy");
                writer.shards().publishShard(shardId, plane.heads().read("hot", s).orElseThrow().term());
            }
        }
        final Settings joining = Settings.builder().put(SearchReadiness.WARM_ON_JOIN, true).build();
        try (
            ServerlessNode peer = new ServerlessNode(settings("zc-peer", "search", "zone-b", Settings.EMPTY));
            ServerlessNode far = new ServerlessNode(settings("zc-far", "search", "zone-a", joining));
            ServerlessNode near = new ServerlessNode(settings("zc-near", "search", "zone-b", joining));
            ServerlessNode allowed = new ServerlessNode(
                settings("zc-allowed", "search", "zone-a", Settings.builder().put(joining).put(CacheWarmCopy.CROSS_ZONE, true).build())
            )
        ) {
            for (ServerlessNode node : List.of(peer, far, near, allowed)) {
                node.startup().start();
                node.setMetadataPlane(plane);
                node.leases().renewLease(plane);
            }
            for (int s = 0; s < SHARDS; s++) {
                final ShardId onPeer = peer.readers().serveAsReader(plane, "hot", s);
                assertEquals(300L, ShardOps.hits(peer.searchService(), onPeer, "msg", "warm"));
            }

            // In another zone: asked what it holds, nothing more; the blocks are left for the store.
            final CacheWarmCopy.Outcome across = CacheWarmCopy.of(far).copyFromPeers();
            logger.info("ZONECOPY|across|{}", across);
            assumeTrue("the joiner ranks first for some shard", across.shards().isEmpty() == false);
            assertEquals("no block crossed", 0, across.blocksCopied());
            assertFalse("the named blocks are left for the store", across.unheld().isEmpty());
            assertNull(
                "and no block transfer was even asked for",
                far.router().zoneTraffic().byAction(ZoneTraffic.Path.CROSS_ZONE).get(CacheBlocksRequest.ACTION)
            );
            final SearchWarmup.Outcome warmed = SearchWarmup.warm(far, plane);
            logger.info("ZONECOPY|warmed|{}", warmed);
            assertTrue("read from the store instead", far.blockCache().bytesFetched() > 0);
            assertTrue("and ready once it has", SearchReadiness.of(far).ready());

            // In the same zone: copied, as before.
            final CacheWarmCopy.Outcome within = CacheWarmCopy.of(near).copyFromPeers();
            logger.info("ZONECOPY|within|{}", within);
            if (within.shards().isEmpty() == false) {
                assertTrue(within.blocksCopied() > 0);
                assertEquals("nothing crossed", 0, near.router().zoneTraffic().total(ZoneTraffic.Path.CROSS_ZONE));
            }

            // Allowed across zones by its setting: copied too.
            final CacheWarmCopy.Outcome permitted = CacheWarmCopy.of(allowed).copyFromPeers();
            if (permitted.shards().isEmpty() == false) {
                assertTrue(permitted.blocksCopied() > 0);
            }
        }
    }

    public void testADeadWritersShardsGoToASurvivorInItsZone() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        try (
            ServerlessNode a = new ServerlessNode(settings("zt-a", "ingest", "zone-a", Settings.EMPTY));
            ServerlessNode b = new ServerlessNode(settings("zt-b", "ingest", "zone-b", Settings.EMPTY));
            ServerlessNode c = new ServerlessNode(settings("zt-c", "ingest", "zone-b", Settings.EMPTY));
            ServerlessNode d = new ServerlessNode(settings("zt-d", "ingest", "zone-c", Settings.EMPTY))
        ) {
            final List<ServerlessNode> live = List.of(a, b, c, d);
            for (ServerlessNode node : live) {
                node.startup().start();
                node.setMetadataPlane(plane);
            }
            final String deadId;
            try (ServerlessNode dead = new ServerlessNode(settings("zt-dead", "ingest", "zone-b", Settings.EMPTY))) {
                dead.startup().start();
                dead.setMetadataPlane(plane);
                dead.leases().renewLease(plane);
                deadId = dead.localNode().getId();
            }
            clock.addAndGet(TTL + 1);
            for (ServerlessNode node : live) {
                node.leases().renewLease(plane);
            }
            final var members = plane.membership().refresh();
            for (int shard = 0; shard < 40; shard++) {
                final String winner = a.nodeCapacity().takeoverWinner(plane, "orphans", shard, deadId).orElseThrow();
                assertEquals("shard " + shard + " stays in the dead owner's zone", "zone-b", Zones.zoneOf(members, winner));
                for (ServerlessNode node : live) {
                    assertEquals(
                        "every member names the same one",
                        winner,
                        node.nodeCapacity().takeoverWinner(plane, "orphans", shard, deadId).orElseThrow()
                    );
                }
            }
        }
    }
}
