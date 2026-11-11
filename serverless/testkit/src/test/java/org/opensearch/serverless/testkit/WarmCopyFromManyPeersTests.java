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
import org.opensearch.serverless.shell.SearchReadiness;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.transport.CacheWarmCopy;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A joiner copies from every peer at once, takes a block two peers both hold only once, and says when it holds a
 * given share of what it was named.
 */
public class WarmCopyFromManyPeersTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int SHARDS = 8;
    private static final String UUID = "uuid-many-000000000";
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, String roles, Settings extra) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-many-peers")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put("serverless.block_cache.block_size", 4096)
            .put(extra)
            .build();
    }

    private static ServerlessNode joined(ServerlessNode node, MetadataPlane plane) throws Exception {
        node.startup().start();
        node.setMetadataPlane(plane);
        node.leases().renewLease(plane);
        return node;
    }

    public void testAJoinerCopiesFromEveryPeerOnceAndReportsTheShareItHolds() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("many", UUID, SHARDS, MAPPING, null));
        try (ServerlessNode writer = new ServerlessNode(nodeSettings("many-w", "ingest", Settings.EMPTY))) {
            writer.startup().start();
            for (int s = 0; s < SHARDS; s++) {
                final ShardId shardId = writer.activation().activateWriter(plane, "many", s).orElseThrow();
                for (int i = 0; i < 1_000; i++) {
                    ShardOps.indexDoc(writer.reconciler().shard(shardId), s + "-" + i, "{\"msg\":\"shared " + i + "\",\"n\":" + i + "}");
                }
                writer.reconciler().shard(shardId).refresh("many");
                writer.shards().publishShard(shardId, plane.heads().read("many", s).orElseThrow().term());
            }
        }
        // Two peers each serve every shard, as two readers of a hot index do: each names the same blocks.
        final Settings spread = Settings.builder()
            .put("serverless.search.reader_spread.target_per_second", 0.001)
            .put("serverless.search.reader_spread.max_readers", 4)
            .build();
        try (
            ServerlessNode first = joined(new ServerlessNode(nodeSettings("many-a", "search", spread)), plane);
            ServerlessNode second = joined(new ServerlessNode(nodeSettings("many-b", "search", spread)), plane);
            ServerlessNode joiner = joined(
                new ServerlessNode(
                    nodeSettings(
                        "many-joiner",
                        "search",
                        Settings.builder().put(SearchReadiness.WARM_ON_JOIN, true).put(CacheWarmCopy.BATCH_BLOCKS, 1).build()
                    )
                ),
                plane
            )
        ) {
            final Set<String> named = new HashSet<>();
            for (ServerlessNode peer : List.of(first, second)) {
                for (int s = 0; s < SHARDS; s++) {
                    final ShardId onPeer = peer.readers().serveAsReader(plane, "many", s);
                    // Enough searches that every shard counts as hot and is spread over every reader, the joiner included.
                    for (int q = 0; q < 20; q++) {
                        assertEquals(1_000L, ShardOps.hits(peer.searchService(), onPeer, "msg", "shared"));
                        org.opensearch.serverless.cluster.HotShardSpread.of(peer, peer.settings()).order("many", s, List.of("x", "y"));
                    }
                }
                named.addAll(peer.blockCache().residentByScope(Set.of(UUID + "#0")).getOrDefault(UUID + "#0", List.of()));
            }

            final AtomicReference<Long> heldAtHalf = new AtomicReference<>();
            final AtomicReference<Set<CacheWarmCopy.Shard>> shareAtHalf = new AtomicReference<>();
            final CacheWarmCopy.Outcome outcome = CacheWarmCopy.of(joiner).copyFromPeers(0.5, shards -> {
                heldAtHalf.set(joiner.blockCache().stats().blocksResident());
                shareAtHalf.set(shards);
            });
            logger.info("MANYPEERS|{}|held at half: {}", outcome, heldAtHalf.get());
            assertEquals(2, outcome.peersAsked());
            assertEquals(0, outcome.peersFailed());
            assertEquals("every shard is hot and spread to the joiner", SHARDS, outcome.shards().size());
            assertEquals("told the share at the half-way point", outcome.shards(), shareAtHalf.get());
            final long resident = joiner.blockCache().stats().blocksResident();
            assertEquals("each block copied once, though both peers named it", resident, outcome.blocksCopied());
            assertTrue(
                "half, not all, was held when it was told: " + heldAtHalf.get() + " of " + resident,
                heldAtHalf.get() >= resident / 2 && heldAtHalf.get() < resident
            );
            for (String key : named) {
                assertTrue("a block of shard 0 either peer held is held: " + key, joiner.blockCache().holds(key));
            }
        }
    }
}
