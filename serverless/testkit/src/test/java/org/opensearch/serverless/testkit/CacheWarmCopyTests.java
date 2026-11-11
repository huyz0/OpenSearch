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
import org.opensearch.serverless.cluster.ReaderPlacement;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.SearchReadiness;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.BlockCache;
import org.opensearch.serverless.transport.CacheWarmCopy;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A search node joining to warm copies its share of the warm cache from the peer that holds it, so that opening those
 * shards and running the searches the peer ran reads nothing from the object store.
 */
public class CacheWarmCopyTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int SHARDS = 12;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, String roles, Settings extra) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-warm-copy")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put("serverless.block_cache.block_size", 4096)
            .put(extra)
            .build();
    }

    public void testAJoinerCopiesItsShareAndThenReadsNothingFromTheStore() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("hot", "uuid-hot-0000000000", SHARDS, MAPPING, null));
        try (ServerlessNode writer = new ServerlessNode(nodeSettings("copy-w", "ingest", Settings.EMPTY))) {
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

        try (
            ServerlessNode peer = new ServerlessNode(nodeSettings("copy-peer", "search", Settings.EMPTY));
            ServerlessNode joiner = new ServerlessNode(
                nodeSettings("copy-joiner", "search", Settings.builder().put(SearchReadiness.WARM_ON_JOIN, true).build())
            );
            ServerlessNode slow = new ServerlessNode(
                nodeSettings(
                    "copy-slow",
                    "search",
                    Settings.builder().put(SearchReadiness.WARM_ON_JOIN, true).put(CacheWarmCopy.MAX_BYTES_PER_SECOND, 64 * 1024).build()
                )
            )
        ) {
            for (ServerlessNode node : List.of(peer, joiner, slow)) {
                node.startup().start();
                node.setMetadataPlane(plane);
                node.leases().renewLease(plane);
            }
            for (int s = 0; s < SHARDS; s++) {
                final ShardId onPeer = peer.readers().serveAsReader(plane, "hot", s);
                assertEquals(300L, ShardOps.hits(peer.searchService(), onPeer, "msg", "warm"));
            }

            final String joinerId = joiner.localNode().getId();
            final Set<CacheWarmCopy.Shard> expected = shareOf(peer.localNode().getId(), joinerId);
            assumeTrue("the joiner ranks first for some shard", expected.isEmpty() == false);

            final CacheWarmCopy.Outcome outcome = CacheWarmCopy.of(joiner).copyFromPeers();
            logger.info("WARMCOPY|{}", outcome);
            assertEquals("the shards the joiner will serve, as the peer ranked them", expected, outcome.shards());
            assertEquals(1, outcome.peersAsked());
            assertEquals(0, outcome.peersFailed());
            assertTrue(outcome.blocksCopied() > 0);
            final BlockCache cache = joiner.blockCache();
            assertEquals("a copy is not a fetch", 0L, cache.bytesFetched());
            assertEquals(outcome.bytesCopied(), cache.bytesCopied());

            for (CacheWarmCopy.Shard shard : expected) {
                final ShardId onJoiner = joiner.readers().serveAsReader(plane, "hot", shard.shard());
                assertEquals(300L, ShardOps.hits(joiner.searchService(), onJoiner, "msg", "warm"));
            }
            assertEquals("opening and searching the copied shards read nothing from the store", 0L, cache.bytesFetched());
            assertTrue("and they were read from the copy", cache.hits() > 0);

            // The same copy, paced to 64 KB a second: it waits about as long as its bytes take at that rate.
            final AtomicLong nanos = new AtomicLong();
            final CacheWarmCopy.Outcome paced = CacheWarmCopy.of(slow).pacedBy(nanos::get, nanos::addAndGet).copyFromPeers();
            logger.info("WARMCOPY|paced|{}", paced);
            if (paced.bytesCopied() > 0) {
                final double seconds = paced.pausedNanos() / 1e9;
                final double due = paced.bytesCopied() / (64.0 * 1024);
                assertEquals("paused " + seconds + " s for " + paced.bytesCopied() + " bytes", due, seconds, 0.01 + due * 0.01);
            }
        }
    }

    /**
     * The shards whose first reader would be the joiner: its share of a cold index. Ranked against the peer alone,
     * since the other joiner is still warming and does not advertise the search role.
     */
    private static Set<CacheWarmCopy.Shard> shareOf(String peerId, String joinerId) {
        final List<NodeLease> searchers = List.of(lease(peerId), lease(joinerId));
        final Set<CacheWarmCopy.Shard> share = new HashSet<>();
        for (int s = 0; s < SHARDS; s++) {
            if (ReaderPlacement.candidatesFor("hot", s, searchers, ServerlessNode.ROLE_SEARCH, 1).equals(List.of(joinerId))) {
                share.add(new CacheWarmCopy.Shard("hot", "uuid-hot-0000000000", s));
            }
        }
        return share;
    }

    private static NodeLease lease(String id) {
        return new NodeLease(id, id, "x", Set.of(ServerlessNode.ROLE_SEARCH), Long.MAX_VALUE);
    }

    public void testTheCacheListsAScopesKeysHottestFirstAndACopyIsNotAFetch() {
        final BlockCache cache = new BlockCache(4, 16);
        cache.put("a#0/t/f#0", new byte[4]);
        cache.put("b#0/t/f#0", new byte[4]);
        cache.put("a#0/t/f#1", new byte[4]);
        cache.get("a#0/t/f#0");
        assertEquals(Map.of("a#0", List.of("a#0/t/f#0", "a#0/t/f#1")), cache.residentByScope(Set.of("a#0")));
        final long fetched = cache.bytesFetched();
        assertTrue(cache.putCopied("c#0/t/f#0", new byte[4]));
        assertFalse("a block already held is kept", cache.putCopied("a#0/t/f#0", new byte[4]));
        assertEquals(fetched, cache.bytesFetched());
        assertEquals(4L, cache.bytesCopied());
        assertEquals(Set.of("a#0/t/f#1", "c#0/t/f#0"), cache.copyOut(List.of("a#0/t/f#1", "gone", "c#0/t/f#0")).keySet());
    }
}
