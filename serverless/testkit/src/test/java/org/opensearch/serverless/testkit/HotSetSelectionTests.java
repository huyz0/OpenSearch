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
import org.opensearch.serverless.cluster.HotShardSpread;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.CacheHotSet;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.BlockCache;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What a peer offers a joiner: blocks of the commit it serves, read recently, the most read first, within a budget. Not
 * every block it holds.
 */
public class HotSetSelectionTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int SHARDS = 6;
    private static final String UUID = "uuid-select-0000000";
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, String roles) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-hot-set")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put("serverless.block_cache.block_size", 4096)
            .build();
    }

    private static int keys(List<CacheHotSet.HotShard> hot) {
        return hot.stream().mapToInt(h -> h.keys().size()).sum();
    }

    public void testAPeerOffersOnlyRecentBlocksOfItsCurrentCommitMostReadFirstWithinABudget() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("select", UUID, SHARDS, MAPPING, null));
        try (ServerlessNode writer = new ServerlessNode(nodeSettings("select-w", "ingest"))) {
            writer.startup().start();
            for (int s = 0; s < SHARDS; s++) {
                final ShardId shardId = writer.activation().activateWriter(plane, "select", s).orElseThrow();
                for (int i = 0; i < 2_000; i++) {
                    ShardOps.indexDoc(
                        writer.reconciler().shard(shardId),
                        s + "-" + i,
                        "{\"msg\":\"often w" + (i % 30) + "\",\"n\":" + i + "}"
                    );
                }
                writer.reconciler().shard(shardId).refresh("select");
                writer.shards().publishShard(shardId, plane.heads().read("select", s).orElseThrow().term());
            }
        }
        try (ServerlessNode peer = new ServerlessNode(nodeSettings("select-peer", "search"))) {
            peer.startup().start();
            peer.setMetadataPlane(plane);
            peer.leases().renewLease(plane);
            final AtomicLong millis = new AtomicLong(10_000_000L);
            final BlockCache cache = peer.blockCache().withClock(millis::get);
            for (int s = 0; s < SHARDS; s++) {
                final ShardId onPeer = peer.readers().serveAsReader(plane, "select", s);
                // "often" many times, "w7" once: the blocks of the first are read more.
                for (int q = 0; q < 5; q++) {
                    assertTrue(ShardOps.hits(peer.searchService(), onPeer, "msg", "often") > 0);
                }
                assertTrue(ShardOps.hits(peer.searchService(), onPeer, "msg", "w7") > 0);
            }
            final String joiner = "joiner-node";
            final var members = plane.membership().refresh();
            final HotShardSpread spread = HotShardSpread.of(peer, peer.settings());

            final List<CacheHotSet.HotShard> all = CacheHotSet.forJoiner(peer, members, joiner, spread, 0L, Long.MAX_VALUE);
            assumeTrue("the joiner ranks first for some shard", all.isEmpty() == false);
            final int offered = keys(all);
            assertTrue(offered > 0);

            // A block of a file the commit no longer has (merged away), read often, is not offered.
            final String scope = UUID + "#" + all.get(0).shard();
            final String stale = scope + "/t0/_merged_away.cfs#0";
            cache.put(stale, new byte[4096]);
            for (int i = 0; i < 50; i++) {
                cache.get(stale);
            }
            final List<CacheHotSet.HotShard> withStale = CacheHotSet.forJoiner(peer, members, joiner, spread, 0L, Long.MAX_VALUE);
            assertEquals(offered, keys(withStale));
            assertTrue(withStale.stream().noneMatch(h -> h.keys().contains(stale)));

            // An hour later, nothing read since counts as hot.
            millis.addAndGet(3_600_000L);
            final long since = millis.get() - 1_800_000L;
            assertEquals("cold after the window", 0, keys(CacheHotSet.forJoiner(peer, members, joiner, spread, since, Long.MAX_VALUE)));
            assertEquals(
                "the shards are still the joiner's",
                all.size(),
                CacheHotSet.forJoiner(peer, members, joiner, spread, since, 0).size()
            );

            // Within a budget of a few blocks, the most read come first.
            final List<CacheHotSet.HotShard> budgeted = CacheHotSet.forJoiner(peer, members, joiner, spread, 0L, 3 * 4096L);
            final List<String> chosen = new ArrayList<>();
            budgeted.forEach(h -> chosen.addAll(h.keys()));
            logger.info("HOTSET|offered {} of {} resident; budget of 3 blocks chose {}", offered, cache.stats().blocksResident(), chosen);
            assertTrue("within the budget: " + chosen, chosen.size() <= 3 && chosen.isEmpty() == false);
            final Set<String> scopes = new java.util.HashSet<>();
            all.forEach(h -> scopes.add(UUID + "#" + h.shard()));
            final int leastChosenReads = cache.residentEntriesByScope(scopes)
                .values()
                .stream()
                .flatMap(List::stream)
                .filter(b -> chosen.contains(b.key()))
                .mapToInt(BlockCache.Resident::reads)
                .min()
                .orElseThrow();
            final int mostReadLeftOut = cache.residentEntriesByScope(scopes)
                .values()
                .stream()
                .flatMap(List::stream)
                .filter(b -> chosen.contains(b.key()) == false && b.key().equals(stale) == false)
                .max(Comparator.comparingInt(BlockCache.Resident::reads))
                .map(BlockCache.Resident::reads)
                .orElse(0);
            assertTrue("the chosen are the most read: " + leastChosenReads + " >= " + mostReadLeftOut, leastChosenReads >= mostReadLeftOut);
        }
    }
}
