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
import org.opensearch.serverless.shell.SearchReadiness;
import org.opensearch.serverless.shell.SearchWarmup;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.transport.CacheWarmCopy;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A search node that joins to warm copies its share, opens those shards, turns ready and advertises the search role in
 * one step, and then serves its share without reading the store. What no peer hands over is read in coalesced ranges.
 */
public class SearchWarmupTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int SHARDS = 8;
    private static final String UUID = "uuid-warm-000000000";
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, String roles, boolean warmOnJoin) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-warmup")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put("serverless.block_cache.block_size", 4096)
            .put(SearchReadiness.WARM_ON_JOIN, warmOnJoin)
            .build();
    }

    /** Runs of consecutive block numbers within each file, among some block cache keys ({@code <file>#<block>}). */
    private static int consecutiveRuns(List<String> keys) {
        final java.util.Map<String, java.util.TreeSet<Long>> byFile = new java.util.HashMap<>();
        for (String key : keys) {
            final int hash = key.lastIndexOf('#');
            byFile.computeIfAbsent(key.substring(0, hash), f -> new java.util.TreeSet<>()).add(Long.parseLong(key.substring(hash + 1)));
        }
        int runs = 0;
        for (java.util.TreeSet<Long> blocks : byFile.values()) {
            long previous = Long.MIN_VALUE;
            for (long block : blocks) {
                if (block != previous + 1) {
                    runs++;
                }
                previous = block;
            }
        }
        return runs;
    }

    private MetadataPlane publishedIndex(AtomicLong clock, int shards, int docs) throws Exception {
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("warm", UUID, shards, MAPPING, null));
        try (ServerlessNode writer = new ServerlessNode(nodeSettings("warmup-w", "ingest", false))) {
            writer.startup().start();
            for (int s = 0; s < shards; s++) {
                final ShardId shardId = writer.activation().activateWriter(plane, "warm", s).orElseThrow();
                for (int i = 0; i < docs; i++) {
                    ShardOps.indexDoc(
                        writer.reconciler().shard(shardId),
                        s + "-" + i,
                        "{\"msg\":\"warm word" + (i % 97) + " " + i + "\",\"n\":" + i + "}"
                    );
                }
                // One segment, so a term's postings are one run of consecutive blocks.
                writer.reconciler()
                    .shard(shardId)
                    .forceMerge(new org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest().maxNumSegments(1).flush(true));
                writer.reconciler().shard(shardId).refresh("warmup");
                writer.shards().publishShard(shardId, plane.heads().read("warm", s).orElseThrow().term());
            }
        }
        return plane;
    }

    private static ServerlessNode joined(ServerlessNode node, MetadataPlane plane) throws Exception {
        node.startup().start();
        node.setMetadataPlane(plane);
        node.leases().renewLease(plane);
        return node;
    }

    private static Set<String> advertised(MetadataPlane plane, String nodeId) throws Exception {
        // Read from the store: the test's nodes share one membership, whose snapshot need not re-read a fresh lease.
        return plane.membership().read(nodeId).map(NodeLease::roles).orElseThrow();
    }

    public void testAJoinerWarmsOpensItsShareAndTurnsReadyInOneStep() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = publishedIndex(clock, SHARDS, 2_000);
        try (
            ServerlessNode peer = joined(new ServerlessNode(nodeSettings("warmup-peer", "search", false)), plane);
            ServerlessNode joiner = joined(new ServerlessNode(nodeSettings("warmup-joiner", "search", true)), plane)
        ) {
            for (int s = 0; s < SHARDS; s++) {
                final ShardId onPeer = peer.readers().serveAsReader(plane, "warm", s);
                assertEquals(2_000L, ShardOps.hits(peer.searchService(), onPeer, "msg", "warm"));
            }
            final String joinerId = joiner.localNode().getId();
            assertEquals(Set.of(), advertised(plane, joinerId));

            final SearchWarmup.Outcome outcome = SearchWarmup.warm(joiner, plane);
            logger.info("WARMUP|{}", outcome);
            assertTrue(SearchReadiness.of(joiner).ready());
            assertEquals(
                "ready and ranked at once, not at the next renewal",
                Set.of(ServerlessNode.ROLE_SEARCH),
                advertised(plane, joinerId)
            );
            assertEquals(outcome.copy().shards().size(), outcome.opened());
            assertEquals(0, outcome.openFailed());
            assertEquals("a warm copy leaves nothing to read from the store", 0, outcome.rangedReads());
            assertEquals(0L, joiner.blockCache().bytesFetched());

            for (CacheWarmCopy.Shard shard : outcome.copy().shards()) {
                final ShardId onJoiner = joiner.readers().serveAsReader(plane, "warm", shard.shard());
                assertEquals(2_000L, ShardOps.hits(joiner.searchService(), onJoiner, "msg", "warm"));
            }
            assertEquals("its share served from the copy", 0L, joiner.blockCache().bytesFetched());
        }
    }

    public void testWhatNoPeerHandedOverIsReadInCoalescedRanges() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = publishedIndex(clock, 1, 40_000);
        try (
            ServerlessNode peer = joined(new ServerlessNode(nodeSettings("ranges-peer", "search", false)), plane);
            ServerlessNode cold = joined(new ServerlessNode(nodeSettings("ranges-cold", "search", false)), plane)
        ) {
            final ShardId onPeer = peer.readers().serveAsReader(plane, "warm", 0);
            assertEquals(40_000L, ShardOps.hits(peer.searchService(), onPeer, "msg", "warm"));
            final List<String> hot = peer.blockCache().residentByScope(Set.of(UUID + "#0")).get(UUID + "#0");

            // As if the peer had gone before handing anything over: the cold node has the names and nothing else.
            final ShardId onCold = cold.readers().serveAsReader(plane, "warm", 0);
            final long opened = cold.blockCache().bytesFetched();
            final List<String> unheldKeys = hot.stream().filter(k -> cold.blockCache().holds(k) == false).toList();
            final long unheld = unheldKeys.size();
            // How many hot blocks are left, and in how many files, follows from the merged segment's layout, which
            // varies from run to run. What does not vary is the claim: one read for each run of consecutive blocks.
            final int runs = consecutiveRuns(unheldKeys);
            final int ranged = SearchWarmup.fetchFromStore(cold, onCold, hot);
            logger.info(
                "WARMUP|ranges|{} hot blocks, {} not held after the open in {} runs, {} ranged reads",
                hot.size(),
                unheld,
                runs,
                ranged
            );
            assertTrue("the peer's hot set leaves adjacent blocks to coalesce: " + unheldKeys, runs > 0 && runs < unheld);
            assertEquals("coalesced: one read per run of consecutive blocks, for " + unheld + " blocks", runs, ranged);

            final long fetched = cold.blockCache().bytesFetched();
            assertTrue(fetched > opened);
            assertEquals(40_000L, ShardOps.hits(cold.searchService(), onCold, "msg", "warm"));
            assertEquals("the search the peer ran reads nothing more", fetched, cold.blockCache().bytesFetched());
        }
    }

    public void testABackgroundWarmUpTurnsTheNodeReady() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        try (ServerlessNode lone = joined(new ServerlessNode(nodeSettings("lone", "search", true)), plane)) {
            assertEquals(SearchReadiness.State.WARMING, SearchReadiness.of(lone).state());
            SearchWarmup.startInBackground(lone, plane);
            assertBusy(() -> assertTrue("with no peers it has nothing to copy and turns ready", SearchReadiness.of(lone).ready()));
            assertBusy(() -> assertEquals(Set.of(ServerlessNode.ROLE_SEARCH), advertised(plane, lone.localNode().getId())));
        }
    }
}
