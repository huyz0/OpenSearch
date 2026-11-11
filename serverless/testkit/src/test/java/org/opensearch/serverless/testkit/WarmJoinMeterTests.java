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
import org.opensearch.serverless.shell.SearchDrain;
import org.opensearch.serverless.shell.SearchReadiness;
import org.opensearch.serverless.shell.SearchWarmup;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What a warm join and a drain save, against a node that joins cold: store reads and the latency of the first searches
 * on the shards the new node is handed, with every store request taking {@value #REQUEST_MILLIS} ms as an S3 GET
 * roughly does.
 */
public class WarmJoinMeterTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int SHARDS = 12;
    private static final int DOCS = 5_000;
    private static final long REQUEST_MILLIS = 20;
    private static final List<String> QUERIES = List.of("w1", "w7", "w13", "w29", "common");
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, String roles, boolean warm) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-warm-meter")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put("serverless.block_cache.block_size", 16 * 1024)
            .put(SearchReadiness.WARM_ON_JOIN, warm)
            .put(SearchDrain.SETTLE, "0ms")
            .build();
    }

    private static ServerlessNode joined(ServerlessNode node, MetadataPlane plane) throws Exception {
        node.startup().start();
        node.setMetadataPlane(plane);
        node.leases().renewLease(plane);
        return node;
    }

    /**
     * Runs the queries on each shard once and returns the mean time of a shard's first search, in ms: opening the reader
     * if it is not open, which is what a first search on a node does, and the query.
     */
    private static double search(ServerlessNode node, MetadataPlane plane, List<Integer> shards) throws Exception {
        long firstNanos = 0;
        for (int shard : shards) {
            final long started = System.nanoTime();
            final ShardId id = node.readers().serveAsReader(plane, "meter", shard);
            for (int q = 0; q < QUERIES.size(); q++) {
                assertTrue(ShardOps.hits(node.searchService(), id, "msg", QUERIES.get(q)) > 0);
                if (q == 0) {
                    firstNanos += System.nanoTime() - started;
                }
            }
        }
        return shards.isEmpty() ? 0 : firstNanos / 1e6 / shards.size();
    }

    /** The shards whose first reader, among some search nodes, is one of them. */
    private static List<Integer> shareOf(String nodeId, String... searchers) {
        final List<NodeLease> leases = new ArrayList<>();
        for (String id : searchers) {
            leases.add(new NodeLease(id, id, "x", Set.of(ServerlessNode.ROLE_SEARCH), Long.MAX_VALUE));
        }
        final List<Integer> share = new ArrayList<>();
        for (int s = 0; s < SHARDS; s++) {
            if (ReaderPlacement.candidatesFor("meter", s, leases, ServerlessNode.ROLE_SEARCH, 1).equals(List.of(nodeId))) {
                share.add(s);
            }
        }
        return share;
    }

    public void testColdJoinAgainstWarmJoinAndDrain() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final LatencyInjectingBlobStore slow = new LatencyInjectingBlobStore(new FsBlobStore(1024, createTempDir(), false), 0);
        final CountingBlobStore store = new CountingBlobStore(slow);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("meter", "uuid-meter-00000000", SHARDS, MAPPING, null));
        try (ServerlessNode writer = new ServerlessNode(nodeSettings("meter-w", "ingest", false))) {
            writer.startup().start();
            for (int s = 0; s < SHARDS; s++) {
                final ShardId shardId = writer.activation().activateWriter(plane, "meter", s).orElseThrow();
                for (int i = 0; i < DOCS; i++) {
                    ShardOps.indexDoc(
                        writer.reconciler().shard(shardId),
                        s + "-" + i,
                        "{\"msg\":\"common w" + (i % 50) + " v" + (i % 977) + "\",\"n\":" + i + "}"
                    );
                }
                writer.reconciler()
                    .shard(shardId)
                    .forceMerge(new org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest().maxNumSegments(1).flush(true));
                writer.reconciler().shard(shardId).refresh("meter");
                writer.shards().publishShard(shardId, plane.heads().read("meter", s).orElseThrow().term());
            }
        }
        try (
            ServerlessNode peer = joined(new ServerlessNode(nodeSettings("meter-peer", "search", false)), plane);
            ServerlessNode warm = joined(new ServerlessNode(nodeSettings("meter-warm", "search", true)), plane);
            ServerlessNode cold = new ServerlessNode(nodeSettings("meter-cold", "search", false))
        ) {
            final List<Integer> all = new ArrayList<>();
            for (int s = 0; s < SHARDS; s++) {
                all.add(s);
            }
            search(peer, plane, all);
            slow.setDelayMillis(REQUEST_MILLIS);

            // Warm: copy, open, fetch what is missing, turn ready; then the first searches.
            final String peerId = peer.localNode().getId();
            final String warmId = warm.localNode().getId();
            final List<Integer> warmShare = shareOf(warmId, peerId, warmId);
            store.reset();
            final long warmStarted = System.nanoTime();
            final SearchWarmup.Outcome warmed = SearchWarmup.warm(warm, plane);
            final long warmUpMillis = (System.nanoTime() - warmStarted) / 1_000_000L;
            final long warmUpReads = store.reads();
            store.reset();
            final double warmFirst = search(warm, plane, warmShare);
            final long warmSearchReads = store.blobReads();

            // Cold: joins ready, and its first searches open and read its share from the store.
            joined(cold, plane);
            final String coldId = cold.localNode().getId();
            final List<Integer> coldShare = shareOf(coldId, peerId, warmId, coldId);
            store.reset();
            final double coldFirst = search(cold, plane, coldShare);
            final long coldReads = store.blobReads();

            // Drain the warm node: its shards go to the peer and the cold node, which then search them.
            store.reset();
            final SearchDrain.Outcome drained = SearchDrain.drain(warm, plane);
            final long drainReads = store.blobReads();

            logger.info(
                "WARMMETER|{} ms per store request|cold join: {} shards, {} block reads ({} per shard), first search {} ms"
                    + "|warm join: {} shards, warm-up {} ms with {} store requests, then {} block reads, first search {} ms"
                    + "|{} blocks copied ({} bytes)|drain: {} shards taken over, {} blocks handed over, {} block reads, {} ms",
                REQUEST_MILLIS,
                coldShare.size(),
                coldReads,
                coldShare.isEmpty() ? 0 : coldReads / coldShare.size(),
                String.format(java.util.Locale.ROOT, "%.1f", coldFirst),
                warmShare.size(),
                warmUpMillis,
                warmUpReads,
                warmSearchReads,
                String.format(java.util.Locale.ROOT, "%.1f", warmFirst),
                warmed.copy().blocksCopied(),
                warmed.copy().bytesCopied(),
                drained.shardsTakenOver(),
                drained.blocksHandedOver(),
                drainReads,
                drained.tookMillis()
            );
            assertEquals("a warm join's searches read no blocks", 0L, warmSearchReads);
            if (coldShare.isEmpty() == false && warmShare.isEmpty() == false) {
                assertTrue("a cold join reads blocks: " + coldReads, coldReads > 0);
                assertTrue("and its first searches are slower: " + coldFirst + " vs " + warmFirst, coldFirst > warmFirst);
            }
            assertEquals(warmShare.size(), drained.shardsTakenOver());
        }
    }
}
