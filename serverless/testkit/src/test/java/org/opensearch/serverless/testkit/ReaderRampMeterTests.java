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
import org.opensearch.serverless.cluster.ReaderRamp;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What a joining reader's slow start costs and saves (docs/specs/reader-slow-start AC-9), on real nodes: two warm search
 * nodes and one that joins with an empty cache, a stream of searches each routed to the first reader the placement picks,
 * with every store request taking {@value #REQUEST_MILLIS} ms as an S3 GET roughly does. The same run twice, the slow start
 * off and on. Time is virtual (one step a second of lease time) so the floor and ceiling curves are exercised without
 * waiting; the searches, the cold reads and their latency are real. A measurement: the numbers are provisional.
 */
public class ReaderRampMeterTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int SHARDS = 12;
    private static final int DOCS = 3_000;
    private static final long REQUEST_MILLIS = 20;
    private static final int STEPS = 70;
    private static final int SEARCHES_PER_STEP = 24;
    private static final long START = 1_000L;
    private static final List<String> QUERIES = List.of("w1", "w7", "w13", "w29", "common");
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, String roles) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-ramp-meter")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put("serverless.block_cache.block_size", 16 * 1024)
            .put(org.opensearch.serverless.node.SearchSettings.WARM_ON_JOIN.getKey(), false)
            .build();
    }

    private static ServerlessNode joined(ServerlessNode node, MetadataPlane plane) throws Exception {
        node.startup().start();
        node.setMetadataPlane(plane);
        node.leases().renewLease(plane);
        return node;
    }

    private static NodeLease lease(ServerlessNode node) {
        final String id = node.localNode().getId();
        return new NodeLease(id, id, "x", Set.of(ServerlessNode.ROLE_SEARCH), Long.MAX_VALUE);
    }

    private static double percentile(List<Double> sorted, double p) {
        return sorted.isEmpty() ? 0 : sorted.get((int) Math.round((sorted.size() - 1) * p));
    }

    /** One arm's numbers. */
    private record Arm(
        List<Double> all,
        List<Double> joiner,
        List<Double> peers,
        double joinerFirstMillis,
        long blobReads,
        int fullAtStep,
        int[] joinerSearchesPer10
    ) {
    }

    private Arm run(
        ReaderRamp.Config config,
        ServerlessNode p1,
        ServerlessNode p2,
        ServerlessNode joiner,
        MetadataPlane plane,
        CountingBlobStore store
    ) throws Exception {
        final String joinerId = joiner.localNode().getId();
        final ReaderRamp.Controller controller = new ReaderRamp.Controller(100, 2_000, 8, new ReaderRamp.Totals(0, 0));
        final List<Double> all = new ArrayList<>();
        final List<Double> onJoiner = new ArrayList<>();
        final List<Double> onPeers = new ArrayList<>();
        final int[] per10 = new int[STEPS / 10];
        double firstJoiner = -1;
        int fullAt = -1;
        long joinerSearches = 0;
        int published = controller.ramp();
        store.reset();
        int counter = 0;
        for (int step = 0; step < STEPS; step++) {
            final long now = START + step * 1_000L;
            final List<NodeLease> leases = new ArrayList<>(List.of(lease(p1), lease(p2)));
            leases.add(lease(joiner).withSearchRamp(START, published));
            if (fullAt < 0 && ReaderRamp.permille(leases.get(2), now, config) >= ReaderRamp.FULL) {
                fullAt = step;
            }
            for (int k = 0; k < SEARCHES_PER_STEP; k++, counter++) {
                final int shard = counter % SHARDS;
                final String chosen = ReaderPlacement.candidatesFor("meter", shard, leases, ServerlessNode.ROLE_SEARCH, 1, config, now)
                    .get(0);
                final ServerlessNode node = chosen.equals(joinerId) ? joiner : chosen.equals(p1.localNode().getId()) ? p1 : p2;
                final long started = System.nanoTime();
                final ShardId id = node.readers().serveAsReader(plane, "meter", shard);
                assertTrue(ShardOps.hits(node.searchService(), id, "msg", QUERIES.get(counter % QUERIES.size())) > 0);
                final double ms = (System.nanoTime() - started) / 1e6;
                all.add(ms);
                if (node == joiner) {
                    onJoiner.add(ms);
                    joinerSearches++;
                    per10[step / 10]++;
                    if (firstJoiner < 0) {
                        firstJoiner = ms;
                    }
                } else {
                    onPeers.add(ms);
                }
            }
            // A lease renewal: the joiner reads its own searches and its own store reads, which are all of them since
            // the peers are warm.
            published = controller.observe(new ReaderRamp.Totals(joinerSearches, store.blobReads() * 1_000L));
        }
        Collections.sort(all);
        Collections.sort(onJoiner);
        Collections.sort(onPeers);
        return new Arm(all, onJoiner, onPeers, firstJoiner, store.blobReads(), fullAt, per10);
    }

    private static String describe(String name, Arm a) {
        return String.format(
            Locale.ROOT,
            "%s: searches %d, p50 %.1f ms p99 %.1f ms, peers p99 %.1f ms, joiner searches %d (p99 %.1f ms, first %.1f ms),"
                + " store block reads %d, full share at step %d, joiner searches per 10 steps %s",
            name,
            a.all().size(),
            percentile(a.all(), 0.5),
            percentile(a.all(), 0.99),
            percentile(a.peers(), 0.99),
            a.joiner().size(),
            percentile(a.joiner(), 0.99),
            a.joinerFirstMillis(),
            a.blobReads(),
            a.fullAtStep(),
            java.util.Arrays.toString(a.joinerSearchesPer10())
        );
    }

    public void testASlowStartAgainstAFullShareForAColdJoiner() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final LatencyInjectingBlobStore slow = new LatencyInjectingBlobStore(new FsBlobStore(1024, createTempDir(), false), 0);
        final CountingBlobStore store = new CountingBlobStore(slow);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("meter", "uuid-meter-00000000", SHARDS, MAPPING, null));
        try (ServerlessNode writer = new ServerlessNode(nodeSettings("ramp-w", "ingest"))) {
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
        final ReaderRamp.Config before = ReaderRamp.config();
        try (
            ServerlessNode p1 = joined(new ServerlessNode(nodeSettings("ramp-p1", "search")), plane);
            ServerlessNode p2 = joined(new ServerlessNode(nodeSettings("ramp-p2", "search")), plane)
        ) {
            // The peers are warm: every shard searched on each, with the queries the run will use.
            for (ServerlessNode peer : List.of(p1, p2)) {
                for (int s = 0; s < SHARDS; s++) {
                    final ShardId id = peer.readers().serveAsReader(plane, "meter", s);
                    for (String q : QUERIES) {
                        assertTrue(ShardOps.hits(peer.searchService(), id, "msg", q) > 0);
                    }
                }
            }
            slow.setDelayMillis(REQUEST_MILLIS);
            final ReaderRamp.Config on = new ReaderRamp.Config(20_000L, 60_000L);
            Arm off;
            Arm ramped;
            try (ServerlessNode cold = joined(new ServerlessNode(nodeSettings("ramp-cold-off", "search")), plane)) {
                off = run(ReaderRamp.Config.OFF, p1, p2, cold, plane, store);
            }
            try (ServerlessNode cold = joined(new ServerlessNode(nodeSettings("ramp-cold-on", "search")), plane)) {
                ramped = run(on, p1, p2, cold, plane, store);
            }
            logger.info(
                "RAMPMETER|{} ms per store request, {} shards, {} searches a step over {} steps|{}|{}",
                REQUEST_MILLIS,
                SHARDS,
                SEARCHES_PER_STEP,
                STEPS,
                describe("off", off),
                describe("on (floor 20 s, ceiling 60 s)", ramped)
            );
            assertTrue("the joiner answered searches with the slow start off", off.joiner().isEmpty() == false);
            assertTrue("and with it on", ramped.joiner().isEmpty() == false);
        } finally {
            ReaderRamp.setConfig(before);
        }
    }
}
