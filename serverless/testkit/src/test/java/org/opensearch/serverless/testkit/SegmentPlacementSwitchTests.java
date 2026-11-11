/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.reconcile.GarbageCollector;
import org.opensearch.serverless.reconcile.NodePackCollector;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.FileLocation;
import org.opensearch.serverless.store.NodePackLedger;
import org.opensearch.serverless.store.NodePackStore;
import org.opensearch.serverless.store.NodePackWriter;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Switching where segments go, per index and for the whole cluster, while writes run against a slow store: every
 * acknowledged write survives a crash and a takeover, every manifest's files stay readable, and once collection has
 * run nothing a manifest does not name is left behind.
 */
public class SegmentPlacementSwitchTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final List<String> INDICES = List.of("alpha", "beta");
    private static final int SHARDS = 2;
    private static final int ROUNDS = 8;

    private Settings nodeSettings(String name, boolean logCommits) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-placement-switch")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.window_millis", 100)
            .put("serverless.wal.node_log.commits", logCommits)
            .put("serverless.segments.node_pack.linger_millis", 50)
            .build();
    }

    private static int call(ServerlessNode node, String method, String path, String body) throws Exception {
        final int port = node.boundHttpAddress().publishAddress().getPort();
        try (HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                    .header("Content-Type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofString(body))
                    .timeout(Duration.ofSeconds(60))
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            );
            assertEquals(response.body(), 200, response.statusCode());
            return response.statusCode();
        }
    }

    private static BackgroundReconciler takeAll(ServerlessNode node, MetadataPlane plane, AtomicLong clock) throws Exception {
        node.leases().renewLease(plane);
        plane.membership().refresh();
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (String index : INDICES) {
            for (int shard = 0; shard < SHARDS; shard++) {
                loop.want(index, shard);
            }
        }
        loop.tick(clock.get());
        assertEquals(INDICES.size() * SHARDS, node.reconciler().openShards().size());
        return loop;
    }

    private static CommitManifest register(MetadataPlane plane, ShardId shardId) throws Exception {
        return plane.segmentPublisher(shardId.getIndexName(), shardId.getIndex().getUUID(), shardId.id()).readManifest().orElseThrow();
    }

    private void switchUnderWrites(boolean logCommits) throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final LatencyInjectingBlobStore slow = new LatencyInjectingBlobStore(new FsBlobStore(8192, createTempDir(), false), 0);
        final MetadataPlane plane = new MetadataPlane(slow, BlobPath.cleanPath(), clock::get, TTL);
        final ServerlessNode first = new ServerlessNode(nodeSettings("placement-first", logCommits));
        final ExecutorService pool = Executors.newFixedThreadPool(3);
        final Map<String, ShardId> acked = new ConcurrentHashMap<>();
        final Set<String> placementsSeen = new HashSet<>();
        try {
            first.startup().start();
            first.setMetadataPlane(plane);
            for (String index : INDICES) {
                call(
                    first,
                    "PUT",
                    "/" + index,
                    "{\"settings\":{\"number_of_shards\":" + SHARDS + "},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"}}}}"
                );
            }
            final BackgroundReconciler loop = takeAll(first, plane, clock);
            final List<ShardId> held = new ArrayList<>(first.reconciler().openShards());
            slow.setDelayMillis(20);
            slow.setTail(50, 200);

            final AtomicBoolean running = new AtomicBoolean(true);
            final List<Future<?>> writers = new ArrayList<>();
            for (int w = 0; w < 3; w++) {
                final int offset = w;
                writers.add(pool.submit(() -> {
                    for (long n = 0; running.get(); n++) {
                        final ShardId shardId = held.get((int) ((offset + n) % held.size()));
                        final String id = "w" + offset + "-" + n;
                        first.writes().index(shardId, id, "{\"msg\":\"switch\"}");
                        acked.put(id, shardId);
                    }
                    return null;
                }));
            }
            for (int round = 0; round < ROUNDS; round++) {
                final String placement = round % 2 == 0 ? "node_pack" : "segment_blob";
                if (round % 4 < 2) {
                    call(first, "PUT", "/alpha/_settings", "{\"index\":{\"serverless.segment_placement\":\"" + placement + "\"}}");
                } else {
                    call(first, "PUT", "/_cluster/settings", "{\"persistent\":{\"serverless.segment_placement\":\"" + placement + "\"}}");
                }
                loop.publishAll();
                loop.checkpointDue();
                for (ShardId shardId : held) {
                    final CommitManifest newest = first.reconciler().newestCommit(shardId);
                    if (newest == null) {
                        continue;
                    }
                    for (String file : newest.files().keySet()) {
                        placementsSeen.add(newest.location(file).isNodePacked() ? "node_pack" : "segment_blob");
                    }
                }
            }
            running.set(false);
            for (Future<?> each : writers) {
                each.get();
            }
            assertEquals("both placements were written", Set.of("node_pack", "segment_blob"), placementsSeen);
            final String firstId = first.localNode().getId();

            // The writer crashes with whatever it had staged; a successor restores the manifests and replays its log.
            clock.addAndGet(2 * TTL);
            try (ServerlessNode second = new ServerlessNode(nodeSettings("placement-second", logCommits))) {
                second.startup().start();
                second.setMetadataPlane(plane);
                final BackgroundReconciler successor = takeAll(second, plane, clock);
                for (Map.Entry<String, ShardId> write : acked.entrySet()) {
                    final ShardId onSecond = second.reconciler()
                        .openShards()
                        .stream()
                        .filter(s -> s.getIndexName().equals(write.getValue().getIndexName()) && s.id() == write.getValue().id())
                        .findFirst()
                        .orElseThrow();
                    assertTrue("acknowledged write " + write.getKey() + " survives", second.reads().get(onSecond, write.getKey()).found());
                }
                successor.publishAll();

                // Collection of the first node's packs, past every grace period: nothing named goes, nothing unnamed stays.
                slow.setDelayMillis(0);
                slow.setTail(0, 0);
                final NodePackCollector collector = new NodePackCollector(
                    new NodePackStore(plane.blobStore(), plane.basePath(), firstId),
                    new NodePackLedger(plane.blobStore(), plane.basePath(), firstId),
                    plane,
                    clock::get,
                    GarbageCollector.DEFAULT_MINIMUM_UNREFERENCED_MILLIS
                );
                for (int pass = 0; pass < 4; pass++) {
                    collector.run();
                    clock.addAndGet(NodePackWriter.COLLECTABLE_AFTER_MILLIS + NodePackCollector.MIN_RECHECK_MILLIS);
                }
                final Set<String> named = new HashSet<>();
                for (ShardId shardId : second.reconciler().openShards()) {
                    final CommitManifest manifest = register(plane, shardId);
                    for (String file : manifest.files().keySet()) {
                        final FileLocation location = manifest.location(file);
                        if (location.isNodePacked()) {
                            named.add(location.termDir() + "/" + location.blob());
                        }
                    }
                    // Every file the manifest names can still be read.
                    try (
                        var reader = org.apache.lucene.index.DirectoryReader.open(second.reconciler().shard(shardId).store().directory())
                    ) {
                        assertTrue(reader.numDocs() > 0);
                    }
                }
                final Set<String> left = new HashSet<>();
                final BlobContainer packs = plane.blobStore().blobContainer(plane.basePath().add(NodePackStore.ROOT).add(firstId));
                for (Map.Entry<String, BlobContainer> epoch : packs.children().entrySet()) {
                    for (String blob : epoch.getValue().listBlobs().keySet()) {
                        left.add("@" + firstId + "/" + epoch.getKey() + "/" + blob);
                    }
                }
                final Set<String> namedWithPrefix = new HashSet<>();
                for (String name : named) {
                    namedWithPrefix.add(name.startsWith("@") ? name : "@" + name);
                }
                assertTrue("a pack a manifest names is kept: " + named + " vs " + left, left.containsAll(namedWithPrefix));
                final Set<String> orphans = new HashSet<>(left);
                orphans.removeAll(namedWithPrefix);
                assertEquals("no pack is left that nothing names", Set.of(), orphans);
            }
        } finally {
            pool.shutdownNow();
            try {
                first.close();
            } catch (Exception e) {
                // It lost every shard it held.
            }
        }
    }

    public void testSwitchingPlacementUnderWritesWithAManifestPerPublish() throws Exception {
        switchUnderWrites(false);
    }

    public void testSwitchingPlacementUnderWritesWithLogCommits() throws Exception {
        switchUnderWrites(true);
    }
}
