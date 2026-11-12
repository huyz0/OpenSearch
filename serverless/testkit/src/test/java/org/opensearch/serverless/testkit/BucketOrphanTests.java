/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.bucket.BucketManifest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.reconcile.GarbageCollector;
import org.opensearch.serverless.reconcile.NodePackCollector;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.NodePackLedger;
import org.opensearch.serverless.store.NodePackStore;
import org.opensearch.serverless.store.NodePackWriter;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Nothing a bucket member wrote outlives it: once its index is deleted, the packs only it named are collected, a pack a
 * surviving member still names is kept, and the bucket's manifest no longer lists it. With every member deleted the
 * node's packs are back to none.
 */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*")
public class BucketOrphanTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String BUCKET = "s02-1";
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings() {
        return Settings.builder()
            .put("node.name", "orphan-writer")
            .put("cluster.name", "serverless-bucket-orphans")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.commits", true)
            .put("serverless.segments.node_pack.enabled", true)
            .put("serverless.buckets.enabled", true)
            .build();
    }

    /** Bytes in the node's packs, ledger aside. */
    private static long packBytes(MetadataPlane plane, String nodeId) throws Exception {
        final BlobContainer packs = plane.blobStore().blobContainer(plane.basePath().add(NodePackStore.ROOT).add(nodeId));
        long bytes = 0;
        for (Map.Entry<String, BlobContainer> epoch : packs.children().entrySet()) {
            for (Map.Entry<String, BlobMetadata> blob : epoch.getValue().listBlobs().entrySet()) {
                if (blob.getKey().startsWith("extra") == false) {
                    bytes += blob.getValue().length();
                }
            }
        }
        return bytes;
    }

    private static void collect(NodePackCollector collector, AtomicLong clock) throws Exception {
        for (int pass = 0; pass < 4; pass++) {
            collector.run();
            clock.addAndGet(NodePackWriter.COLLECTABLE_AFTER_MILLIS + NodePackCollector.MIN_RECHECK_MILLIS);
        }
    }

    public void testDeletedMembersLeaveNoPacksBehind() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            final String[] names = { "orphan-a", "orphan-b", "orphan-c" };
            BucketManifest bucket = BucketManifest.empty(BUCKET);
            for (String name : names) {
                bucket = bucket.withMember("uuid-" + name, new BucketManifest.Member(name, null));
            }
            plane.buckets().swap(bucket, BlobRegister.ABSENT_GENERATION).orElseThrow();
            for (String name : names) {
                plane.createIndex(new IndexDescriptor(name, "uuid-" + name, 1, MAPPING, null).withBucket(BUCKET));
            }
            try (ServerlessNode writer = new ServerlessNode(nodeSettings())) {
                writer.startup().start();
                writer.setMetadataPlane(plane);
                writer.leases().renewLease(plane);
                plane.membership().refresh();
                final String nodeId = writer.localNode().getId();
                final BackgroundReconciler loop = new BackgroundReconciler(writer, plane);
                for (String name : names) {
                    loop.want(name, 0);
                }
                for (int pass = 0; pass < 20 && writer.reconciler().openShards().size() < names.length; pass++) {
                    loop.tick(clock.get());
                }
                final List<ShardId> held = new ArrayList<>(writer.reconciler().openShards());
                // Two rounds, so each member has a superseded commit's packs as well as its live ones.
                for (int round = 0; round < 2; round++) {
                    for (ShardId shardId : held) {
                        for (int d = 0; d < 20; d++) {
                            writer.writes().index(shardId, round + "-" + d, "{\"msg\":\"orphan " + d + "\"}");
                        }
                    }
                    loop.publishAll();
                }
                final NodePackCollector collector = new NodePackCollector(
                    new NodePackStore(plane.blobStore(), plane.basePath(), nodeId),
                    new NodePackLedger(plane.blobStore(), plane.basePath(), nodeId),
                    plane,
                    clock::get,
                    GarbageCollector.DEFAULT_MINIMUM_UNREFERENCED_MILLIS
                );
                final long written = packBytes(plane, nodeId);
                assertTrue(written > 0);

                for (String name : List.of("orphan-a", "orphan-b")) {
                    loop.stopWanting(name, 0);
                    assertTrue(plane.deleteIndex(name));
                }
                loop.tick(clock.get());
                assertEquals(
                    "the deleted members leave the manifest",
                    List.of("uuid-orphan-c"),
                    List.copyOf(plane.buckets().read(BUCKET).orElseThrow().manifest().members().keySet())
                );
                store.reset();
                collect(collector, clock);
                final long lists = store.listings();
                final long deletes = store.deletes();
                final long gets = store.blobReads() + store.registerReads();
                final long survivorOnly = packBytes(plane, nodeId);
                logger.info(
                    "BUCKET-ORPHANS|written {} B|after deleting 2 of 3 {} B|collector LIST {} DELETE {} GET {}",
                    written,
                    survivorOnly,
                    lists,
                    deletes,
                    gets
                );
                assertTrue("packs only the deleted members named are gone", survivorOnly < written);
                final var survivor = plane.segmentPublisher("orphan-c", "uuid-orphan-c", 0).readManifest().orElseThrow();
                for (String file : survivor.files().keySet()) {
                    try (var in = plane.segmentPublisher("orphan-c", "uuid-orphan-c", 0).openFile(survivor, file)) {
                        assertTrue("the survivor's files are all still there", in.read() >= 0 || survivor.lengthOf(file) == 0);
                    }
                }

                loop.stopWanting("orphan-c", 0);
                assertTrue(plane.deleteIndex("orphan-c"));
                loop.tick(clock.get());
                collect(collector, clock);
                assertEquals("with every member deleted, no pack is left", 0L, packBytes(plane, nodeId));
                assertEquals(0, plane.buckets().read(BUCKET).orElseThrow().manifest().size());
            }
        }
    }
}
