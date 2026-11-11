/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.BlockCache;
import org.opensearch.serverless.store.BlockCacheDirectory;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.FileLocation;
import org.opensearch.serverless.store.NodePackBlob;
import org.opensearch.serverless.store.NodePackStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * With node packs on, the segment blobs of every shard on a node go into packs beside the node's log instead of each
 * shard's own term container, and every reader and successor follows them there. With them off, nothing changes.
 */
public class NodePackPublishTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, Settings extra) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-node-pack-publish")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.segments.pack.enabled", true)
            .put("serverless.wal.node_log.window_millis", 20)
            .put(extra)
            .build();
    }

    private static Settings nodePacksOn() {
        return Settings.builder()
            .put("serverless.segments.node_pack.enabled", true)
            .put("serverless.segments.node_pack.linger_millis", 5)
            .build();
    }

    private static void write(ServerlessNode node, ShardId shardId, String prefix, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            node.writes().index(shardId, prefix + i, "{\"msg\":\"node pack\",\"n\":" + i + "}");
        }
    }

    private static int docsIn(FsBlobStore store, BlobPath shardBase, CommitManifest manifest) throws Exception {
        try (
            BlockCacheDirectory directory = BlockCacheDirectory.create(
                new ByteBuffersDirectory(),
                store,
                shardBase,
                new BlockCache(4096, 4096),
                "node-pack",
                manifest
            );
            DirectoryReader reader = DirectoryReader.open(directory)
        ) {
            return reader.numDocs();
        }
    }

    /** Every blob a shard holds under its own prefix, ignoring the stray files the test filesystem adds. */
    private static Set<String> shardBlobs(Path root, BlobPath shardBase) throws Exception {
        final Path dir = root.resolve(shardBase.buildAsString());
        final Set<String> blobs = new HashSet<>();
        if (Files.exists(dir) == false) {
            return blobs;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.filter(Files::isRegularFile)
                .map(path -> dir.relativize(path).toString())
                .filter(name -> name.contains("extra") == false)
                .forEach(blobs::add);
        }
        return blobs;
    }

    public void testShardsOfSeveralIndicesPublishIntoNodePacks() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final Path root = createTempDir();
        final FsBlobStore store = new FsBlobStore(8192, root, false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("bravo", "uuid-bravo-00000000", 2, MAPPING, null));
        final Map<String, CommitManifest> manifests = new HashMap<>();
        final List<String[]> shards = List.of(new String[] { "alpha", "0" }, new String[] { "bravo", "0" }, new String[] { "bravo", "1" });
        try (ServerlessNode node = new ServerlessNode(nodeSettings("node-pack-one", nodePacksOn()))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            node.leases().renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            for (String[] shard : shards) {
                loop.want(shard[0], Integer.parseInt(shard[1]));
            }
            loop.tick(clock.get());
            for (ShardId shardId : node.reconciler().openShards()) {
                write(node, shardId, shardId.getIndexName() + "-" + shardId.id() + "-", 10);
            }
            loop.publishAll();
            final String nodeId = node.localNode().getId();
            assertEquals(
                "the writer's epoch was recorded before its first pack",
                1,
                new org.opensearch.serverless.store.NodePackLedger(store, BlobPath.cleanPath(), nodeId).read().epochs().size()
            );
            for (String[] shard : shards) {
                final int number = Integer.parseInt(shard[1]);
                final CommitManifest manifest = plane.segmentPublisher(shard[0], number).readManifest().orElseThrow();
                manifests.put(shard[0] + "#" + number, manifest);
                final BlobPath shardBase = plane.shardData(shard[0], number);
                for (String file : manifest.files().keySet()) {
                    final FileLocation location = manifest.location(file);
                    assertTrue(file + " should be in a node pack: " + manifest.files().get(file), location.isNodePacked());
                    assertTrue(location.referencedBlob(), location.referencedBlob().startsWith("@" + nodeId + "/e="));
                }
                assertEquals("a shard's own prefix holds no segment blobs", Set.of(), segmentBlobsIn(shardBlobs(root, shardBase)));
                assertEquals(10, docsIn(store, shardBase, manifest));
                assertPackHeadersName(store, shardBase, manifest);
            }
            loop.setIdleAfterMillis(1);
            for (String[] shard : shards) {
                loop.stopWanting(shard[0], Integer.parseInt(shard[1]));
            }
            loop.releaseIdle(System.currentTimeMillis() + 60_000L);
        }

        // A successor that has node packs off still reads what is in them, and writes what it adds the default way.
        try (ServerlessNode second = new ServerlessNode(nodeSettings("node-pack-second", Settings.EMPTY))) {
            second.startup().start();
            second.setMetadataPlane(plane);
            second.leases().renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(second, plane);
            loop.want("bravo", 1);
            loop.tick(clock.get());
            final ShardId shardId = second.reconciler().openShards().iterator().next();
            for (int i = 0; i < 10; i++) {
                assertTrue("read out of the node pack", second.reads().get(shardId, "bravo-1-" + i).found());
            }
            write(second, shardId, "later-", 5);
            loop.publishAll();
            final CommitManifest after = plane.segmentPublisher("bravo", 1).readManifest().orElseThrow();
            final CommitManifest before = manifests.get("bravo#1");
            boolean addedInShard = false;
            for (String file : after.files().keySet()) {
                if (before.files().containsKey(file)) {
                    assertEquals("an inherited file keeps its node pack range", before.files().get(file), after.files().get(file));
                } else if (file.startsWith("segments") == false) {
                    assertFalse(file + " was added with node packs off", after.location(file).isNodePacked());
                    addedInShard = true;
                }
            }
            assertTrue("the successor added a segment: " + after.files(), addedInShard);
            assertEquals(15, docsIn(store, plane.shardData("bravo", 1), after));
        }
    }

    public void testNodePacksAreOffByDefaultAndSkipLargeBlobs() throws Exception {
        final boolean nodePacksSetButTooSmall = randomBoolean();
        final Settings extra = nodePacksSetButTooSmall
            ? Settings.builder().put(nodePacksOn()).put("serverless.segments.node_pack.max_blob_bytes", "1b").build()
            : Settings.EMPTY;
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final Path root = createTempDir();
        final FsBlobStore store = new FsBlobStore(8192, root, false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        try (ServerlessNode node = new ServerlessNode(nodeSettings("node-pack-off", extra))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            node.leases().renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            write(node, node.reconciler().openShards().iterator().next(), "doc-", 10);
            loop.publishAll();
            final CommitManifest manifest = plane.segmentPublisher("alpha", 0).readManifest().orElseThrow();
            for (String file : manifest.files().keySet()) {
                final FileLocation location = manifest.location(file);
                assertFalse(file + ": " + manifest.files().get(file), location.isNodePacked());
                assertTrue(location.termDir(), location.termDir().startsWith("t="));
            }
            // The filesystem store makes directories as containers are opened; what matters is that no blob is written.
            assertEquals("no node packs at all", Set.of(), shardBlobs(root, BlobPath.cleanPath().add(NodePackStore.ROOT)));
            assertFalse(segmentBlobsIn(shardBlobs(root, plane.shardData("alpha", 0))).isEmpty());
            assertEquals(10, docsIn(store, plane.shardData("alpha", 0), manifest));
        }
    }

    public void testConcurrentPublishesOfManyShardsShareNodePacks() throws Exception {
        final int shards = 8;
        final long off = publishPuts(Settings.EMPTY, shards);
        final long on = publishPuts(
            Settings.builder().put(nodePacksOn()).put("serverless.segments.node_pack.linger_millis", 200).build(),
            shards
        );
        logger.info("blob PUTs publishing {} shards: {} with node packs off, {} on", shards, off, on);
        assertTrue("node packs should cut a publish's segment PUTs: off " + off + ", on " + on, on < off);
    }

    /** Publishes one small segment on each shard of one index and counts the blob PUTs the publish made. */
    private long publishPuts(Settings extra, int shards) throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            plane.createIndex(new IndexDescriptor("many", "uuid-many-000000000", shards, MAPPING, null));
            try (ServerlessNode node = new ServerlessNode(nodeSettings("node-pack-many", extra))) {
                node.startup().start();
                node.setMetadataPlane(plane);
                node.leases().renewLease(plane);
                final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
                for (int shard = 0; shard < shards; shard++) {
                    loop.want("many", shard);
                }
                loop.tick(clock.get());
                for (ShardId shardId : node.reconciler().openShards()) {
                    write(node, shardId, "s" + shardId.id() + "-", 3);
                }
                store.reset();
                assertEquals(shards, loop.publishAll().size());
                return store.blobWrites();
            }
        }
    }

    private static Set<String> segmentBlobsIn(Set<String> blobs) {
        final Set<String> segments = new HashSet<>();
        for (String blob : blobs) {
            if (blob.endsWith(".seg")) {
                segments.add(blob);
            }
        }
        return segments;
    }

    /** Each pack a manifest points at names, in its header, this shard's blocks around every file. */
    private static void assertPackHeadersName(FsBlobStore store, BlobPath shardBase, CommitManifest manifest) throws Exception {
        for (String file : manifest.files().keySet()) {
            final FileLocation location = manifest.location(file);
            final byte[] leading;
            try (InputStream in = store.blobContainer(location.containerPath(shardBase)).readBlob(location.blob())) {
                leading = in.readAllBytes();
            }
            final NodePackBlob.Header header = NodePackBlob.decode(leading);
            final boolean covered = header.blocks()
                .stream()
                .anyMatch(
                    block -> shardBase.buildAsString().contains(block.indexUuid() + "#" + block.shard() + "/")
                        && block.offset() <= location.offset()
                        && location.offset() + location.length() <= block.offset() + block.length()
                );
            assertTrue(file + " lies in a block of its own shard: " + header, covered);
        }
    }
}
