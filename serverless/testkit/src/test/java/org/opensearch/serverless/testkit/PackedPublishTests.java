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
import org.opensearch.serverless.store.SegmentPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/** With packing on, a publish writes the files it adds as one blob, and every reader and successor follows it there. */
public class PackedPublishTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-packed-publish")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.wal.node_log.commits", false)
            .put("serverless.segments.pack.enabled", true)
            // One blob per segment under the shard, which is what these check.
            .put("serverless.segments.node_pack.enabled", false)
            .put("serverless.wal.node_log.window_millis", 20)
            .build();
    }

    private static void write(ServerlessNode node, ShardId shardId, String prefix, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            node.writes().index(shardId, prefix + i, "{\"msg\":\"packed\",\"n\":" + i + "}");
        }
    }

    /** The packs a manifest's files are in, by their path under the shard. */
    private static Set<String> packsOf(CommitManifest manifest) {
        final Set<String> packs = new HashSet<>();
        for (String file : manifest.files().keySet()) {
            final FileLocation location = manifest.location(file);
            assertTrue(file + " should be packed: " + manifest.files().get(file), location.isPacked());
            packs.add(location.referencedBlob());
        }
        return packs;
    }

    private static int docsIn(FsBlobStore store, BlobPath shardBase, CommitManifest manifest) throws Exception {
        try (
            BlockCacheDirectory directory = BlockCacheDirectory.create(
                new ByteBuffersDirectory(),
                store,
                shardBase,
                new BlockCache(4096, 4096),
                "packed",
                manifest
            );
            DirectoryReader reader = DirectoryReader.open(directory)
        ) {
            return reader.numDocs();
        }
    }

    public void testEachPublishWritesOneBlobAndKeepsWhatItInherits() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final FsBlobStore store = new FsBlobStore(8192, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        final BlobPath shardBase = plane.shardData("alpha", 0);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("packed-one"))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            node.leases().renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();

            write(node, shardId, "first-", 20);
            loop.publishAll();
            final CommitManifest first = plane.segmentPublisher("alpha", 0).readManifest().orElseThrow();
            final Set<String> firstPacks = packsOf(first);
            final String termDir = first.location(first.files().keySet().iterator().next()).termDir();
            assertEquals("one blob per segment: " + first.files(), segmentsOf(first), blobSegments(firstPacks));
            final Set<String> listed = new HashSet<>();
            for (String blob : store.blobContainer(shardBase.add(termDir)).listBlobs().keySet()) {
                // The test filesystem drops stray extraN files into directories; they are not ours.
                if (blob.startsWith("extra") == false) {
                    listed.add(termDir + "/" + blob);
                }
            }
            assertEquals("the term container holds the segment blobs and nothing else", firstPacks, listed);
            assertEquals(20, docsIn(store, shardBase, first));

            write(node, shardId, "second-", 20);
            loop.publishAll();
            final CommitManifest second = plane.segmentPublisher("alpha", 0).readManifest().orElseThrow();
            final Set<String> secondPacks = packsOf(second);
            for (Map.Entry<String, String> file : first.files().entrySet()) {
                if (second.files().containsKey(file.getKey())) {
                    assertEquals("an inherited file keeps its range", file.getValue(), second.files().get(file.getKey()));
                }
            }
            final Set<String> added = new HashSet<>(secondPacks);
            added.removeAll(firstPacks);
            final Set<String> newSegments = segmentsOf(second);
            newSegments.removeAll(segmentsOf(first));
            assertEquals("a blob for each segment the second publish added: " + second.files(), newSegments, blobSegments(added));
            assertEquals(40, docsIn(store, shardBase, second));
            assertEquals(39L, new SegmentPublisher(store, shardBase).committedMaxSeqNo(second).getAsLong());

            // A segment's blob is named after it, and starts with a header naming its files.
            final String firstSegmentBlob = firstPacks.iterator().next();
            assertTrue(firstSegmentBlob, firstSegmentBlob.matches("t=\\d+/_[0-9a-z]+\\.seg"));
            final String blobName = firstSegmentBlob.substring(firstSegmentBlob.indexOf('/') + 1);
            try (java.io.InputStream in = store.blobContainer(shardBase.add(termDir)).readBlob(blobName)) {
                final byte[] head = in.readNBytes(8);
                assertEquals("SEG1", new String(head, 0, 4, java.nio.charset.StandardCharsets.US_ASCII));
                final int jsonLength = java.nio.ByteBuffer.wrap(head, 4, 4).getInt();
                final String json = new String(in.readNBytes(jsonLength), java.nio.charset.StandardCharsets.UTF_8);
                for (Map.Entry<String, String> file : first.files().entrySet()) {
                    final FileLocation location = first.location(file.getKey());
                    if (location.referencedBlob().equals(firstSegmentBlob) == false) {
                        continue;
                    }
                    assertTrue(
                        json + " should place " + file.getKey(),
                        json.contains("\"" + file.getKey() + "\":[" + location.offset() + "," + location.length() + "]")
                    );
                }
            }

            // Deleting a document adds files to a segment already published -- its deletions, and the soft-delete field's
            // updated values -- and those get a blob of their own beside the segment's.
            node.writes().delete(shardId, "first-3");
            loop.publishAll();
            final CommitManifest third = plane.segmentPublisher("alpha", 0).readManifest().orElseThrow();
            final Set<String> thirdPacks = packsOf(third);
            thirdPacks.removeAll(secondPacks);
            assertTrue(
                "a blob for the deletion beside its segment's: " + thirdPacks,
                thirdPacks.stream().anyMatch(blob -> blob.matches("t=\\d+/_[0-9a-z]+-.+\\.seg"))
            );
            for (Map.Entry<String, String> file : second.files().entrySet()) {
                if (third.files().containsKey(file.getKey())) {
                    assertEquals(file.getValue(), third.files().get(file.getKey()));
                }
            }
            assertTrue(docsIn(store, shardBase, third) >= 39);
        }
    }

    public void testASuccessorOpensThePackedCommitAndPacksWhatItAdds() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final FsBlobStore store = new FsBlobStore(8192, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        final BlobPath shardBase = plane.shardData("alpha", 0);
        final CommitManifest before;
        try (ServerlessNode first = new ServerlessNode(nodeSettings("packed-first"))) {
            first.startup().start();
            first.setMetadataPlane(plane);
            first.leases().renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(first, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = first.reconciler().openShards().iterator().next();
            write(first, shardId, "first-", 15);
            loop.publishAll();
            loop.setIdleAfterMillis(1);
            loop.stopWanting("alpha", 0);
            loop.releaseIdle(System.currentTimeMillis() + 60_000L);
            before = plane.segmentPublisher("alpha", 0).readManifest().orElseThrow();
        }
        final Set<String> inheritedPacks = packsOf(before);
        try (ServerlessNode second = new ServerlessNode(nodeSettings("packed-second"))) {
            second.startup().start();
            second.setMetadataPlane(plane);
            second.leases().renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(second, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = second.reconciler().openShards().iterator().next();
            for (int i = 0; i < 15; i++) {
                assertTrue("first-" + i + " is read out of the inherited pack", second.reads().get(shardId, "first-" + i).found());
            }
            write(second, shardId, "second-", 15);
            loop.publishAll();
            final CommitManifest after = plane.segmentPublisher("alpha", 0).readManifest().orElseThrow();
            assertTrue(after.term() > before.term());
            final Set<String> afterPacks = packsOf(after);
            final List<String> added = afterPacks.stream().filter(pack -> inheritedPacks.contains(pack) == false).toList();
            assertFalse("the successor packed what it added: " + after.files(), added.isEmpty());
            for (String blob : added) {
                assertTrue("in its own term: " + blob, blob.startsWith(SegmentPublisher.termSegment(after.term()) + "/"));
            }
            assertEquals(30, docsIn(store, shardBase, after));
        }
    }

    /** The segments a commit holds, by name. */
    private static Set<String> segmentsOf(CommitManifest manifest) {
        final Set<String> segments = new HashSet<>();
        for (String file : manifest.files().keySet()) {
            if (file.startsWith("segments") == false) {
                segments.add(org.apache.lucene.index.IndexFileNames.parseSegmentName(file));
            }
        }
        return segments;
    }

    /** The segment each blob is named after. */
    private static Set<String> blobSegments(Set<String> blobs) {
        final Set<String> segments = new HashSet<>();
        for (String blob : blobs) {
            final String name = blob.substring(blob.indexOf('/') + 1);
            segments.add(name.replaceAll("(-.*)?\\.seg$", ""));
        }
        return segments;
    }
}
