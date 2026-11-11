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
import org.opensearch.serverless.store.FileLocation;
import org.opensearch.serverless.store.NodePackBlob;
import org.opensearch.serverless.store.NodePackStore;
import org.opensearch.serverless.store.NodePackWriter;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Shards that publish at about the same time share one node pack, and a file's location reads back from it. */
public class NodePackWriterTests extends OpenSearchTestCase {

    private static NodePackStore.Part part(String uuid, int shard, String name, byte[] content) {
        return new NodePackStore.Part(uuid, shard, name, content.length, () -> new ByteArrayInputStream(content));
    }

    private static byte[] read(InputStream in) throws IOException {
        try (in) {
            return in.readAllBytes();
        }
    }

    public void testConcurrentPublishesShareOnePut() throws Exception {
        final int shards = between(2, 8);
        final List<byte[]> contents = new ArrayList<>();
        long total = 0L;
        for (int i = 0; i < shards; i++) {
            final byte[] content = randomByteArrayOfLength(between(1, 4096));
            contents.add(content);
            total += content.length;
        }
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final NodePackStore packs = new NodePackStore(store, BlobPath.cleanPath(), "node-1");
            // A linger far past the test, and a size that only every shard's blob together reaches: the pack is
            // written once the last shard joins, so all of them must be in it.
            final NodePackWriter writer = new NodePackWriter(packs, 7L, 60_000L, total);
            store.reset();
            final CyclicBarrier start = new CyclicBarrier(shards);
            final ExecutorService executor = Executors.newFixedThreadPool(shards);
            try {
                final List<Future<List<NodePackWriter.Landed>>> futures = new ArrayList<>();
                for (int i = 0; i < shards; i++) {
                    final int shard = i;
                    futures.add(executor.submit(() -> {
                        start.await();
                        return writer.add(List.of(part("uuid-" + (shard % 2), shard, "_0.seg", contents.get(shard))));
                    }));
                }
                final Set<Long> ordinals = new HashSet<>();
                for (int i = 0; i < shards; i++) {
                    final NodePackWriter.Landed landed = futures.get(i).get(30, TimeUnit.SECONDS).get(0);
                    ordinals.add(landed.ordinal());
                    assertEquals(7L, landed.epoch());
                    assertEquals(i, landed.block().shard());
                    final FileLocation location = landed.fileAt(0L, contents.get(i).length);
                    assertTrue(location.isNodePacked());
                    assertEquals(location, FileLocation.parse("f", location.encode()));
                    assertArrayEquals(
                        contents.get(i),
                        read(
                            store.blobContainer(location.containerPath(shardBase()))
                                .readBlob(location.blob(), location.offset(), location.length())
                        )
                    );
                }
                assertEquals("every shard in one pack", Set.of(0L), ordinals);
                assertEquals("one PUT for every shard", 1L, store.blobWrites());
            } finally {
                executor.shutdownNow();
            }
        }
    }

    public void testAPublishAloneWritesItsOwnPackAndAFailedPackSpendsItsOrdinal() throws Exception {
        try (FsBlobStore store = new FsBlobStore(8192, createTempDir(), false)) {
            final NodePackStore packs = new NodePackStore(store, BlobPath.cleanPath(), "node-1");
            final NodePackWriter writer = new NodePackWriter(packs, 3L, 0L, Long.MAX_VALUE);
            final byte[] a = "segment a".getBytes(StandardCharsets.UTF_8);
            final byte[] b = "segment b, longer".getBytes(StandardCharsets.UTF_8);
            final List<NodePackWriter.Landed> first = writer.add(List.of(part("uuid-a", 0, "_0.seg", a), part("uuid-a", 0, "_1.seg", b)));
            assertEquals("one publish's blobs go in one pack", first.get(0).ordinal(), first.get(1).ordinal());
            final NodePackBlob.Header header = packs.readHeader(3L, first.get(0).ordinal()).orElseThrow();
            assertEquals(List.of(first.get(0).block(), first.get(1).block()), header.blocks());

            final NodePackStore.Part broken = new NodePackStore.Part(
                "uuid-a",
                0,
                "_2.seg",
                10,
                () -> { throw new IOException("the segment is gone"); }
            );
            final IOException failed = expectThrows(IOException.class, () -> writer.add(List.of(broken)));
            assertEquals("node pack write failed", failed.getMessage());

            final List<NodePackWriter.Landed> after = writer.add(List.of(part("uuid-a", 0, "_3.seg", a)));
            assertEquals("the failed pack's ordinal is not reused", first.get(0).ordinal() + 2, after.get(0).ordinal());
            assertTrue(writer.add(List.of()).isEmpty());
        }
    }

    public void testAFileRangeMustLieInsideItsBlock() throws Exception {
        final NodePackWriter.Landed landed = new NodePackWriter.Landed(
            "node-1",
            1L,
            2L,
            new NodePackBlob.Block("uuid-a", 0, "_0.seg", 100L, 50L)
        );
        assertEquals(FileLocation.nodePacked("node-1", 1L, 2L, 110L, 40L), landed.fileAt(10L, 40L));
        expectThrows(IllegalArgumentException.class, () -> landed.fileAt(11L, 40L));
        expectThrows(IllegalArgumentException.class, () -> landed.fileAt(-1L, 1L));
    }

    public void testANodePackLocationRoundTripsAndResolvesBesideTheShards() throws Exception {
        final FileLocation location = FileLocation.nodePacked("node-7", 12L, 3L, 4096L, 77L);
        final String encoded = location.encode();
        assertEquals("@node-7/" + NodePackStore.epochDir(12L) + "/" + NodePackStore.name(3L) + "#4096:77", encoded);
        assertEquals(location, FileLocation.parse("_0.cfs", encoded));
        assertTrue(location.isNodePacked());
        assertTrue(location.isPacked());
        assertEquals(encoded.substring(0, encoded.indexOf('#')), location.referencedBlob());
        assertEquals(
            NodePackStore.epochPath(BlobPath.cleanPath().add("base"), "node-7", 12L).buildAsString(),
            location.containerPath(BlobPath.cleanPath().add("base").add("segments").add("alpha~uuid~0")).buildAsString()
        );
        final FileLocation inShard = FileLocation.packed("t=4", "_0.seg", 10L, 5L);
        assertFalse(inShard.isNodePacked());
        assertEquals("segments/alpha~uuid~0/t=4/", inShard.containerPath(shardBase()).buildAsString());

        expectThrows(IllegalArgumentException.class, () -> FileLocation.nodePacked("node/7", 1L, 0L, 0L, 1L));
        expectThrows(IllegalArgumentException.class, () -> FileLocation.packed("@node-7", "x", 0L, 1L));
        for (String bad : List.of(
            "@node-7#0:1",
            "@node-7/e=1/00000000000000000003#0:1",
            "@node-7/" + NodePackStore.epochDir(1L) + "/x#0:1"
        )) {
            expectThrows(IOException.class, () -> FileLocation.parse("f", bad));
        }
    }

    private static BlobPath shardBase() {
        return BlobPath.cleanPath().add("segments").add("alpha~uuid~0");
    }
}
