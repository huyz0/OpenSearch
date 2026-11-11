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
import org.opensearch.serverless.store.NodePackBlob;
import org.opensearch.serverless.store.NodePackInputStream;
import org.opensearch.serverless.store.NodePackStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A node pack holds segment blobs of several shards of several indices in one PUT, and each block reads back alone by
 * its range, with no knowledge of the blocks around it.
 */
public class NodePackStoreTests extends OpenSearchTestCase {

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static NodePackStore.Part part(String uuid, int shard, String name, byte[] content) {
        return new NodePackStore.Part(uuid, shard, name, content.length, () -> new ByteArrayInputStream(content));
    }

    private static byte[] read(InputStream in) throws IOException {
        try (in) {
            return in.readAllBytes();
        }
    }

    public void testOnePackHoldsShardsOfSeveralIndicesAndEachBlockReadsAlone() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final NodePackStore packs = new NodePackStore(store, BlobPath.cleanPath(), "node-1");
            final byte[] a0 = bytes("index a, shard 0, segment _3");
            final byte[] b2 = bytes("index b has its own mapping; its blob is bytes here and nothing more");
            final byte[] a1 = randomByteArrayOfLength(between(1, 70_000));
            store.reset();
            final NodePackBlob.Header written = packs.write(
                4L,
                0L,
                List.of(part("uuid-a", 0, "_3.seg", a0), part("uuid-b", 2, "_0.seg", b2), part("uuid-a", 1, "_7.seg", a1))
            );
            assertEquals("three shards of two indices are one PUT", 1L, store.blobWrites());
            assertEquals(3, written.blocks().size());

            store.reset();
            final NodePackBlob.Header header = packs.readHeader(4L, 0L).orElseThrow();
            assertEquals("a header is one ranged read", 1L, store.blobReads());
            assertEquals(written, header);
            assertEquals("node-1", header.nodeId());
            assertEquals(
                Set.of(
                    new NodePackBlob.ShardKey("uuid-a", 0),
                    new NodePackBlob.ShardKey("uuid-b", 2),
                    new NodePackBlob.ShardKey("uuid-a", 1)
                ),
                header.shards()
            );

            final byte[][] expected = { a0, b2, a1 };
            for (int i = 0; i < expected.length; i++) {
                final NodePackBlob.Block block = header.blocks().get(i);
                store.reset();
                assertArrayEquals(expected[i], read(packs.readRange(4L, 0L, block.offset(), block.length())));
                assertEquals("one block is one ranged read", 1L, store.blobReads());
            }
            assertEquals(header.blocks().get(1), header.block("uuid-b", 2, "_0.seg").orElseThrow());
            assertTrue(header.block("uuid-b", 0, "_0.seg").isEmpty());

            // A file inside a block is a narrower range of the same pack, which is what a manifest will point at.
            final NodePackBlob.Block b = header.block("uuid-b", 2, "_0.seg").orElseThrow();
            assertEquals("own mapping", new String(read(packs.readRange(4L, 0L, b.offset() + 16, 11)), StandardCharsets.UTF_8));
        }
    }

    public void testASlotIsWrittenOnce() throws Exception {
        try (FsBlobStore store = new FsBlobStore(8192, createTempDir(), false)) {
            final NodePackStore packs = new NodePackStore(store, BlobPath.cleanPath(), "node-1");
            packs.write(0L, 5L, List.of(part("uuid-a", 0, "_0.seg", bytes("first"))));
            expectThrows(
                FileAlreadyExistsException.class,
                () -> packs.write(0L, 5L, List.of(part("uuid-a", 0, "_1.seg", bytes("second"))))
            );
            final NodePackBlob.Block block = packs.readHeader(0L, 5L).orElseThrow().blocks().get(0);
            assertEquals("_0.seg", block.name());
            assertArrayEquals(bytes("first"), read(packs.readRange(0L, 5L, block.offset(), block.length())));
        }
    }

    public void testAnEmptySlotHasNoHeaderAndDeletesGoByName() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final NodePackStore packs = new NodePackStore(store, BlobPath.cleanPath(), "node-1");
            assertTrue(packs.readHeader(1L, 0L).isEmpty());
            for (long ordinal = 0; ordinal < 3; ordinal++) {
                packs.write(1L, ordinal, List.of(part("uuid-a", 0, "_" + ordinal + ".seg", bytes("block " + ordinal))));
            }
            store.reset();
            packs.delete(1L, List.of(0L, 2L, 9L));
            assertEquals("no listing to find what to delete", 0L, store.listings());
            assertTrue(packs.readHeader(1L, 0L).isEmpty());
            assertTrue(packs.readHeader(1L, 1L).isPresent());
            assertTrue(packs.readHeader(1L, 2L).isEmpty());
        }
    }

    public void testAHeaderLongerThanTheFirstReadCostsOneMore() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final NodePackStore packs = new NodePackStore(store, BlobPath.cleanPath(), "node-1");
            final List<NodePackStore.Part> parts = new ArrayList<>();
            final String longUuid = "u".repeat(200);
            for (int shard = 0; shard < 400; shard++) {
                parts.add(part(longUuid, shard, "_" + shard + ".seg", bytes("s" + shard)));
            }
            packs.write(0L, 0L, parts);
            store.reset();
            final NodePackBlob.Header header = packs.readHeader(0L, 0L).orElseThrow();
            assertTrue("this header is past the first read", header.bodyStart() > 64 * 1024);
            assertEquals("a long header is read in two ranged reads", 2L, store.blobReads());
            assertEquals(400, header.shards().size());
            final NodePackBlob.Block last = header.blocks().get(399);
            assertArrayEquals(bytes("s399"), read(packs.readRange(0L, 0L, last.offset(), last.length())));
        }
    }

    public void testTheStreamRewindsAnywhereForARetriedUpload() throws Exception {
        final byte[] header = bytes("HEADER");
        final List<byte[]> blocks = List.of(bytes("alpha"), new byte[0], bytes("bravo-charlie"), bytes("d"));
        final List<NodePackInputStream.Source> sources = new ArrayList<>();
        final List<Long> lengths = new ArrayList<>();
        final ByteArrayOutputStream whole = new ByteArrayOutputStream();
        whole.write(header);
        for (byte[] block : blocks) {
            sources.add(() -> new ByteArrayInputStream(block));
            lengths.add((long) block.length);
            whole.write(block);
        }
        final byte[] expected = whole.toByteArray();
        try (NodePackInputStream stream = new NodePackInputStream(header, sources, lengths)) {
            assertEquals(expected.length, stream.length());
            final int markAt = between(0, expected.length);
            assertEquals(markAt, stream.readNBytes(markAt).length);
            stream.mark(Integer.MAX_VALUE);
            final byte[] rest = stream.readAllBytes();
            stream.reset();
            assertArrayEquals(rest, stream.readAllBytes());
            final byte[] tail = new byte[expected.length - markAt];
            System.arraycopy(expected, markAt, tail, 0, tail.length);
            assertArrayEquals(tail, rest);
        }
    }

    public void testABlockOfTheWrongLengthFailsTheUpload() throws Exception {
        try (FsBlobStore store = new FsBlobStore(8192, createTempDir(), false)) {
            final NodePackStore packs = new NodePackStore(store, BlobPath.cleanPath(), "node-1");
            final NodePackStore.Part shortPart = new NodePackStore.Part(
                "uuid-a",
                0,
                "_0.seg",
                10,
                () -> new ByteArrayInputStream(bytes("short"))
            );
            expectThrows(IOException.class, () -> packs.write(0L, 0L, List.of(shortPart)));
            final NodePackStore.Part longPart = new NodePackStore.Part(
                "uuid-a",
                0,
                "_0.seg",
                2,
                () -> new ByteArrayInputStream(bytes("long"))
            );
            expectThrows(IOException.class, () -> packs.write(0L, 1L, List.of(longPart)));
        }
    }

    public void testPacksOfDifferentNodesAndEpochsNeverShareAName() {
        final Set<String> paths = new LinkedHashSet<>();
        for (String node : List.of("node-1", "node-2")) {
            for (long epoch = 0; epoch < 2; epoch++) {
                paths.add(NodePackStore.epochPath(BlobPath.cleanPath(), node, epoch).buildAsString() + NodePackStore.name(0L));
            }
        }
        assertEquals(4, paths.size());
        assertTrue(paths.iterator().next().startsWith(NodePackStore.ROOT + "/node-1/e="));
    }
}
