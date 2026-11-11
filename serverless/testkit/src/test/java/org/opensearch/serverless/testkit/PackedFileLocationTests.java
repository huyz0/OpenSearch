/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.ConditionalBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.GarbageCollector;
import org.opensearch.serverless.store.BlockCache;
import org.opensearch.serverless.store.BlockCacheDirectory;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.FileLocation;
import org.opensearch.serverless.store.SegmentPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A commit whose files are ranges of one pack reads as one whose files are blobs of their own, and a pack a commit
 * names is kept by the sweep.
 */
public class PackedFileLocationTests extends OpenSearchTestCase {

    private static final String TERM_DIR = "t=2";
    private static final String PACK = "pack-abc";
    private static final int DOCS = 500;

    public void testALocationReadsBackAsItWasWritten() throws Exception {
        final FileLocation whole = FileLocation.parse("_0.cfs", "t=3");
        assertFalse(whole.isPacked());
        assertEquals("t=3/_0.cfs", whole.referencedBlob());
        assertEquals("t=3", whole.encode());

        final FileLocation packed = FileLocation.packed("t=3", PACK, 1024L, 77L);
        assertEquals("t=3/pack-abc#1024:77", packed.encode());
        assertEquals(packed, FileLocation.parse("_0.cfs", packed.encode()));
        assertEquals("t=3/pack-abc", packed.referencedBlob());

        for (String malformed : List.of(
            "t=3/",
            "t=3/pack",
            "t=3/pack#",
            "t=3/pack#1",
            "t=3/pack#1:",
            "t=3/#1:2",
            "/pack#1:2",
            "t=3/p#-1:2"
        )) {
            expectThrows(IOException.class, () -> FileLocation.parse("_0.cfs", malformed));
        }
    }

    public void testAManifestNamingBothFormsSurvivesItsRegisterAndTheWire() throws Exception {
        final Map<String, String> files = new LinkedHashMap<>();
        files.put("_0.cfs", "t=1");
        files.put("_1.cfs", "t=2/pack-abc#512:300");
        final CommitManifest manifest = new CommitManifest(2L, files, "writer", Map.of("_0.cfs", 99L));

        final CommitManifest parsed = CommitManifest.fromStream(manifest.toBytes().streamInput());
        assertEquals(files, parsed.files());
        assertEquals(FileLocation.packed("t=2", PACK, 512L, 300L), parsed.location("_1.cfs"));
        assertEquals(Long.valueOf(300L), parsed.lengthOf("_1.cfs"));
        assertEquals(Long.valueOf(99L), parsed.lengthOf("_0.cfs"));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            manifest.writeTo(out);
            final CommitManifest wired = new CommitManifest(out.bytes().streamInput());
            assertEquals(files, wired.files());
            assertEquals(Long.valueOf(300L), wired.lengthOf("_1.cfs"));
        }

        expectThrows(IllegalArgumentException.class, () -> new CommitManifest(1L, Map.of("_0.cfs", "t=1/pack#x:1")));
        final String bad = "{\"term\":1,\"files\":{\"_0.cfs\":\"t=1/pack#1\"}}";
        expectThrows(
            IOException.class,
            () -> CommitManifest.fromStream(new ByteArrayInputStream(bad.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
        );
    }

    public void testACommitInAPackOpensRestoresAndAnswersItsCommitPoint() throws Exception {
        final FsBlobStore store = new FsBlobStore(1 << 20, createTempDir(), false);
        final BlobPath shardBase = BlobPath.cleanPath().add("segments").add("logs#uuid#0");
        final CommitManifest manifest = packCommit(store, shardBase);

        // Read through the block cache, block by block, with and without fetching ahead at open.
        final ExecutorService helpers = Executors.newFixedThreadPool(4);
        try {
            for (BlockCache cache : List.of(new BlockCache(1024, 4_096), new BlockCache(1024, 4_096).prefetchingWith(helpers))) {
                try (
                    BlockCacheDirectory directory = BlockCacheDirectory.create(
                        new ByteBuffersDirectory(),
                        store,
                        shardBase,
                        cache,
                        "logs#0",
                        manifest
                    );
                    DirectoryReader reader = DirectoryReader.open(directory)
                ) {
                    assertEquals(DOCS, reader.numDocs());
                    assertEquals(DOCS / 2, new IndexSearcher(reader).count(new TermQuery(new Term("parity", "even"))));
                }
            }
        } finally {
            helpers.shutdownNow();
        }

        // The commit point is read out of the pack too.
        final SegmentPublisher publisher = new SegmentPublisher(store, shardBase);
        assertEquals(DOCS - 1, publisher.committedMaxSeqNo(manifest).getAsLong());
        assertEquals(DOCS - 1, publisher.committedLocalCheckpoint(manifest).getAsLong());
    }

    public void testARestoredPackedCommitIsWholeOnLocalDisk() throws Exception {
        final FsBlobStore store = new FsBlobStore(1 << 20, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1_000L, 30_000L);
        plane.createIndex(new IndexDescriptor("logs", "uuid-logs-00000000", 1, "{}", null));
        final BlobPath shardBase = plane.shardData("logs", 0);
        final CommitManifest manifest = packCommit(store, shardBase);
        installManifest(store, shardBase, manifest);

        try (Directory target = new ByteBuffersDirectory()) {
            plane.segmentPublisher("logs", 0).restoreInto(target, new ShardId("logs", "uuid-logs-00000000", 0));
            try (DirectoryReader reader = DirectoryReader.open(target)) {
                assertEquals(DOCS, reader.numDocs());
            }
        }
    }

    public void testTheSweepKeepsAPackTheCommitNamesAndTakesOneItDoesNot() throws Exception {
        final FsBlobStore store = new FsBlobStore(1 << 20, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1_000L, 30_000L);
        plane.createIndex(new IndexDescriptor("logs", "uuid-logs-00000000", 1, "{}", null));
        final BlobPath shardBase = plane.shardData("logs", 0);
        final CommitManifest manifest = packCommit(store, shardBase);
        installManifest(store, shardBase, manifest);
        // In an older term: the current term's unreferenced blobs may be a publish in flight, and are left alone.
        final BlobContainer older = store.blobContainer(shardBase.add("t=1"));
        older.writeBlob("pack-orphan", new ByteArrayInputStream(new byte[] { 1, 2, 3 }), 3, false);

        final List<String> deleted = new GarbageCollector(store, BlobPath.cleanPath()).collectShard(plane, "logs", 0);
        assertEquals(List.of("t=1/pack-orphan"), deleted);
        assertTrue(store.blobContainer(shardBase.add(TERM_DIR)).blobExists(PACK));
    }

    /** Writes a small commit locally and publishes it as one pack, the way a packed publish lays it out. */
    private CommitManifest packCommit(FsBlobStore store, BlobPath shardBase) throws Exception {
        final java.nio.file.Path path = createTempDir();
        try (Directory local = new NIOFSDirectory(path)) {
            final IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(local, config)) {
                for (int id = 0; id < DOCS; id++) {
                    final Document doc = new Document();
                    doc.add(new StringField("_id", Integer.toString(id), Field.Store.YES));
                    doc.add(new StringField("parity", id % 2 == 0 ? "even" : "odd", Field.Store.NO));
                    writer.addDocument(doc);
                    if (id == DOCS / 2) {
                        writer.flush();
                    }
                }
                writer.setLiveCommitData(
                    Map.of(
                        SequenceNumbers.MAX_SEQ_NO,
                        Long.toString(DOCS - 1),
                        SequenceNumbers.LOCAL_CHECKPOINT_KEY,
                        Long.toString(DOCS - 1)
                    ).entrySet()
                );
                writer.commit();
            }
            final ByteArrayOutputStream pack = new ByteArrayOutputStream();
            // A few bytes ahead of the first file, so an offset of zero is never what makes a read work.
            pack.write(new byte[] { 9, 9, 9 });
            final Map<String, String> files = new LinkedHashMap<>();
            final Map<String, Long> lengths = new LinkedHashMap<>();
            for (String name : SegmentInfos.readLatestCommit(local).files(true)) {
                final byte[] bytes = java.nio.file.Files.readAllBytes(path.resolve(name));
                files.put(name, FileLocation.packed(TERM_DIR, PACK, pack.size(), bytes.length).encode());
                lengths.put(name, (long) bytes.length);
                pack.write(bytes);
            }
            final byte[] packed = pack.toByteArray();
            store.blobContainer(shardBase.add(TERM_DIR)).writeBlob(PACK, new ByteArrayInputStream(packed), packed.length, true);
            return new CommitManifest(2L, files, "writer", lengths);
        }
    }

    private static void installManifest(FsBlobStore store, BlobPath shardBase, CommitManifest manifest) throws IOException {
        assertTrue(
            ConditionalBlobContainer.of(store.blobContainer(shardBase))
                .compareAndSwapRegister(SegmentPublisher.MANIFEST, BlobRegister.ABSENT_GENERATION, manifest.toBytes())
                .applied()
        );
    }
}
