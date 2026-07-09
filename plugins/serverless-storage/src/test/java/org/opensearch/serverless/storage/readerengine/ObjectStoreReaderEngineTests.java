/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.store.Store;
import org.opensearch.serverless.storage.directory.InMemoryShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectoryEntry;
import org.opensearch.serverless.storage.directory.ShardRole;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;

public class ObjectStoreReaderEngineTests extends EngineTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;
    private static final String LOCAL_NODE_ID = "test-node";
    private static final long PRIMARY_TERM = 1;

    public void testMaterializerProducesARealSearchableLuceneCommit() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        SegmentInfos segmentInfos;
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig();
            try (IndexWriter writer = new IndexWriter(writerDirectory, config)) {
                Document doc1 = new Document();
                doc1.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc1);
                Document doc2 = new Document();
                doc2.add(new StringField("id", "2", Field.Store.YES));
                writer.addDocument(doc2);
                writer.commit();
            }
            segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);

            CommitManifest manifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                segmentInfos.getGeneration(),
                1,
                1,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );

            // Materialize the published manifest into a brand new, otherwise-empty directory --
            // simulating a reader shard on a different node that never had this segment locally.
            try (Directory readerDirectory = new ByteBuffersDirectory()) {
                new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer)).materialize(manifest, readerDirectory);

                try (org.apache.lucene.index.DirectoryReader reader = org.apache.lucene.index.DirectoryReader.open(readerDirectory)) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    TopDocs hits = searcher.search(new TermQuery(new Term("id", "1")), 10);
                    assertEquals(1, hits.totalHits.value());
                    hits = searcher.search(new TermQuery(new Term("id", "2")), 10);
                    assertEquals(1, hits.totalHits.value());
                    assertEquals(2, reader.numDocs());
                }
            }
        }
    }

    public void testObjectStoreReaderEngineOpensAgainstAMaterializedManifest() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        CommitManifest manifest;
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig();
            try (IndexWriter writer = new IndexWriter(writerDirectory, config)) {
                Document doc = new Document();
                doc.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            manifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                segmentInfos.getGeneration(),
                0,
                0,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
        }

        // Build a real Store/EngineConfig backed by an empty local directory (as a reader shard on
        // a node that has never seen this shard's data would have) using the same EngineTestCase
        // infrastructure the server module's own engine tests are built on.
        try (Store store = createStore()) {
            EngineConfig engineConfig = config(defaultSettings, store, createTempDir(), newMergePolicy(), null);
            ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer));
            ShardDirectory shardDirectory = new InMemoryShardDirectory();

            try (
                ObjectStoreReaderEngine readerEngine = ObjectStoreReaderEngine.open(
                    engineConfig,
                    manifest,
                    materializer,
                    PRIMARY_TERM,
                    new org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore(blobContainer),
                    new BlobContainerManifestStore(blobContainer),
                    shardDirectory,
                    LOCAL_NODE_ID
                )
            ) {
                try (Engine.Searcher searcher = readerEngine.acquireSearcher("test")) {
                    TopDocs hits = searcher.search(new TermQuery(new Term("id", "1")), 10);
                    assertEquals(1, hits.totalHits.value());
                }

                ShardDirectoryEntry entry = shardDirectory.lookup(
                    engineConfig.getShardId().getIndex().getUUID(),
                    engineConfig.getShardId().getId()
                ).orElseThrow(() -> new AssertionError("opening the reader engine should report an entry to the shard directory"));
                assertEquals(LOCAL_NODE_ID, entry.nodeId());
                assertEquals(ShardRole.READER, entry.role());
            }
        }
    }

    public void testAdmissionControllerRejectsOpeningBeyondItsCapacityThenAllowsAgainAfterClose() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        CommitManifest manifest;
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig();
            try (IndexWriter writer = new IndexWriter(writerDirectory, config)) {
                Document doc = new Document();
                doc.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            manifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                segmentInfos.getGeneration(),
                0,
                0,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
        }

        ReaderShardAdmissionController admissionController = new ReaderShardAdmissionController(1);
        ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer));
        ShardDirectory shardDirectory = new InMemoryShardDirectory();

        org.opensearch.serverless.storage.shardstate.ShardStateStore shardStateStore =
            new org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore(blobContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);

        try (Store firstStore = createStore()) {
            EngineConfig firstConfig = config(defaultSettings, firstStore, createTempDir(), newMergePolicy(), null);
            ObjectStoreReaderEngine first = ObjectStoreReaderEngine.open(
                firstConfig,
                manifest,
                materializer,
                PRIMARY_TERM,
                shardStateStore,
                manifestStore,
                shardDirectory,
                LOCAL_NODE_ID,
                admissionController
            );
            try {
                assertEquals(0, admissionController.availablePermits());

                try (Store secondStore = createStore()) {
                    EngineConfig secondConfig = config(defaultSettings, secondStore, createTempDir(), newMergePolicy(), null);
                    expectThrows(
                        IllegalStateException.class,
                        () -> ObjectStoreReaderEngine.open(
                            secondConfig,
                            manifest,
                            materializer,
                            PRIMARY_TERM,
                            shardStateStore,
                            manifestStore,
                            shardDirectory,
                            LOCAL_NODE_ID,
                            admissionController
                        )
                    );
                    // A rejected open must not have leaked a permit or left any other side effect.
                    assertEquals(0, admissionController.availablePermits());
                }
            } finally {
                first.close();
            }
            assertEquals("closing the first engine must release its permit", 1, admissionController.availablePermits());
        }
    }

    public void testEngineAdvancesToANewerManifestGenerationOncePublished() throws Exception {
        // The scenario rfc-serverless-opensearch.md &sect;16 Phase 3's own "refresh-to-newer-generation"
        // note was tracking: a writer publishes a second manifest after this reader has already
        // opened the first one, and the reader must pick it up without a full engine reopen.
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );
        org.opensearch.serverless.storage.shardstate.ShardStateStore shardStateStore =
            new org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore(blobContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);
        ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer));
        ShardDirectory shardDirectory = new InMemoryShardDirectory();

        // Both manifests are published from the SAME continuing IndexWriter/Directory, exactly like
        // a real writer's successive flushes -- Lucene's own generation/file-naming counter keeps
        // advancing across commits, so the two manifests' segment files never collide by name. Two
        // independent fresh IndexWriters (each restarting Lucene's own naming from scratch) would
        // collide instead -- a test-construction pitfall, not anything real production hits, since a
        // shard's Lucene commit history is always one continuous sequence.
        Directory writerDirectory = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig());

        try (Store store = createStore()) {
            EngineConfig engineConfig = config(defaultSettings, store, createTempDir(), newMergePolicy(), null);
            // Publishing/CAS calls below must key on this engine's own real shard identity, not
            // this test class's INDEX_UUID/SHARD_ID constants -- ObjectStoreReaderEngine polls
            // ShardStateStore using config.getShardId(), and EngineTestCase's own fixture shard id
            // does not actually equal those constants (a mismatch here silently makes every poll a
            // same-as-empty no-op, since shardStateStore.get(...) just finds nothing under the
            // wrong key -- no exception, no warning, exactly the bug this comment now prevents
            // regressing).
            String indexUuid = engineConfig.getShardId().getIndex().getUUID();
            int shardId = engineConfig.getShardId().getId();

            CommitManifest firstManifest;
            {
                Document doc = new Document();
                doc.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
                SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
                firstManifest = publisher.publishCommit(
                    writerDirectory,
                    segmentInfos,
                    indexUuid,
                    shardId,
                    PRIMARY_TERM,
                    1,
                    0,
                    0,
                    new WalPosition("epoch-0", 0),
                    0,
                    PruningStats.empty()
                );
            }
            assertEquals(
                org.opensearch.serverless.storage.shardstate.CasResult.SUCCESS,
                shardStateStore.compareAndSet(
                    indexUuid,
                    shardId,
                    java.util.Optional.empty(),
                    new org.opensearch.serverless.storage.shardstate.ShardHead(PRIMARY_TERM, null, 0L, firstManifest.generation())
                )
            );

            try (
                ObjectStoreReaderEngine readerEngine = ObjectStoreReaderEngine.open(
                    engineConfig,
                    firstManifest,
                    materializer,
                    PRIMARY_TERM,
                    shardStateStore,
                    manifestStore,
                    shardDirectory,
                    LOCAL_NODE_ID
                )
            ) {
                try (Engine.Searcher searcher = readerEngine.acquireSearcher("test")) {
                    assertEquals(1, searcher.search(new TermQuery(new Term("id", "1")), 10).totalHits.value());
                    assertEquals(0, searcher.search(new TermQuery(new Term("id", "2")), 10).totalHits.value());
                }
                assertEquals(1L, readerEngine.currentManifestGenerationForTesting());

                // A second document is published as a new manifest generation -- simulating the
                // writer's own next flush, from the same continuing writer -- and the shard head is
                // advanced to point at it.
                CommitManifest secondManifest;
                {
                    Document doc2 = new Document();
                    doc2.add(new StringField("id", "2", Field.Store.YES));
                    writer.addDocument(doc2);
                    writer.commit();
                    SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
                    secondManifest = publisher.publishCommit(
                        writerDirectory,
                        segmentInfos,
                        indexUuid,
                        shardId,
                        PRIMARY_TERM,
                        2,
                        1,
                        1,
                        new WalPosition("epoch-0", 0),
                        0,
                        PruningStats.empty()
                    );
                }
                long currentVersion = shardStateStore.get(indexUuid, shardId).orElseThrow().version();
                assertEquals(
                    org.opensearch.serverless.storage.shardstate.CasResult.SUCCESS,
                    shardStateStore.compareAndSet(
                        indexUuid,
                        shardId,
                        java.util.Optional.of(currentVersion),
                        new org.opensearch.serverless.storage.shardstate.ShardHead(PRIMARY_TERM, null, 0L, secondManifest.generation())
                    )
                );

                readerEngine.pollForNewerManifestForTesting();

                assertEquals(
                    "the engine must have advanced to the newer manifest generation",
                    2L,
                    readerEngine.currentManifestGenerationForTesting()
                );
                try (Engine.Searcher searcher = readerEngine.acquireSearcher("test")) {
                    assertEquals(
                        "doc 2, only present in the newer generation, must now be visible without a full engine reopen",
                        1,
                        searcher.search(new TermQuery(new Term("id", "2")), 10).totalHits.value()
                    );
                }
            }
        } finally {
            writer.close();
            writerDirectory.close();
        }
    }
}
