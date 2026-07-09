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
                admissionController,
                null,
                null
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
                            admissionController,
                            null,
                            null
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

    public void testEngineSkipsAPollTickWhileOverBudgetAndCatchesUpOnceBudgetEases() throws Exception {
        // rfc-serverless-opensearch.md &sect;18 risk #3's per-refresh admission control: a poll
        // tick found while the node's file cache is over budget must NOT materialize the newer
        // generation -- the shard keeps serving its current, stale generation instead. Once the
        // cache eases back under budget, the very next poll tick catches up normally.
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

        org.opensearch.index.store.remote.filecache.FileCache fileCache = org.opensearch.index.store.remote.filecache.FileCacheFactory
            .createConcurrentLRUFileCache(1024L, 1);
        ReaderShardAdmissionController admissionController = new ReaderShardAdmissionController(10, fileCache, 0.5);

        Directory writerDirectory = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig());

        try (Store store = createStore()) {
            EngineConfig engineConfig = config(defaultSettings, store, createTempDir(), newMergePolicy(), null);
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

            // Opened while the cache is still empty (well under budget), so admission succeeds.
            try (
                ObjectStoreReaderEngine readerEngine = ObjectStoreReaderEngine.open(
                    engineConfig,
                    firstManifest,
                    materializer,
                    PRIMARY_TERM,
                    shardStateStore,
                    manifestStore,
                    shardDirectory,
                    LOCAL_NODE_ID,
                    admissionController,
                    null,
                    null
                )
            ) {
                assertEquals(1L, readerEngine.currentManifestGenerationForTesting());

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

                // Push the shared cache over its configured 50% budget before the next poll tick.
                fileCache.put(createTempDir().resolve("over-budget"), new org.opensearch.index.store.remote.filecache.CachedIndexInput() {
                    @Override
                    public org.apache.lucene.store.IndexInput getIndexInput() {
                        throw new UnsupportedOperationException("not needed for admission-control tests");
                    }

                    @Override
                    public long length() {
                        return 600L;
                    }

                    @Override
                    public boolean isClosed() {
                        return false;
                    }

                    @Override
                    public void close() {}
                });
                assertTrue(admissionController.isOverBudgetForRefresh());

                readerEngine.pollForNewerManifestForTesting();
                assertEquals(
                    "an over-budget poll tick must not materialize the newer generation",
                    1L,
                    readerEngine.currentManifestGenerationForTesting()
                );

                // Budget eases back (e.g. the over-budget entry is evicted) -- the very next poll
                // tick must catch up normally, with no other state having been disturbed.
                fileCache.clear();
                assertFalse(admissionController.isOverBudgetForRefresh());

                readerEngine.pollForNewerManifestForTesting();
                assertEquals(
                    "the engine must catch up to the newer generation once budget eases",
                    2L,
                    readerEngine.currentManifestGenerationForTesting()
                );
            }
        } finally {
            writer.close();
            writerDirectory.close();
        }
    }

    public void testReaderEnginesOwnBackgroundSchedulerCompactsAQuiescentShardWithNoWriterEverActivating() throws Exception {
        // The gap found while wiring this up: CompactionSchedulerTask was fully implemented and
        // tested in isolation, but nothing in ServerlessStoragePlugin ever actually constructed one
        // -- background compaction was real in tests only, never in a running node. This proves the
        // real wiring: a reader engine's own scheduler compacts a multi-segment, writer-less shard
        // on its own, with no test-only direct method call the way testEngineAdvancesToANewerManifestGenerationOncePublished
        // above exercises pollForNewerManifest.
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

        Directory writerDirectory = new ByteBuffersDirectory();
        // No merges of its own: every commit below must land as a genuinely separate segment, so
        // there is real multi-segment work for the compactor to do.
        IndexWriter writer = new IndexWriter(
            writerDirectory,
            new IndexWriterConfig().setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE)
        );

        try (Store store = createStore()) {
            EngineConfig engineConfig = config(defaultSettings, store, createTempDir(), newMergePolicy(), null);
            String indexUuid = engineConfig.getShardId().getIndex().getUUID();
            int shardId = engineConfig.getShardId().getId();

            CommitManifest manifest = null;
            for (int generation = 1; generation <= 3; generation++) {
                Document doc = new Document();
                doc.add(new StringField("id", String.valueOf(generation), Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
                SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
                manifest = publisher.publishCommit(
                    writerDirectory,
                    segmentInfos,
                    indexUuid,
                    shardId,
                    PRIMARY_TERM,
                    generation,
                    generation - 1,
                    generation - 1,
                    new WalPosition("epoch-0", 0),
                    0,
                    PruningStats.empty()
                );
            }
            // No lease held -- exactly the "no writer ever activating" case the scheduler exists
            // for; the manifest's own segment count (3, one per commit above, no local merging) is
            // what CompactionPolicy below is tuned to treat as a candidate.
            assertEquals(
                org.opensearch.serverless.storage.shardstate.CasResult.SUCCESS,
                shardStateStore.compareAndSet(
                    indexUuid,
                    shardId,
                    java.util.Optional.empty(),
                    new org.opensearch.serverless.storage.shardstate.ShardHead(PRIMARY_TERM, null, 0L, manifest.generation())
                )
            );

            org.opensearch.serverless.storage.compaction.CompactionSchedulerConfig compactionConfig =
                new org.opensearch.serverless.storage.compaction.CompactionSchedulerConfig(
                    org.opensearch.common.unit.TimeValue.timeValueMillis(20),
                    manifestStore,
                    new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer)),
                    publisher,
                    new org.opensearch.serverless.storage.compaction.CompactionPolicy(2, 5L * 1024 * 1024 * 1024, 0.99),
                    new org.opensearch.serverless.storage.compaction.CompactionRebaseExecutor(shardStateStore, 5)
                );

            try (
                ObjectStoreReaderEngine readerEngine = ObjectStoreReaderEngine.open(
                    engineConfig,
                    manifest,
                    materializer,
                    PRIMARY_TERM,
                    shardStateStore,
                    manifestStore,
                    shardDirectory,
                    LOCAL_NODE_ID,
                    null,
                    compactionConfig,
                    null
                )
            ) {
                long generationBeforeCompaction = manifest.generation();
                assertBusy(() -> {
                    org.opensearch.serverless.storage.shardstate.VersionedShardHead current = shardStateStore.get(indexUuid, shardId)
                        .orElseThrow();
                    assertTrue(
                        "the background scheduler must have published a newer, compacted manifest generation on its own",
                        current.head().latestManifestGeneration() > generationBeforeCompaction
                    );
                    CommitManifest compacted = manifestStore.readManifest(PRIMARY_TERM, current.head().latestManifestGeneration());
                    // The whole point of compaction: fewer Lucene segments than the 3 uncompacted
                    // commits above produced (each with its own multi-file footprint -- .si, .cfs,
                    // etc. -- so comparing files().size() directly would be counting the wrong
                    // thing), with no writer ever activating to do it itself.
                    int compactedSegmentCount = org.opensearch.serverless.storage.compaction.ManifestSegmentMetrics.from(
                        compacted
                    ).segmentCount;
                    assertTrue(
                        "the compacted manifest must have fewer segments than the uncompacted one, got " + compactedSegmentCount,
                        compactedSegmentCount < 3
                    );
                });
            }
        } finally {
            writer.close();
            writerDirectory.close();
        }
    }

    public void testReaderEnginesOwnBackgroundSchedulerSweepsASupersededUnpinnedManifestAndItsBundle() throws Exception {
        // Same "implemented and tested in isolation, never actually wired into a running node" gap
        // as CompactionSchedulerTask above, found for GcSchedulerTask/BundleReferenceCounter/
        // ManifestRetentionPolicy while wiring the compaction scheduler in: nothing ever deleted a
        // superseded manifest or its now-unreferenced bundle. See GcSchedulerTaskTests for the
        // retention-window/pin-safety unit coverage; this proves the real scheduled wiring.
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );
        org.opensearch.serverless.storage.shardstate.ShardStateStore shardStateStore =
            new org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore(blobContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);
        ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(bundleStore);
        org.opensearch.serverless.storage.retention.DurablePinRegistry pinRegistry =
            new org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry(blobContainer);
        ShardDirectory shardDirectory = new InMemoryShardDirectory();

        Directory writerDirectory = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(
            writerDirectory,
            new IndexWriterConfig().setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE)
        );

        try (Store store = createStore()) {
            EngineConfig engineConfig = config(defaultSettings, store, createTempDir(), newMergePolicy(), null);
            String indexUuid = engineConfig.getShardId().getIndex().getUUID();
            int shardId = engineConfig.getShardId().getId();

            CommitManifest gen1;
            {
                Document doc = new Document();
                doc.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
                gen1 = publisher.publishCommit(
                    writerDirectory,
                    SegmentInfos.readLatestCommit(writerDirectory),
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
            CommitManifest gen2;
            {
                Document doc = new Document();
                doc.add(new StringField("id", "2", Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
                gen2 = publisher.publishCommit(
                    writerDirectory,
                    SegmentInfos.readLatestCommit(writerDirectory),
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
            // No lease held, gen 2 is the published latest -- gen 1 is superseded and unpinned, so
            // once past the (deliberately tiny, for this test) retention window it must be swept.
            assertEquals(
                org.opensearch.serverless.storage.shardstate.CasResult.SUCCESS,
                shardStateStore.compareAndSet(
                    indexUuid,
                    shardId,
                    java.util.Optional.empty(),
                    new org.opensearch.serverless.storage.shardstate.ShardHead(PRIMARY_TERM, null, 0L, gen2.generation())
                )
            );

            org.opensearch.serverless.storage.gc.GcSchedulerConfig gcConfig = new org.opensearch.serverless.storage.gc.GcSchedulerConfig(
                org.opensearch.common.unit.TimeValue.timeValueMillis(20),
                1L, // effectively no retention delay -- the point here is the wiring, not the window
                manifestStore,
                bundleStore,
                pinRegistry
            );

            try (
                ObjectStoreReaderEngine readerEngine = ObjectStoreReaderEngine.open(
                    engineConfig,
                    gen2,
                    materializer,
                    PRIMARY_TERM,
                    shardStateStore,
                    manifestStore,
                    shardDirectory,
                    LOCAL_NODE_ID,
                    null,
                    null,
                    gcConfig
                )
            ) {
                String gen1Bundle = gen1.referencedBundles().iterator().next();
                String gen2Bundle = gen2.referencedBundles().iterator().next();
                assertBusy(() -> {
                    java.util.List<CommitManifest> remaining = manifestStore.listManifests();
                    assertFalse(
                        "gen 1 is superseded and unpinned -- the background scheduler must have swept it on its own",
                        remaining.stream().anyMatch(m -> m.generation() == gen1.generation())
                    );
                    assertTrue(
                        "gen 2 is the current latest -- it must never be swept",
                        remaining.stream().anyMatch(m -> m.generation() == gen2.generation())
                    );
                    java.util.Set<String> remainingBundles = bundleStore.listBundleNames();
                    assertFalse("gen 1's now-orphaned bundle must have been deleted too", remainingBundles.contains(gen1Bundle));
                    assertTrue("gen 2's bundle must survive", remainingBundles.contains(gen2Bundle));
                });
            }
        } finally {
            writer.close();
            writerDirectory.close();
        }
    }
}
