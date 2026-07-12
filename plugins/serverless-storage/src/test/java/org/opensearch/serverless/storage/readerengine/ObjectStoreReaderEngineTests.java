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

    public void testMillisSinceLastQueryUpdatesOnRealSearchesButNotOnInternalSearcherAcquisitions() throws Exception {
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
                assertTrue(
                    "freshly opened, never-queried engine should report roughly zero idle time, not an arbitrarily large one",
                    readerEngine.millisSinceLastQuery() < 5000
                );

                // docStats() acquires its searcher purely via SearcherScope.INTERNAL (unlike
                // segmentsStats(), which also does an EXTERNAL acquisition internally) -- must
                // never look like real query traffic.
                Thread.sleep(50);
                readerEngine.docStats();
                long idleAfterInternalOnly = readerEngine.millisSinceLastQuery();
                assertTrue("an internal-scope searcher acquisition must not reset the idle clock", idleAfterInternalOnly >= 40);

                // acquireSearcher("test") (used throughout this test class) goes through
                // SearcherScope.EXTERNAL -- a real client-facing query.
                try (Engine.Searcher searcher = readerEngine.acquireSearcher("test")) {
                    TopDocs hits = searcher.search(new TermQuery(new Term("id", "1")), 10);
                    assertEquals(1, hits.totalHits.value());
                }
                assertTrue("a real query must reset the idle clock back down near zero", readerEngine.millisSinceLastQuery() < 5000);

                // queriesPerMinute() deliberately reports the previous *completed* window's count,
                // never the in-progress one -- see the field's own javadoc. So a shard that has
                // only ever had queries land inside its very first (still-open) window reports 0,
                // not the count of queries it has actually seen so far.
                assertEquals(
                    "an engine still inside its first query-rate window must report 0, not the in-progress count",
                    0L,
                    readerEngine.queriesPerMinute()
                );
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

    public void testRegistryPollNowForcesAnImmediateCatchUpToANewerGeneration() throws Exception {
        // The receiving side of section 8's publication notification mechanism: a caller (standing
        // in for TransportPollNowAction) reaches a specific reader engine by (indexUuid, shardId)
        // through ReaderShardActivityRegistry alone, exactly as a real cross-node notification would.
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
        ReaderShardActivityRegistry registry = new ReaderShardActivityRegistry();

        Directory writerDirectory = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig());

        try (Store store = createStore()) {
            EngineConfig engineConfig = config(defaultSettings, store, createTempDir(), newMergePolicy(), null);
            String indexUuid = engineConfig.getShardId().getIndex().getUUID();
            int shardId = engineConfig.getShardId().getId();

            assertFalse("no reader engine registered yet -- must report false, not throw", registry.pollNow(indexUuid, shardId));

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
                registry.register(indexUuid, shardId, readerEngine);

                Document doc2 = new Document();
                doc2.add(new StringField("id", "2", Field.Store.YES));
                writer.addDocument(doc2);
                writer.commit();
                SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
                CommitManifest secondManifest = publisher.publishCommit(
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
                long currentVersion = shardStateStore.get(indexUuid, shardId).orElseThrow().version();
                shardStateStore.compareAndSet(
                    indexUuid,
                    shardId,
                    java.util.Optional.of(currentVersion),
                    new org.opensearch.serverless.storage.shardstate.ShardHead(PRIMARY_TERM, null, 0L, secondManifest.generation())
                );

                assertEquals(1L, readerEngine.currentManifestGenerationForTesting());
                assertTrue("a registered reader engine must be found and polled", registry.pollNow(indexUuid, shardId));
                assertEquals(
                    "pollNow must force an immediate catch-up, not wait for the background schedule",
                    2L,
                    readerEngine.currentManifestGenerationForTesting()
                );
            }
        } finally {
            writer.close();
            writerDirectory.close();
        }
    }

    public void testWaitForGenerationTimesOutIfTheGenerationNeverArrives() throws Exception {
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

        try (Directory writerDirectory = new ByteBuffersDirectory(); Store store = createStore()) {
            EngineConfig engineConfig = config(defaultSettings, store, createTempDir(), newMergePolicy(), null);
            String indexUuid = engineConfig.getShardId().getIndex().getUUID();
            int shardId = engineConfig.getShardId().getId();

            CommitManifest firstManifest;
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
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
                long start = System.currentTimeMillis();
                boolean reached = readerEngine.waitForGeneration(2L, org.opensearch.common.unit.TimeValue.timeValueMillis(200));
                long elapsed = System.currentTimeMillis() - start;
                assertFalse("a generation that never publishes must time out, not hang or return true", reached);
                assertTrue("must actually wait roughly the full timeout, not return immediately", elapsed >= 150);
                assertEquals(1L, readerEngine.currentManifestGenerationForTesting());
            }
        }
    }

    public void testWaitForGenerationReturnsTrueOnceThePublishLands() throws Exception {
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
                // Publishes the second manifest and CASes the head from a background thread, after a
                // short delay -- proof waitForGeneration actually blocks and picks it up, rather than
                // the generation already being there before the wait even starts.
                Thread publisherThread = new Thread(() -> {
                    try {
                        Thread.sleep(150);
                        Document doc2 = new Document();
                        doc2.add(new StringField("id", "2", Field.Store.YES));
                        writer.addDocument(doc2);
                        writer.commit();
                        SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
                        CommitManifest secondManifest = publisher.publishCommit(
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
                        long currentVersion = shardStateStore.get(indexUuid, shardId).orElseThrow().version();
                        shardStateStore.compareAndSet(
                            indexUuid,
                            shardId,
                            java.util.Optional.of(currentVersion),
                            new org.opensearch.serverless.storage.shardstate.ShardHead(PRIMARY_TERM, null, 0L, secondManifest.generation())
                        );
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                publisherThread.start();
                try {
                    boolean reached = readerEngine.waitForGeneration(2L, org.opensearch.common.unit.TimeValue.timeValueSeconds(10));
                    assertTrue("must return true once the background publish actually lands", reached);
                    assertEquals(2L, readerEngine.currentManifestGenerationForTesting());
                    try (Engine.Searcher searcher = readerEngine.acquireSearcher("test")) {
                        assertEquals(
                            "doc 2 must be visible once waitForGeneration returns true",
                            1,
                            searcher.search(new TermQuery(new Term("id", "2")), 10).totalHits.value()
                        );
                    }
                } finally {
                    publisherThread.join();
                }
            }
        } finally {
            writer.close();
            writerDirectory.close();
        }
    }

    /**
     * rfc-serverless-opensearch.md &sect;17's "Staleness/consistency" testing-strategy bullet:
     * "linearizability-style checker for the RYW path (indexed doc with generation token must be
     * visible to a routed search); monotonicity checker for readers." Runs a real writer publishing
     * 10 successive generations concurrently against this same reader engine, checked two ways at
     * once:
     *
     * <ul>
     *   <li>Monotonicity: three independent observer threads each repeatedly sample {@link
     *       ObjectStoreReaderEngine#currentManifestGenerationForTesting()} and record every value
     *       they personally saw -- each thread's own sequence must never decrease, exactly the
     *       "a reader never goes backward" property &sect;8's consistency model promises.
     *   <li>RYW linearizability: a checker thread calls {@link ObjectStoreReaderEngine#waitForGeneration}
     *       for each generation in strict order and, the instant it returns {@code true}, immediately
     *       searches for that generation's own uniquely-identifying document -- proving the
     *       linearization point actually holds: {@code waitForGeneration} returning {@code true}
     *       for generation N means N's writes are really visible to a search issued right then, not
     *       merely that some internal counter reached N.
     * </ul>
     */
    public void testMonotonicityAndReadYourWriteLinearizationHoldUnderConcurrentPublishing() throws Exception {
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
        IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig());

        try (Store store = createStore()) {
            EngineConfig engineConfig = config(defaultSettings, store, createTempDir(), newMergePolicy(), null);
            String indexUuid = engineConfig.getShardId().getIndex().getUUID();
            int shardId = engineConfig.getShardId().getId();

            Document firstDoc = new Document();
            firstDoc.add(new StringField("id", "gen-1", Field.Store.YES));
            writer.addDocument(firstDoc);
            writer.commit();
            SegmentInfos firstSegments = SegmentInfos.readLatestCommit(writerDirectory);
            CommitManifest firstManifest = publisher.publishCommit(
                writerDirectory,
                firstSegments,
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
            assertEquals(
                org.opensearch.serverless.storage.shardstate.CasResult.SUCCESS,
                shardStateStore.compareAndSet(
                    indexUuid,
                    shardId,
                    java.util.Optional.empty(),
                    new org.opensearch.serverless.storage.shardstate.ShardHead(PRIMARY_TERM, null, 0L, firstManifest.generation())
                )
            );

            int lastGeneration = 10;
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
                java.util.concurrent.atomic.AtomicBoolean publishingDone = new java.util.concurrent.atomic.AtomicBoolean(false);
                java.util.concurrent.atomic.AtomicReference<Throwable> failure = new java.util.concurrent.atomic.AtomicReference<>();

                Thread publisherThread = new Thread(() -> {
                    try {
                        for (int gen = 2; gen <= lastGeneration; gen++) {
                            Document doc = new Document();
                            doc.add(new StringField("id", "gen-" + gen, Field.Store.YES));
                            writer.addDocument(doc);
                            writer.commit();
                            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
                            CommitManifest manifest = publisher.publishCommit(
                                writerDirectory,
                                segmentInfos,
                                indexUuid,
                                shardId,
                                PRIMARY_TERM,
                                gen,
                                gen - 1,
                                gen - 1,
                                new WalPosition("epoch-0", 0),
                                0,
                                PruningStats.empty()
                            );
                            long currentVersion = shardStateStore.get(indexUuid, shardId).orElseThrow().version();
                            shardStateStore.compareAndSet(
                                indexUuid,
                                shardId,
                                java.util.Optional.of(currentVersion),
                                new org.opensearch.serverless.storage.shardstate.ShardHead(PRIMARY_TERM, null, 0L, manifest.generation())
                            );
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    } finally {
                        publishingDone.set(true);
                    }
                });

                // Three independent monotonicity observers.
                Thread[] observerThreads = new Thread[3];
                for (int i = 0; i < observerThreads.length; i++) {
                    observerThreads[i] = new Thread(() -> {
                        long lastSeen = 0;
                        try {
                            while (publishingDone.get() == false || lastSeen < lastGeneration) {
                                long current = readerEngine.currentManifestGenerationForTesting();
                                if (current < lastSeen) {
                                    throw new AssertionError(
                                        "monotonicity violated: observed generation "
                                            + current
                                            + " after already having observed "
                                            + lastSeen
                                    );
                                }
                                lastSeen = current;
                                if (publishingDone.get() && lastSeen >= lastGeneration) {
                                    break;
                                }
                            }
                        } catch (Throwable t) {
                            failure.compareAndSet(null, t);
                        }
                    });
                }

                // RYW linearizability checker: for each generation in order, wait for it, then
                // immediately verify its own document is really searchable right then.
                Thread rywThread = new Thread(() -> {
                    try {
                        for (int gen = 1; gen <= lastGeneration; gen++) {
                            boolean reached = readerEngine.waitForGeneration(
                                gen,
                                org.opensearch.common.unit.TimeValue.timeValueSeconds(30)
                            );
                            if (reached == false) {
                                throw new AssertionError("waitForGeneration(" + gen + ") timed out");
                            }
                            try (Engine.Searcher searcher = readerEngine.acquireSearcher("test")) {
                                int hits = searcher.search(new TermQuery(new Term("id", "gen-" + gen)), 10).totalHits.value() > 0 ? 1 : 0;
                                if (hits == 0) {
                                    throw new AssertionError(
                                        "RYW linearizability violated: waitForGeneration("
                                            + gen
                                            + ") returned true but gen-"
                                            + gen
                                            + "'s own document is not yet searchable"
                                    );
                                }
                            }
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                });

                publisherThread.start();
                for (Thread observer : observerThreads) {
                    observer.start();
                }
                rywThread.start();

                publisherThread.join();
                for (Thread observer : observerThreads) {
                    observer.join();
                }
                rywThread.join();

                if (failure.get() != null) {
                    throw new AssertionError("concurrent linearizability/monotonicity check failed", failure.get());
                }
                assertEquals(lastGeneration, readerEngine.currentManifestGenerationForTesting());
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
                assertEquals("nothing newer has been observed yet, so lag must be zero", 0L, readerEngine.manifestGenerationLag());

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
                assertEquals(
                    "an over-budget tick still reads the shard head before deciding to skip "
                        + "materialization, so it must correctly report the real one-generation gap "
                        + "it's now stuck behind -- not a stale zero from before the second manifest "
                        + "was even published",
                    1L,
                    readerEngine.manifestGenerationLag()
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
                assertEquals("caught up, so lag must be zero again", 0L, readerEngine.manifestGenerationLag());
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
