/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.migration;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
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
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.readerengine.ObjectStoreReaderEngine;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;

import java.util.Optional;

/**
 * Proves rfc-serverless-opensearch.md &sect;16 Phase 6's "conversion is manifest synthesis...
 * not data re-upload" claim end to end: an already-locally-populated Lucene directory (standing
 * in for a classic index's real recovered state, whatever recovery path produced it) is adopted
 * into serverless storage via {@link ClassicIndexMigrator#migrate} alone, and the result is a real,
 * openable {@link ObjectStoreReaderEngine} serving the migrated documents -- proof this isn't just
 * writing a manifest that looks right, but one a real reader engine actually accepts and serves.
 */
public class ClassicIndexMigratorTests extends EngineTestCase {

    private static final String LOCAL_NODE_ID = "test-node";

    private BlobContainer newFsBlobContainer() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    public void testMigratingAnAlreadyPopulatedDirectoryProducesARealOpenableManifest() throws Exception {
        BlobContainer blobContainer = newFsBlobContainer();
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);

        String indexUuid = "classic-index-uuid";
        int shardId = 0;
        long primaryTerm = 1L;

        try (Directory classicDirectory = new ByteBuffersDirectory()) {
            // Stands in for a classic index's real, already-recovered local commit -- migrate()
            // doesn't know or care that this came from a plain IndexWriter rather than real peer
            // recovery, a remote-store restore, or a snapshot-mount import; all three leave behind
            // exactly this: an ordinary valid local Lucene commit.
            try (IndexWriter writer = new IndexWriter(classicDirectory, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new StringField("id", "pre-existing-doc", Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(classicDirectory);

            CommitManifest manifest = ClassicIndexMigrator.migrate(
                classicDirectory,
                segmentInfos,
                indexUuid,
                shardId,
                primaryTerm,
                0,
                0,
                commitPublisher,
                shardStateStore
            );

            assertEquals(1L, manifest.generation());
            assertEquals(primaryTerm, manifest.primaryTerm());

            Optional<VersionedShardHead> head = shardStateStore.get(indexUuid, shardId);
            assertTrue("migration must CAS in a real shard head", head.isPresent());
            assertEquals(primaryTerm, head.get().head().primaryTerm());
            assertEquals(1L, head.get().head().latestManifestGeneration());

            // The real proof: a genuine ObjectStoreReaderEngine, built the exact same way any
            // ordinary reader shard would be, must open against this manifest and serve the
            // migrated document -- not just "a manifest object with plausible-looking fields."
            try (Store store = createStore()) {
                EngineConfig engineConfig = config(defaultSettings, store, createTempDir(), newMergePolicy(), null);
                ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer));
                ShardDirectory shardDirectory = new InMemoryShardDirectory();

                try (
                    ObjectStoreReaderEngine readerEngine = ObjectStoreReaderEngine.open(
                        engineConfig,
                        manifest,
                        materializer,
                        primaryTerm,
                        shardStateStore,
                        new BlobContainerManifestStore(blobContainer),
                        shardDirectory,
                        LOCAL_NODE_ID
                    )
                ) {
                    try (Engine.Searcher searcher = readerEngine.acquireSearcher("test")) {
                        TopDocs hits = searcher.search(new TermQuery(new Term("id", "pre-existing-doc")), 10);
                        assertEquals("the migrated document must be searchable through a real reader engine", 1, hits.totalHits.value());
                    }
                }
            }
        }
    }

    public void testMigratingAShardThatAlreadyHasAHeadFailsRatherThanOverwriting() throws Exception {
        BlobContainer blobContainer = newFsBlobContainer();
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);

        String indexUuid = "already-active-index";
        int shardId = 0;

        // A shard that's already active under serverless storage -- migration must refuse to touch it.
        assertEquals(
            CasResult.SUCCESS,
            shardStateStore.compareAndSet(indexUuid, shardId, Optional.empty(), new ShardHead(1L, null, 0L, 5L))
        );

        try (Directory directory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);

            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> ClassicIndexMigrator.migrate(directory, segmentInfos, indexUuid, shardId, 1L, 0, 0, commitPublisher, shardStateStore)
            );
            assertTrue(e.getMessage(), e.getMessage().contains("already has a serverless-storage head"));

            // The pre-existing head must be completely untouched by the refused migration attempt.
            Optional<VersionedShardHead> head = shardStateStore.get(indexUuid, shardId);
            assertEquals(5L, head.orElseThrow().head().latestManifestGeneration());
        }
    }
}
