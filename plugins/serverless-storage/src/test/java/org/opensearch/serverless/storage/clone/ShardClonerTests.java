/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.clone;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
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
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinRecord;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

public class ShardClonerTests extends OpenSearchTestCase {

    private static final String SOURCE_INDEX_UUID = "source-idx";
    private static final String TARGET_INDEX_UUID = "target-idx";
    private static final int SHARD_ID = 0;
    private static final long PRIMARY_TERM = 1;

    private BlobContainer sourceContainer;
    private BlobContainer targetContainer;
    private BlobContainerManifestStore sourceManifestStore;
    private BlobContainerManifestStore targetManifestStore;
    private BlobContainerBundleStore sourceBundleStore;
    private BlobContainerBundleStore targetBundleStore;
    private ShardStateStore sourceShardStateStore;
    private ShardStateStore targetShardStateStore;
    private DurablePinRegistry sourcePinRegistry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore sourceBlobStore = new FsBlobStore(1024, createTempDir(), false);
        sourceContainer = new FsBlobContainer(sourceBlobStore, BlobPath.cleanPath(), sourceBlobStore.path());
        FsBlobStore targetBlobStore = new FsBlobStore(1024, createTempDir(), false);
        targetContainer = new FsBlobContainer(targetBlobStore, BlobPath.cleanPath(), targetBlobStore.path());

        sourceManifestStore = new BlobContainerManifestStore(sourceContainer);
        targetManifestStore = new BlobContainerManifestStore(targetContainer);
        sourceBundleStore = new BlobContainerBundleStore(sourceContainer);
        targetBundleStore = new BlobContainerBundleStore(targetContainer);
        sourceShardStateStore = new BlobContainerShardStateStore(sourceContainer);
        targetShardStateStore = new BlobContainerShardStateStore(targetContainer);
        sourcePinRegistry = new BlobContainerDurablePinRegistry(sourceContainer);
    }

    /** Real IndexWriter -> real ObjectStoreCommitPublisher -> real published head, exactly what a live writer shard would have produced. */
    private CommitManifest publishSourceCommit() throws Exception {
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(sourceBundleStore, sourceManifestStore);
        CommitManifest manifest;
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                Document doc1 = new Document();
                doc1.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc1);
                Document doc2 = new Document();
                doc2.add(new StringField("id", "2", Field.Store.YES));
                writer.addDocument(doc2);
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            manifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                SOURCE_INDEX_UUID,
                SHARD_ID,
                PRIMARY_TERM,
                1,
                1,
                1,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
        }
        assertEquals(
            CasResult.SUCCESS,
            sourceShardStateStore.compareAndSet(
                SOURCE_INDEX_UUID,
                SHARD_ID,
                java.util.Optional.empty(),
                new ShardHead(PRIMARY_TERM, null, 0L, manifest.generation())
            )
        );
        return manifest;
    }

    public void testCloneRefusesASourceWithNoPublishedManifest() {
        expectThrows(
            java.io.IOException.class,
            () -> ShardCloner.clone(
                SOURCE_INDEX_UUID,
                SHARD_ID,
                sourceManifestStore,
                sourceShardStateStore,
                sourcePinRegistry,
                TARGET_INDEX_UUID,
                SHARD_ID,
                targetManifestStore,
                targetShardStateStore,
                1L
            )
        );
    }

    public void testCloneRefusesToOverwriteATargetThatAlreadyHasAPublishedHead() throws Exception {
        publishSourceCommit();
        assertEquals(
            CasResult.SUCCESS,
            targetShardStateStore.compareAndSet(TARGET_INDEX_UUID, SHARD_ID, java.util.Optional.empty(), ShardHead.initial())
        );
        expectThrows(
            java.io.IOException.class,
            () -> ShardCloner.clone(
                SOURCE_INDEX_UUID,
                SHARD_ID,
                sourceManifestStore,
                sourceShardStateStore,
                sourcePinRegistry,
                TARGET_INDEX_UUID,
                SHARD_ID,
                targetManifestStore,
                targetShardStateStore,
                1L
            )
        );
    }

    public void testClonePinsTheExactSourceGenerationItClonedFrom() throws Exception {
        CommitManifest sourceManifest = publishSourceCommit();
        ShardCloner.clone(
            SOURCE_INDEX_UUID,
            SHARD_ID,
            sourceManifestStore,
            sourceShardStateStore,
            sourcePinRegistry,
            TARGET_INDEX_UUID,
            SHARD_ID,
            targetManifestStore,
            targetShardStateStore,
            System.currentTimeMillis()
        );
        Set<PinRecord> pins = sourcePinRegistry.getPins(SOURCE_INDEX_UUID, SHARD_ID);
        assertEquals(1, pins.size());
        PinRecord pin = pins.iterator().next();
        assertEquals(ShardCloner.clonePinId(TARGET_INDEX_UUID, SHARD_ID), pin.pinId());
        assertEquals(sourceManifest.primaryTerm(), pin.primaryTerm());
        assertEquals(sourceManifest.generation(), pin.generation());
    }

    public void testClonedShardIsSearchableThroughTheFallbackReaderWithoutCopyingAnyBundleBytes() throws Exception {
        publishSourceCommit();
        long cloneTimeMillis = System.currentTimeMillis();
        ShardCloner.clone(
            SOURCE_INDEX_UUID,
            SHARD_ID,
            sourceManifestStore,
            sourceShardStateStore,
            sourcePinRegistry,
            TARGET_INDEX_UUID,
            SHARD_ID,
            targetManifestStore,
            targetShardStateStore,
            cloneTimeMillis
        );

        // The target's own bundle container is empty -- nothing was copied -- so a materializer
        // that only consulted it would fail to find every file the cloned manifest references.
        assertTrue(
            "clone must not have copied any bundle bytes into the target's own container",
            targetBundleStore.listBundleNames().isEmpty()
        );

        java.util.Optional<org.opensearch.serverless.storage.shardstate.VersionedShardHead> targetHead = targetShardStateStore.get(
            TARGET_INDEX_UUID,
            SHARD_ID
        );
        assertTrue(targetHead.isPresent());
        CommitManifest targetManifest = targetManifestStore.readManifest(
            targetHead.get().head().primaryTerm(),
            targetHead.get().head().latestManifestGeneration()
        );
        assertEquals(TARGET_INDEX_UUID, targetManifest.indexUuid());

        ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(
            new FallbackBundleFileReader(targetBundleStore, sourceBundleStore)
        );
        try (Directory materialized = new ByteBuffersDirectory()) {
            materializer.materialize(targetManifest, materialized);
            try (DirectoryReader reader = DirectoryReader.open(materialized)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                TopDocs hits = searcher.search(new TermQuery(new Term("id", "1")), 10);
                assertEquals(1, hits.totalHits.value());
                hits = searcher.search(new TermQuery(new Term("id", "2")), 10);
                assertEquals(1, hits.totalHits.value());
            }
        }
    }

}
