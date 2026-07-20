/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

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
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinRecord;

import java.util.Set;

/**
 * {@link EngineNativeSnapshotSupport} is the shared restore/release helper reused by both a live
 * {@link WriterEngineFactory#recoverFromEngineNativeSnapshot} call and the node-level {@link
 * org.opensearch.index.engine.EngineNativeSnapshotReleasers} registry -- proves both of its own
 * operations directly against a real, disk-backed source container, the same way {@code
 * ShardClonerTests} proves {@link org.opensearch.serverless.storage.clone.ShardCloner}: a real
 * Lucene commit published for real, then read back/released through this class alone.
 */
public class EngineNativeSnapshotSupportTests extends IndexShardTestCase {

    private static final String SOURCE_INDEX_UUID = "source-idx";
    private static final int SHARD_ID = 0;
    private static final long PRIMARY_TERM = 1;

    private BlobContainer sourceContainer;
    private BlobContainerBundleStore sourceBundleStore;
    private BlobContainerManifestStore sourceManifestStore;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore sourceBlobStore = new FsBlobStore(1024, createTempDir(), false);
        sourceContainer = new FsBlobContainer(sourceBlobStore, BlobPath.cleanPath(), sourceBlobStore.path());
        sourceBundleStore = new BlobContainerBundleStore(sourceContainer);
        sourceManifestStore = new BlobContainerManifestStore(sourceContainer);
    }

    /** Real IndexWriter -> real ObjectStoreCommitPublisher -> real published commit, mirroring ShardClonerTests's own helper. */
    private CommitManifest publishSourceCommit() throws Exception {
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(sourceBundleStore, sourceManifestStore);
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
            return publisher.publishCommit(
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
    }

    public void testRecoverFromEngineNativeSnapshotMaterializesTheOriginalCommitIntoTheTargetStore() throws Exception {
        CommitManifest manifest = publishSourceCommit();
        EngineNativeSnapshotPayload payload = new EngineNativeSnapshotPayload("test-snap-uuid", manifest);
        EngineNativeSnapshotSupport support = new EngineNativeSnapshotSupport((indexUuid, shardId) -> sourceContainer);

        IndexShard targetShard = newShard(true);
        try {
            boolean recovered = support.recoverFromEngineNativeSnapshot(targetShard, targetShard.store(), payload.toBytes());
            assertTrue("a valid engine-native pointer must always be recoverable", recovered);

            try (DirectoryReader reader = DirectoryReader.open(targetShard.store().directory())) {
                IndexSearcher searcher = new IndexSearcher(reader);
                TopDocs hits = searcher.search(new TermQuery(new Term("id", "1")), 10);
                assertEquals("doc 1 from the original shard must be present in the restored target store", 1, hits.totalHits.value());
                hits = searcher.search(new TermQuery(new Term("id", "2")), 10);
                assertEquals("doc 2 from the original shard must be present in the restored target store", 1, hits.totalHits.value());
            }
        } finally {
            closeShard(targetShard, false);
        }
    }

    public void testReleaseEngineNativeSnapshotRemovesExactlyItsOwnPin() throws Exception {
        CommitManifest manifest = publishSourceCommit();
        String pinId = "test-snap-uuid";
        EngineNativeSnapshotPayload payload = new EngineNativeSnapshotPayload(pinId, manifest);
        EngineNativeSnapshotSupport support = new EngineNativeSnapshotSupport((indexUuid, shardId) -> sourceContainer);

        DurablePinRegistry sourcePinRegistry = new BlobContainerDurablePinRegistry(sourceContainer);
        PinRecord ownPin = new PinRecord(pinId, manifest.primaryTerm(), manifest.generation());
        PinRecord unrelatedPin = new PinRecord("unrelated-pin", manifest.primaryTerm(), manifest.generation());
        sourcePinRegistry.addPin(SOURCE_INDEX_UUID, SHARD_ID, ownPin);
        sourcePinRegistry.addPin(SOURCE_INDEX_UUID, SHARD_ID, unrelatedPin);

        support.releaseEngineNativeSnapshot(payload.toBytes());

        assertEquals(
            "release must remove exactly the pin its own payload identifies, leaving an unrelated pin (e.g. a concurrent PITR "
                + "pin) on the same generation untouched",
            Set.of(unrelatedPin),
            sourcePinRegistry.getPins(SOURCE_INDEX_UUID, SHARD_ID)
        );
    }
}
