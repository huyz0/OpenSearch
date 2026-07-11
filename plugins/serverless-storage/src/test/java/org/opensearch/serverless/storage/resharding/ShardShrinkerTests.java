/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.resharding.ShardShrinker.ShrinkSource;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ShardShrinkerTests extends OpenSearchTestCase {

    private static final String TARGET_INDEX_UUID = "shrink-target-idx";
    private static final int SHARD_ID = 0;

    private BlobContainer targetContainer;
    private ShardStateStore targetShardStateStore;
    private ObjectStoreCommitPublisher targetCommitPublisher;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore targetBlobStore = new FsBlobStore(1024, createTempDir(), false);
        targetContainer = new FsBlobContainer(targetBlobStore, BlobPath.cleanPath(), targetBlobStore.path());
        targetShardStateStore = new BlobContainerShardStateStore(targetContainer);
        targetCommitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(targetContainer),
            new BlobContainerManifestStore(targetContainer)
        );
    }

    private ShrinkSource publishSource(String indexUuid, int startId, int count, long maxSeqNo, long mappingVersion) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer container = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(container);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(container);
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);

        CommitManifest manifest;
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                for (int i = 0; i < count; i++) {
                    Document doc = new Document();
                    doc.add(
                        new Field(IdFieldMapper.NAME, Uid.encodeId(indexUuid + "-doc-" + (startId + i)), IdFieldMapper.Defaults.FIELD_TYPE)
                    );
                    writer.addDocument(doc);
                }
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            manifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                indexUuid,
                SHARD_ID,
                1,
                1,
                maxSeqNo,
                maxSeqNo,
                new WalPosition("epoch-0", 0),
                mappingVersion,
                PruningStats.empty()
            );
        }
        return new ShrinkSource(manifest, new ObjectStoreCommitMaterializer(bundleStore));
    }

    public void testShrinkRejectsAnEmptySourceList() {
        expectThrows(
            java.io.IOException.class,
            () -> ShardShrinker.shrink(List.of(), TARGET_INDEX_UUID, SHARD_ID, targetShardStateStore, targetCommitPublisher, 1L)
        );
    }

    public void testShrinkMergesEveryDocumentFromEverySourceIntoOneTarget() throws Exception {
        ShrinkSource sourceA = publishSource("shrink-source-a", 0, 5, 4, 1);
        ShrinkSource sourceB = publishSource("shrink-source-b", 100, 7, 6, 2);
        ShrinkSource sourceC = publishSource("shrink-source-c", 200, 3, 2, 0);

        ShardShrinker.shrink(
            List.of(sourceA, sourceB, sourceC),
            TARGET_INDEX_UUID,
            SHARD_ID,
            targetShardStateStore,
            targetCommitPublisher,
            System.currentTimeMillis()
        );

        VersionedShardHead targetHead = targetShardStateStore.get(TARGET_INDEX_UUID, SHARD_ID).orElseThrow();
        BlobContainerManifestStore targetManifestStore = new BlobContainerManifestStore(targetContainer);
        CommitManifest targetManifest = targetManifestStore.readManifest(
            targetHead.head().primaryTerm(),
            targetHead.head().latestManifestGeneration()
        );

        Directory targetDirectory = new ByteBuffersDirectory();
        new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(targetContainer)).materialize(targetManifest, targetDirectory);
        try (DirectoryReader reader = DirectoryReader.open(targetDirectory)) {
            assertEquals("the target must contain every document from every source, no more, no fewer", 15, reader.numDocs());
            Set<String> ids = new HashSet<>();
            for (int i = 0; i < reader.maxDoc(); i++) {
                byte[] idBytes = reader.storedFields().document(i).getField(IdFieldMapper.NAME).binaryValue().bytes;
                ids.add(Uid.decodeId(idBytes));
            }
            assertEquals(15, ids.size());
        }

        assertEquals("the merged maxSeqNo must be the maximum across every source", 6L, targetManifest.maxSeqNo());
        assertEquals("the merged mappingVersion must be the maximum across every source", 2L, targetManifest.mappingVersion());
    }

    public void testShrinkRefusesToOverwriteATargetThatAlreadyHasAPublishedHead() throws Exception {
        ShrinkSource sourceA = publishSource("shrink-source-a2", 0, 2, 1, 0);
        ShrinkSource sourceB = publishSource("shrink-source-b2", 10, 2, 1, 0);
        assertEquals(
            CasResult.SUCCESS,
            targetShardStateStore.compareAndSet(TARGET_INDEX_UUID, SHARD_ID, Optional.empty(), ShardHead.initial())
        );

        List<ShrinkSource> sources = new ArrayList<>();
        sources.add(sourceA);
        sources.add(sourceB);
        expectThrows(
            java.io.IOException.class,
            () -> ShardShrinker.shrink(sources, TARGET_INDEX_UUID, SHARD_ID, targetShardStateStore, targetCommitPublisher, 1L)
        );
    }
}
