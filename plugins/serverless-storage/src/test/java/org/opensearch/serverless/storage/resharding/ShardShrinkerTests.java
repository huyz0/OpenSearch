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
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
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
    private BlobContainerManifestStore targetManifestStore;
    private ObjectStoreCommitPublisher targetCommitPublisher;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore targetBlobStore = new FsBlobStore(1024, createTempDir(), false);
        targetContainer = new FsBlobContainer(targetBlobStore, BlobPath.cleanPath(), targetBlobStore.path());
        targetShardStateStore = new BlobContainerShardStateStore(targetContainer);
        targetManifestStore = new BlobContainerManifestStore(targetContainer);
        targetCommitPublisher = new ObjectStoreCommitPublisher(new BlobContainerBundleStore(targetContainer), targetManifestStore);
    }

    /** One document, shaped the way an OpenSearch writer shapes a root document. */
    private static Document document(String id) {
        Document doc = new Document();
        doc.add(new Field(IdFieldMapper.NAME, Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, 1L));
        return doc;
    }

    private ShrinkSource publishSource(String indexUuid, int startId, int count, long maxSeqNo, long mappingVersion) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer container = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(container);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(container);
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);

        CommitManifest manifest;
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            // Finding R-9: every source in this package used to be built with a bare
            // IndexWriterConfig and never took a delete or an update, so no __soft_deletes FieldInfo
            // ever existed and IndexWriter.addIndexes' FieldInfos.verifySoftDeletedFieldName check
            // never fired. Any real shard that has taken a single DELETE or _update carries that
            // FieldInfo, so ShardShrinker threw IllegalArgumentException on it -- via an
            // always-reachable REST action. One softUpdateDocument here reproduces it.
            try (
                IndexWriter writer = new IndexWriter(
                    writerDirectory,
                    new IndexWriterConfig().setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
                )
            ) {
                for (int i = 0; i < count; i++) {
                    writer.addDocument(document(indexUuid + "-doc-" + (startId + i)));
                }
                // A real update of the first document, leaving its previous version soft-deleted.
                String updatedId = indexUuid + "-doc-" + startId;
                writer.softUpdateDocument(
                    new Term(IdFieldMapper.NAME, Uid.encodeId(updatedId)),
                    document(updatedId),
                    new NumericDocValuesField(Lucene.SOFT_DELETES_FIELD, 1)
                );
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
            () -> ShardShrinker.shrink(
                List.of(),
                TARGET_INDEX_UUID,
                SHARD_ID,
                targetShardStateStore,
                targetCommitPublisher,
                targetManifestStore,
                1L
            )
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
            targetManifestStore,
            System.currentTimeMillis()
        );

        VersionedShardHead targetHead = targetShardStateStore.get(TARGET_INDEX_UUID, SHARD_ID).orElseThrow();
        CommitManifest targetManifest = targetManifestStore.readManifest(
            targetHead.head().primaryTerm(),
            targetHead.head().latestManifestGeneration()
        );

        Directory targetDirectory = new ByteBuffersDirectory();
        new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(targetContainer)).materialize(targetManifest, targetDirectory);
        // Read through the soft-deletes wrapper, the way the engine's own read path does: each
        // source contributed one soft-deleted previous version (see publishSource), and a shrink is
        // required to carry that history across rather than drop it, so the physical segments hold
        // more documents than are live.
        try (
            DirectoryReader reader = new SoftDeletesDirectoryReaderWrapper(DirectoryReader.open(targetDirectory), Lucene.SOFT_DELETES_FIELD)
        ) {
            assertEquals("the target must contain every live document from every source, no more, no fewer", 15, reader.numDocs());
            Set<String> ids = new HashSet<>();
            org.apache.lucene.util.Bits liveDocs = org.apache.lucene.index.MultiBits.getLiveDocs(reader);
            for (int i = 0; i < reader.maxDoc(); i++) {
                if (liveDocs != null && liveDocs.get(i) == false) {
                    continue;
                }
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
            () -> ShardShrinker.shrink(
                sources,
                TARGET_INDEX_UUID,
                SHARD_ID,
                targetShardStateStore,
                targetCommitPublisher,
                targetManifestStore,
                1L
            )
        );
    }

    public void testShrinkRollsBackItsOwnManifestWhenTheHeadCasLosesToAnUnrelatedWrite() throws Exception {
        ShrinkSource sourceA = publishSource("shrink-source-a3", 0, 2, 1, 0);
        ShrinkSource sourceB = publishSource("shrink-source-b3", 10, 2, 1, 0);

        // Simulates "an ordinary index creation racing this call" (this method's own javadoc): some
        // unrelated writer already established the target's real head before shrink's own CAS runs,
        // at a DIFFERENT generation so shrink's own writeManifest at (1, 1) still lands successfully
        // -- only the head CAS itself loses.
        assertEquals(
            CasResult.SUCCESS,
            targetShardStateStore.compareAndSet(TARGET_INDEX_UUID, SHARD_ID, Optional.empty(), new ShardHead(1, null, 0L, 7))
        );

        List<ShrinkSource> sources = new ArrayList<>();
        sources.add(sourceA);
        sources.add(sourceB);
        expectThrows(
            java.io.IOException.class,
            () -> ShardShrinker.shrink(
                sources,
                TARGET_INDEX_UUID,
                SHARD_ID,
                targetShardStateStore,
                targetCommitPublisher,
                targetManifestStore,
                1L
            )
        );

        // The test container here is a plain, unrestricted FsBlobContainer (unlike the real,
        // delete-denied production wiring this method's own javadoc describes), so the best-effort
        // rollback delete genuinely succeeds and should leave no stray manifest behind.
        assertFalse(
            "a lost head CAS must roll back this attempt's own orphaned manifest when delete is actually permitted",
            targetManifestStore.manifestExists(1, 1)
        );
    }
}
