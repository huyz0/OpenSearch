/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
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
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;

public class ManifestSegmentMetricsTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;

    private ObjectStoreCommitPublisher commitPublisher;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );
    }

    /** Commits N separate single-document segments (no merge), so a real multi-segment index results. */
    private SegmentInfos commitSeparateSegments(Directory directory, int count) throws Exception {
        SegmentInfos infos = null;
        for (int i = 0; i < count; i++) {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                Document doc = new Document();
                doc.add(new StringField("id", "doc-" + i, Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }
            infos = SegmentInfos.readLatestCommit(directory);
        }
        return infos;
    }

    public void testSegmentCountMatchesTheRealNumberOfLuceneSegments() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos infos = commitSeparateSegments(directory, 5);
            assertEquals(5, infos.size());

            CommitManifest manifest = commitPublisher.publishCommit(
                directory,
                infos,
                INDEX_UUID,
                SHARD_ID,
                1,
                infos.getGeneration(),
                4,
                4,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );

            ManifestSegmentMetrics metrics = ManifestSegmentMetrics.from(manifest);
            assertEquals(5, metrics.segmentCount);
            long expectedTotalBytes = manifest.files().values().stream().mapToLong(f -> f.length()).sum();
            assertEquals(expectedTotalBytes, metrics.totalBytes);
            assertTrue(
                "estimator must not count the top-level segments_N file as a segment",
                metrics.segmentCount < manifest.files().size()
            );
            assertEquals("no documents were deleted, so the ratio is genuinely zero", 0.0, metrics.estimatedDeleteRatio, 0.0);
        }
    }

    public void testSingleSegmentCommitReportsOneSegmentAndItsTotalSizeAsTheLargest() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig();
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                Document doc = new Document();
                doc.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }
            SegmentInfos infos = SegmentInfos.readLatestCommit(directory);
            assertEquals(1, infos.size());

            CommitManifest manifest = commitPublisher.publishCommit(
                directory,
                infos,
                INDEX_UUID,
                SHARD_ID,
                1,
                infos.getGeneration(),
                0,
                0,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );

            ManifestSegmentMetrics metrics = ManifestSegmentMetrics.from(manifest);
            assertEquals(1, metrics.segmentCount);
            // totalBytes also includes the top-level segments_N file, which isn't part of any
            // segment, so it's strictly >= the one segment's own size, not necessarily equal.
            assertTrue(metrics.largestSingleSegmentBytes <= metrics.totalBytes);
            assertTrue(metrics.largestSingleSegmentBytes > 0);
        }
    }

    /**
     * Closes rfc-serverless-opensearch.md &sect;7.4's "genuinely unimplemented" gap: a real,
     * non-hypothetical soft delete (the dominant real-world deletion path, matching this section's
     * own investigation) must now produce a nonzero {@code estimatedDeleteRatio}, not the honest but
     * unconditional {@code 0.0} this class used to always return.
     */
    public void testEstimatedDeleteRatioReflectsARealSoftDeletedDocument() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            String softDeletesField = org.opensearch.common.lucene.Lucene.SOFT_DELETES_FIELD;
            IndexWriterConfig config = new IndexWriterConfig();
            config.setSoftDeletesField(softDeletesField);
            SegmentInfos infos;
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                for (int i = 0; i < 4; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", "doc-" + i, Field.Store.YES));
                    writer.addDocument(doc);
                }
                writer.commit();

                // Soft-delete one of the four documents -- the same "re-index the tombstone with a
                // soft-deletes doc-value" path every OpenSearch index actually uses by default.
                Document tombstone = new Document();
                tombstone.add(new StringField("id", "doc-0", Field.Store.YES));
                writer.softUpdateDocument(new Term("id", "doc-0"), tombstone, new NumericDocValuesField(softDeletesField, 1));
                writer.commit();
                infos = SegmentInfos.readLatestCommit(directory);
            }

            long realSoftDeleteCount = infos.asList().stream().mapToLong(SegmentCommitInfo::getSoftDelCount).sum();
            assertTrue("test setup must produce a real soft-deleted document", realSoftDeleteCount > 0);

            CommitManifest manifest = commitPublisher.publishCommit(
                directory,
                infos,
                INDEX_UUID,
                SHARD_ID,
                1,
                infos.getGeneration(),
                3,
                3,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );

            assertEquals(
                "the manifest must carry the exact real soft-delete count computed from SegmentInfos",
                realSoftDeleteCount,
                manifest.deletedDocCount()
            );
            assertTrue("the manifest's totalDocCount must reflect every indexed document", manifest.totalDocCount() >= 4);

            ManifestSegmentMetrics metrics = ManifestSegmentMetrics.from(manifest);
            assertTrue(
                "estimatedDeleteRatio must now be nonzero, reflecting the real soft delete rather than " + "the old unconditional 0.0",
                metrics.estimatedDeleteRatio > 0.0
            );
            assertEquals(manifest.deleteRatio(), metrics.estimatedDeleteRatio, 0.0);
        }
    }
}
