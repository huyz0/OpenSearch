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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

public class ObjectStoreCommitPublisherTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;

    private BlobContainer blobContainer;
    private ObjectStoreCommitPublisher publisher;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        publisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );
    }

    private static SegmentInfos commitTwoDocuments(Directory directory) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig();
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            Document doc1 = new Document();
            doc1.add(new StringField("id", "1", Field.Store.YES));
            writer.addDocument(doc1);
            Document doc2 = new Document();
            doc2.add(new StringField("id", "2", Field.Store.YES));
            writer.addDocument(doc2);
            writer.commit();
        }
        return SegmentInfos.readLatestCommit(directory);
    }

    public void testPublishedManifestReferencesEveryFileInTheLuceneCommit() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos segmentInfos = commitTwoDocuments(directory);

            CommitManifest manifest = publisher.publishCommit(
                directory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                0,
                1,
                1,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );

            for (String fileName : segmentInfos.files(true)) {
                assertTrue("manifest should reference " + fileName, manifest.files().containsKey(fileName));
            }
            assertEquals(segmentInfos.getSegmentsFileName(), manifest.segmentsFileName());
        }
    }

    public void testBundledFileBytesRoundTripExactlyAgainstTheLocalLuceneFiles() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos segmentInfos = commitTwoDocuments(directory);

            CommitManifest manifest = publisher.publishCommit(
                directory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                0,
                1,
                1,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );

            BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);
            for (Map.Entry<String, FileReference> entry : manifest.files().entrySet()) {
                String fileName = entry.getKey();
                FileReference ref = entry.getValue();
                byte[] localBytes = readFile(directory, fileName);
                byte[] bundledBytes = bundleStore.readFile(
                    ref.bundleName(),
                    new org.opensearch.serverless.storage.format.BundleFileEntry(fileName, ref.offset(), ref.length(), ref.checksum())
                );
                assertArrayEquals("bundled bytes for " + fileName + " must match the local Lucene file", localBytes, bundledBytes);
            }
        }
    }

    public void testManifestPersistsAndCanBeReadBackByGeneration() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos segmentInfos = commitTwoDocuments(directory);

            CommitManifest written = publisher.publishCommit(
                directory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                3,
                7,
                1,
                1,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );

            CommitManifest reread = new BlobContainerManifestStore(blobContainer).readManifest(3, 7);
            assertEquals(written.manifestName(), reread.manifestName());
            assertEquals(written.files().keySet(), reread.files().keySet());
        }
    }

    public void testPublishCommitWithQuiescentFlagMarksTheManifest() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos segmentInfos = commitTwoDocuments(directory);

            CommitManifest written = publisher.publishCommit(
                directory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                3,
                7,
                1,
                1,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty(),
                true
            );

            assertTrue(written.quiescent());
            CommitManifest reread = new BlobContainerManifestStore(blobContainer).readManifest(3, 7);
            assertTrue("the quiescent flag must survive a real write/read round trip", reread.quiescent());
        }
    }

    public void testPublishCommitWithoutQuiescentArgumentDefaultsToFalse() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos segmentInfos = commitTwoDocuments(directory);

            CommitManifest written = publisher.publishCommit(
                directory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                3,
                7,
                1,
                1,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );

            assertFalse(written.quiescent());
        }
    }

    public void testRetriedPublishForTheSameGenerationIsANoOpAndReturnsTheExistingManifest() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos segmentInfos = commitTwoDocuments(directory);

            CommitManifest first = publisher.publishCommit(
                directory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                0,
                1,
                1,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );

            String bundleName = first.files().values().iterator().next().bundleName();
            byte[] bundleBytesAfterFirstPublish;
            try (java.io.InputStream in = blobContainer.readBlob(bundleName)) {
                bundleBytesAfterFirstPublish = in.readAllBytes();
            }

            // A retry for the identical (primaryTerm, generation) must not re-upload the bundle:
            // it should short-circuit to the already-published manifest.
            CommitManifest retried = publisher.publishCommit(
                directory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                0,
                999, // deliberately different metadata to prove this is ignored, not re-published
                999,
                new WalPosition("epoch-1", 42),
                999,
                PruningStats.empty()
            );

            assertEquals(first.manifestName(), retried.manifestName());
            assertEquals(first.maxSeqNo(), retried.maxSeqNo());
            assertEquals(first.walPosition(), retried.walPosition());

            byte[] bundleBytesAfterRetry;
            try (java.io.InputStream in = blobContainer.readBlob(bundleName)) {
                bundleBytesAfterRetry = in.readAllBytes();
            }
            assertArrayEquals(bundleBytesAfterFirstPublish, bundleBytesAfterRetry);
        }
    }

    public void testPublishAtAnAlreadyOccupiedGenerationWithDifferentContentThrowsInsteadOfReturningTheForeignManifest() throws Exception {
        try (Directory foreignDirectory = new ByteBuffersDirectory(); Directory ownDirectory = new ByteBuffersDirectory()) {
            // Simulates a foreign write already occupying (primaryTerm=1, generation=0) -- e.g. a
            // lost ShardCloner/ShardShrinker attempt whose own head CAS never landed -- followed by
            // this shard's own, unrelated genuine commit computing the exact same (primaryTerm,
            // generation), the collision ShardCloner/ShardShrinker's own javadoc warns about.
            SegmentInfos foreignSegmentInfos = commitTwoDocuments(foreignDirectory);
            publisher.publishCommit(
                foreignDirectory,
                foreignSegmentInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                0,
                1,
                1,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );

            IndexWriterConfig config = new IndexWriterConfig();
            try (IndexWriter writer = new IndexWriter(ownDirectory, config)) {
                Document doc = new Document();
                doc.add(new StringField("id", "own-unique-doc", Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }
            SegmentInfos ownSegmentInfos = SegmentInfos.readLatestCommit(ownDirectory);

            IOException thrown = expectThrows(
                IOException.class,
                () -> publisher.publishCommit(
                    ownDirectory,
                    ownSegmentInfos,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    0,
                    2,
                    2,
                    new WalPosition("epoch-0", 1),
                    0,
                    PruningStats.empty()
                )
            );
            assertTrue(thrown.getMessage().contains("different content"));
        }
    }

    private static byte[] readFile(Directory directory, String fileName) throws IOException {
        try (IndexInput input = directory.openInput(fileName, IOContext.READONCE)) {
            byte[] bytes = new byte[(int) input.length()];
            input.readBytes(bytes, 0, bytes.length);
            return bytes;
        }
    }
}
