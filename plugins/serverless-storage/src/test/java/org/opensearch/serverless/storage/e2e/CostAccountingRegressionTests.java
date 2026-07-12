/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.e2e;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

/**
 * rfc-serverless-opensearch.md &sect;17's "Cost accounting" testing-strategy bullet: "per-workload
 * object-store request counts as a regression metric -- a change that doubles PUT count is a
 * failed build, same as a latency regression." {@link ObjectStoreCommitPublisher#publishCommit}
 * publishing a single commit is the smallest real, representative workload this plugin has: one
 * segment bundle write and one manifest write, both via {@link
 * BlobContainer#writeBlobAtomic}, and nothing else -- see that class's own callers ({@link
 * BlobContainerBundleStore}, {@link BlobContainerManifestStore}), both confirmed by reading them
 * to issue exactly one write each per commit, not once per file.
 */
public class CostAccountingRegressionTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "cost-accounting";
    private static final int SHARD_ID = 0;

    /** Counts every write-shaped call this plugin's own code paths actually use, ignoring reads/lists this test doesn't exercise. */
    private static final class RequestCountingBlobContainer extends FilterBlobContainer {

        private final AtomicLong putCount = new AtomicLong();

        RequestCountingBlobContainer(BlobContainer delegate) {
            super(delegate);
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new RequestCountingBlobContainer(child);
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            putCount.incrementAndGet();
            super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            putCount.incrementAndGet();
            super.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        long putCount() {
            return putCount.get();
        }
    }

    public void testPublishingCommitsStaysWithinItsExpectedPutBudget() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        RequestCountingBlobContainer blobContainer = new RequestCountingBlobContainer(
            new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path())
        );
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        int commitCount = 5;
        try (
            Directory writerDirectory = new ByteBuffersDirectory();
            IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())
        ) {
            for (int i = 0; i < commitCount; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", Integer.toString(i), Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
                SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
                publisher.publishCommit(
                    writerDirectory,
                    segmentInfos,
                    INDEX_UUID,
                    SHARD_ID,
                    1L,
                    segmentInfos.getGeneration(),
                    i,
                    i,
                    new WalPosition("epoch-0", 0),
                    0,
                    PruningStats.empty()
                );
            }
        }

        // Exactly 2 PUTs per commit (one bundle, one manifest) -- not per-file, not per-commit-plus-something.
        // A future change that starts writing bundle files individually, or adds an extra manifest
        // write, or otherwise inflates this ratio must fail this assertion, exactly the "doubling
        // PUT count is a failed build" gate section 17 asks for.
        assertEquals(
            "publishing "
                + commitCount
                + " commits must cost exactly 2 PUTs each (one bundle, one manifest) -- got "
                + blobContainer.putCount()
                + ", a change likely inflated per-commit object-store request cost",
            commitCount * 2L,
            blobContainer.putCount()
        );
    }

    public void testPublishingASingleCommitWithManyDocumentsStillCostsExactlyTwoPuts() throws Exception {
        // A single commit's PUT cost must not scale with document count within that commit -- all
        // segment files for one commit are packaged into one bundle blob, not one blob per file or
        // per document. Proven with enough documents to guarantee multiple real segment files
        // exist before the single commit() call bundles them together.
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        RequestCountingBlobContainer blobContainer = new RequestCountingBlobContainer(
            new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path())
        );
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        try (
            Directory writerDirectory = new ByteBuffersDirectory();
            IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())
        ) {
            for (int i = 0; i < 50; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", Integer.toString(i), Field.Store.YES));
                writer.addDocument(doc);
                if (i % 7 == 0) {
                    // Force multiple real segments to exist before the one commit() below bundles them.
                    writer.flush();
                }
            }
            writer.commit();
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            java.util.Collection<String> files = segmentInfos.files(true);
            assertTrue("test setup must produce more than a couple of real segment files to be meaningful", files.size() > 2);

            publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                1L,
                segmentInfos.getGeneration(),
                0,
                0,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
        }

        assertEquals(
            "one commit must cost exactly 2 PUTs regardless of how many underlying segment files it packages",
            2L,
            blobContainer.putCount()
        );
    }
}
