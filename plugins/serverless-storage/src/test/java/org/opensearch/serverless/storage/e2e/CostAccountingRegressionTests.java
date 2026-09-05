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
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.storage.compaction.CompactionPolicy;
import org.opensearch.serverless.storage.compaction.CompactionRebaseExecutor;
import org.opensearch.serverless.storage.compaction.LuceneMergeCompactionPublisher;
import org.opensearch.serverless.storage.compaction.RebaseResult;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.security.RegisterDelegatingBlobContainer;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
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
 *
 * <p>Also gates the compaction publish path (rfc-serverless-opensearch.md &sect;17's own
 * still-open note that GC sweep, compaction publish, and PITR reconciliation had no equivalent
 * gate): {@link LuceneMergeCompactionPublisher} reuses {@code ObjectStoreCommitPublisher#publishCommit}
 * internally for its own republish step, so its PUT cost should be structurally identical to a
 * plain writer publish regardless of how many source segments it merges -- this is what {@link
 * #testCompactionPublishingStaysWithinItsExpectedPutBudget} proves directly, rather than assumed
 * from reading the code alone.
 */
public class CostAccountingRegressionTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "cost-accounting";
    private static final int SHARD_ID = 0;

    /**
     * Counts every write-shaped call this plugin's own code paths actually use, ignoring
     * reads/lists this test doesn't exercise. Extends {@link RegisterDelegatingBlobContainer}, not
     * a bare {@code FilterBlobContainer}, so register-based operations (the compaction path's own
     * head CAS) actually delegate through instead of throwing {@link UnsupportedOperationException}
     * -- see that base class's own javadoc for the exact regression this avoids.
     */
    private static final class RequestCountingBlobContainer extends RegisterDelegatingBlobContainer {

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

    /** Commits N separate single-document segments (no merge), so a real multi-segment index results. */
    private static SegmentInfos commitSeparateSegments(Directory directory, int count) throws Exception {
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

    public void testCompactionPublishingStaysWithinItsExpectedPutBudget() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer uncountedContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());

        // Setup: publish a real multi-segment source commit, uncounted -- this represents state
        // that already existed before the compaction workload this test actually measures.
        ObjectStoreCommitPublisher setupPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(uncountedContainer),
            new BlobContainerManifestStore(uncountedContainer)
        );
        ShardStateStore setupShardStateStore = new BlobContainerShardStateStore(uncountedContainer);
        SegmentInfos sourceInfos;
        try (Directory sourceDirectory = new ByteBuffersDirectory()) {
            sourceInfos = commitSeparateSegments(sourceDirectory, 5);
            assertTrue("test setup should produce multiple segments", sourceInfos.size() > 1);
            CommitManifest sourceManifest = setupPublisher.publishCommit(
                sourceDirectory,
                sourceInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                sourceInfos.getGeneration(),
                4,
                4,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
            ShardHead initialHead = new ShardHead(1, "node-1", Long.MAX_VALUE, sourceManifest.generation());
            assertEquals(CasResult.SUCCESS, setupShardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), initialHead));
        }

        // The measured workload: one compaction publish through the real orchestration path
        // (LuceneMergeCompactionPublisher + CompactionRebaseExecutor), wrapping the same underlying
        // store in a fresh counting container so setup's own writes above aren't included.
        RequestCountingBlobContainer countingContainer = new RequestCountingBlobContainer(uncountedContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(countingContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(countingContainer),
            manifestStore
        );
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(countingContainer);
        LuceneMergeCompactionPublisher compactionPublisher = new LuceneMergeCompactionPublisher(
            INDEX_UUID,
            SHARD_ID,
            manifestStore,
            new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(countingContainer)),
            commitPublisher,
            CompactionPolicy.withDefaults(),
            createTempDir()
        );
        CompactionRebaseExecutor rebaseExecutor = new CompactionRebaseExecutor(shardStateStore, 10);

        RebaseResult result = rebaseExecutor.publish(INDEX_UUID, SHARD_ID, compactionPublisher);
        assertEquals(RebaseResult.Outcome.PUBLISHED, result.outcome());

        // Same "one bundle, one manifest" budget as a plain writer publish -- compaction's own
        // orchestration (materialize, merge, rebase-CAS the head) must add no extra PUT-shaped
        // requests of its own; the head CAS itself goes through compareAndSwapRegister, a different
        // request category this test doesn't fold into the PUT budget.
        assertEquals(
            "compacting away a 5-segment source commit must cost exactly the same 2 PUTs as a plain "
                + "publish (one bundle, one manifest) -- got "
                + countingContainer.putCount()
                + ", the compaction path's own orchestration likely added extra object-store requests",
            2L,
            countingContainer.putCount()
        );
    }
}
