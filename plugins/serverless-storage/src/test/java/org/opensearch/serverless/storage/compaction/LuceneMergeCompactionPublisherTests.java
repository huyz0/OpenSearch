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
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

public class LuceneMergeCompactionPublisherTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;

    private BlobContainer blobContainer;
    private ObjectStoreCommitPublisher commitPublisher;
    private BlobContainerManifestStore manifestStore;
    private ShardStateStore shardStateStore;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);
        manifestStore = new BlobContainerManifestStore(blobContainer);
        commitPublisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);
        shardStateStore = new BlobContainerShardStateStore(blobContainer);
    }

    /** Commits N separate single-document segments (no merge), so a real multi-segment index results. */
    private SegmentInfos commitSeparateSegments(Directory directory, int count) throws Exception {
        SegmentInfos infos = null;
        for (int i = 0; i < count; i++) {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE);
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

    public void testCompactionMergesMultipleSegmentsIntoOnePreservingAllDocuments() throws Exception {
        try (Directory sourceDirectory = new ByteBuffersDirectory()) {
            SegmentInfos sourceInfos = commitSeparateSegments(sourceDirectory, 5);
            assertTrue("test setup should produce multiple segments", sourceInfos.size() > 1);

            CommitManifest sourceManifest = commitPublisher.publishCommit(
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
            assertEquals(CasResult.SUCCESS, shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), initialHead));

            LuceneMergeCompactionPublisher compactionPublisher = new LuceneMergeCompactionPublisher(
                INDEX_UUID,
                SHARD_ID,
                manifestStore,
                new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer)),
                commitPublisher,
                CompactionPolicy.withDefaults()
            );
            CompactionRebaseExecutor rebaseExecutor = new CompactionRebaseExecutor(shardStateStore, 10);

            RebaseResult result = rebaseExecutor.publish(INDEX_UUID, SHARD_ID, compactionPublisher);
            assertEquals(RebaseResult.Outcome.PUBLISHED, result.outcome());

            ShardHead compactedHead = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            assertTrue(compactedHead.latestManifestGeneration() > initialHead.latestManifestGeneration());

            CommitManifest compactedManifest = manifestStore.readManifest(
                compactedHead.primaryTerm(),
                compactedHead.latestManifestGeneration()
            );
            // Metadata not affected by a pure segment merge must carry over unchanged.
            assertEquals(sourceManifest.maxSeqNo(), compactedManifest.maxSeqNo());
            assertEquals(sourceManifest.localCheckpoint(), compactedManifest.localCheckpoint());

            try (Directory materializedDirectory = new ByteBuffersDirectory()) {
                new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer)).materialize(
                    compactedManifest,
                    materializedDirectory
                );
                try (DirectoryReader reader = DirectoryReader.open(materializedDirectory)) {
                    assertEquals("compaction must merge into a single segment", 1, reader.leaves().size());
                    assertEquals("compaction must not lose or duplicate documents", 5, reader.numDocs());

                    IndexSearcher searcher = new IndexSearcher(reader);
                    for (int i = 0; i < 5; i++) {
                        TopDocs hits = searcher.search(new TermQuery(new Term("id", "doc-" + i)), 10);
                        assertEquals("doc-" + i + " must survive compaction", 1, hits.totalHits.value());
                    }
                }
            }
        }
    }

    public void testCompactionShapesToMultipleSegmentsWhenSourceExceedsOneTargetBundle() throws Exception {
        try (Directory sourceDirectory = new ByteBuffersDirectory()) {
            SegmentInfos sourceInfos = commitSeparateSegments(sourceDirectory, 6);
            assertTrue("test setup should produce multiple segments", sourceInfos.size() > 1);

            CommitManifest sourceManifest = commitPublisher.publishCommit(
                sourceDirectory,
                sourceInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                sourceInfos.getGeneration(),
                5,
                5,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
            ShardHead initialHead = new ShardHead(1, "node-1", Long.MAX_VALUE, sourceManifest.generation());
            assertEquals(CasResult.SUCCESS, shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), initialHead));

            long totalBytes = sourceManifest.files().values().stream().mapToLong(f -> f.length()).sum();
            assertTrue("test setup should have produced a non-trivial total size", totalBytes > 0);
            // A tiny max-target-bundle-size relative to the real source size forces more than one
            // target segment -- this is what proves the policy's target actually reaches the merge,
            // not just that shouldCompact()'s independent threshold logic works.
            CompactionPolicy tightPolicy = new CompactionPolicy(2, Math.max(1, totalBytes / 3), 1.0);
            int expectedTargetSegments = tightPolicy.targetSegmentCount(totalBytes);
            assertTrue("test setup should force more than one target segment", expectedTargetSegments > 1);

            LuceneMergeCompactionPublisher compactionPublisher = new LuceneMergeCompactionPublisher(
                INDEX_UUID,
                SHARD_ID,
                manifestStore,
                new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer)),
                commitPublisher,
                tightPolicy
            );
            CompactionRebaseExecutor rebaseExecutor = new CompactionRebaseExecutor(shardStateStore, 10);

            RebaseResult result = rebaseExecutor.publish(INDEX_UUID, SHARD_ID, compactionPublisher);
            assertEquals(RebaseResult.Outcome.PUBLISHED, result.outcome());

            ShardHead compactedHead = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            CommitManifest compactedManifest = manifestStore.readManifest(
                compactedHead.primaryTerm(),
                compactedHead.latestManifestGeneration()
            );

            try (Directory materializedDirectory = new ByteBuffersDirectory()) {
                new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer)).materialize(
                    compactedManifest,
                    materializedDirectory
                );
                try (DirectoryReader reader = DirectoryReader.open(materializedDirectory)) {
                    assertTrue(
                        "compaction under a tight target-bundle-size should not collapse to a single segment,"
                            + " got "
                            + reader.leaves().size()
                            + " leaves",
                        reader.leaves().size() > 1
                    );
                    assertTrue(
                        "compaction must never exceed forceMerge's requested segment count",
                        reader.leaves().size() <= expectedTargetSegments
                    );
                    assertEquals("compaction must not lose or duplicate documents", 6, reader.numDocs());
                }
            }
        }
    }

    public void testCompactionRebasesAgainstAConcurrentWriterPublication() throws Exception {
        try (Directory sourceDirectory = new ByteBuffersDirectory()) {
            SegmentInfos sourceInfos = commitSeparateSegments(sourceDirectory, 3);

            CommitManifest sourceManifest = commitPublisher.publishCommit(
                sourceDirectory,
                sourceInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                sourceInfos.getGeneration(),
                2,
                2,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
            ShardHead initialHead = new ShardHead(1, "node-1", Long.MAX_VALUE, sourceManifest.generation());
            assertEquals(CasResult.SUCCESS, shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), initialHead));

            // A concurrent writer publishes a newer generation before the compactor's CAS lands.
            CommitManifest racingManifest = commitPublisher.publishCommit(
                sourceDirectory,
                sourceInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                sourceManifest.generation() + 100,
                3,
                3,
                new WalPosition("epoch-0", 1),
                0,
                PruningStats.empty()
            );
            var currentVersioned = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow();
            ShardHead racingHead = currentVersioned.head().withPublishedGeneration(racingManifest.generation());
            assertEquals(
                CasResult.SUCCESS,
                shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.of(currentVersioned.version()), racingHead)
            );

            LuceneMergeCompactionPublisher compactionPublisher = new LuceneMergeCompactionPublisher(
                INDEX_UUID,
                SHARD_ID,
                manifestStore,
                new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer)),
                commitPublisher,
                CompactionPolicy.withDefaults()
            );
            CompactionRebaseExecutor rebaseExecutor = new CompactionRebaseExecutor(shardStateStore, 10);

            // The compactor was handed initialHead's generation as its starting point (simulating
            // a candidate selected before the race), but the executor always re-reads the actual
            // current head first, so the compactor rebases against racingHead's generation instead.
            RebaseResult result = rebaseExecutor.publish(INDEX_UUID, SHARD_ID, compactionPublisher);
            assertEquals(RebaseResult.Outcome.PUBLISHED, result.outcome());

            ShardHead finalHead = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            assertTrue(
                "compaction must rebase onto the writer's newer generation, not overwrite it",
                finalHead.latestManifestGeneration() > racingManifest.generation()
            );
        }
    }

    /**
     * Reproduces the exact "permanently stuck" scenario this class's own javadoc describes: a
     * previous attempt's bundle upload succeeded under the deterministic target name, but that
     * attempt never got as far as writing a manifest for it (simulated here by writing a bundle
     * directly, bypassing {@code commitPublisher} entirely, under the exact name {@code
     * ObjectStoreCommitPublisher} would target for {@code initialHead}'s next generation). On a
     * quiescent shard (no writer commit ever moves the head past this), every compaction attempt
     * would recompute the same target name and collide forever before this class's own
     * bundle-name-collision retry existed. With it, a single {@link CompactionRebaseExecutor#publish}
     * call now succeeds by retrying under a fresh suffix, no manual intervention or delete
     * permission needed.
     */
    public void testCompactionUnsticksAPermanentlyCollidingTargetGeneration() throws Exception {
        try (Directory sourceDirectory = new ByteBuffersDirectory()) {
            SegmentInfos sourceInfos = commitSeparateSegments(sourceDirectory, 3);

            CommitManifest sourceManifest = commitPublisher.publishCommit(
                sourceDirectory,
                sourceInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                sourceInfos.getGeneration(),
                2,
                2,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
            ShardHead initialHead = new ShardHead(1, "node-1", Long.MAX_VALUE, sourceManifest.generation());
            assertEquals(CasResult.SUCCESS, shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), initialHead));

            // Simulates a previous compaction attempt whose bundle upload succeeded but whose
            // manifest write never landed: a real bundle, under the exact deterministic name this
            // shard's next compaction will target, with content that could never match a real
            // merge's own packed bytes (a single tiny made-up file) -- guaranteeing writeBundle's
            // collision guard fires on the very first real attempt below, exactly like a genuine
            // stuck leftover would.
            long targetGeneration = initialHead.latestManifestGeneration() + 1;
            String stuckBundleName = BlobContainerBundleStore.NAME_PREFIX + INDEX_UUID + "-" + SHARD_ID + "-" + 1 + "-" + targetGeneration;
            new BlobContainerBundleStore(blobContainer).writeBundle(
                stuckBundleName,
                java.util.List.of(
                    new org.opensearch.serverless.storage.format.BundleFileContent("not-a-real-segment-file", new byte[] { 1, 2, 3 })
                )
            );
            assertFalse(
                "test setup must not have accidentally also published a manifest for the target generation",
                manifestStore.manifestExists(1, targetGeneration)
            );

            LuceneMergeCompactionPublisher compactionPublisher = new LuceneMergeCompactionPublisher(
                INDEX_UUID,
                SHARD_ID,
                manifestStore,
                new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer)),
                commitPublisher,
                CompactionPolicy.withDefaults()
            );
            CompactionRebaseExecutor rebaseExecutor = new CompactionRebaseExecutor(shardStateStore, 10);

            RebaseResult result = rebaseExecutor.publish(INDEX_UUID, SHARD_ID, compactionPublisher);
            assertEquals(
                "compaction must unstick itself past the colliding leftover and genuinely publish, "
                    + "not merely abort via the collision guard",
                RebaseResult.Outcome.PUBLISHED,
                result.outcome()
            );

            ShardHead compactedHead = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            CommitManifest compactedManifest = manifestStore.readManifest(
                compactedHead.primaryTerm(),
                compactedHead.latestManifestGeneration()
            );
            assertNotEquals(
                "the published bundle must have landed under an alternate (suffixed) name, not the "
                    + "permanently-stuck deterministic one",
                stuckBundleName,
                compactedManifest.files().values().iterator().next().bundleName()
            );

            try (Directory materializedDirectory = new ByteBuffersDirectory()) {
                new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer)).materialize(
                    compactedManifest,
                    materializedDirectory
                );
                try (DirectoryReader reader = DirectoryReader.open(materializedDirectory)) {
                    assertEquals("compaction must still merge into a single segment", 1, reader.leaves().size());
                    assertEquals("compaction must not lose or duplicate documents", 3, reader.numDocs());
                }
            }
        }
    }
}
