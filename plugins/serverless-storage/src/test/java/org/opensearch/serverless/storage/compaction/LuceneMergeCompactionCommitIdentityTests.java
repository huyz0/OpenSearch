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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
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
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * The shape of the artifact compaction publishes was never checked against what a reader does with
 * it. Merging into a fresh empty directory made every compacted commit {@code segments_1} -- Lucene
 * generation 1, no matter what generation its source was at -- and restarted the segment-name
 * counter, so successive compactions recycled {@code _0}, {@code _1}, ... straight back over the
 * writer's own segment names in the reader's shared directory. Both tests here fail against that.
 */
public class LuceneMergeCompactionCommitIdentityTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "AbCdEfGhIjKlMnOpQrStUv";
    private static final int SHARD_ID = 0;

    /** Publishes a commit with {@code segmentCount} single-document segments, at the given manifest generation. */
    private CommitManifest publishManySegments(ObjectStoreCommitPublisher publisher, long generation, int segmentCount) throws Exception {
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                for (int i = 0; i < segmentCount; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", "doc-" + i, Field.Store.YES));
                    writer.addDocument(doc);
                    writer.flush();
                    writer.commit();
                }
            }
            SegmentInfos infos = SegmentInfos.readLatestCommit(writerDirectory);
            return publisher.publishCommit(
                writerDirectory,
                infos,
                INDEX_UUID,
                SHARD_ID,
                1,
                generation,
                segmentCount - 1,
                segmentCount - 1,
                null,
                0,
                PruningStats.empty()
            );
        }
    }

    private static Set<String> segmentNamesOf(CommitManifest manifest) {
        Set<String> names = new HashSet<>();
        for (String file : manifest.files().keySet()) {
            if (file.startsWith("_")) {
                names.add(file.substring(0, file.indexOf('.') < 0 ? file.length() : file.indexOf('.')));
            }
        }
        return names;
    }

    /**
     * A compacted commit must be at a strictly higher Lucene generation than the source it merged,
     * and must not reuse any of the source's segment names. Merging into an empty directory
     * guaranteed the exact opposite of both, which is what let a compaction publish a commit the
     * reader silently never selected and then, on the second compaction, overwrite the writer's own
     * {@code _0.*} files in place.
     */
    public void testACompactedCommitOutranksAndDoesNotCollideWithItsSource() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);
        ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(bundleStore);

        CommitManifest source = publishManySegments(commitPublisher, 5, 4);
        long sourceLuceneGeneration = SegmentInfos.generationFromSegmentsFileName(source.segmentsFileName());
        Set<String> sourceSegmentNames = segmentNamesOf(source);
        assertFalse("test setup: the source must actually have segments", sourceSegmentNames.isEmpty());

        ShardHead head = new ShardHead(1, "node-1", Long.MAX_VALUE, source.generation());
        try (
            LuceneMergeCompactionPublisher publisher = new LuceneMergeCompactionPublisher(
                INDEX_UUID,
                SHARD_ID,
                manifestStore,
                materializer,
                commitPublisher,
                CompactionPolicy.withDefaults(),
                createTempDir()
            )
        ) {
            Optional<ShardHead> newHead = publisher.computeNewHead(head);
            assertTrue(newHead.isPresent());
            CommitManifest compacted = manifestStore.readManifest(1, newHead.get().latestManifestGeneration());

            long compactedLuceneGeneration = SegmentInfos.generationFromSegmentsFileName(compacted.segmentsFileName());
            assertTrue(
                "the compacted commit must outrank its source ("
                    + compacted.segmentsFileName()
                    + " vs "
                    + source.segmentsFileName()
                    + "); merging into an empty directory always produced segments_1 instead",
                compactedLuceneGeneration > sourceLuceneGeneration
            );

            Set<String> compactedSegmentNames = segmentNamesOf(compacted);
            assertFalse("the merged commit must have segments of its own", compactedSegmentNames.isEmpty());
            for (String name : compactedSegmentNames) {
                assertFalse(
                    "a compacted segment name must not collide with a source segment name: " + name,
                    sourceSegmentNames.contains(name)
                );
            }
            assertTrue("compaction must actually have reduced the segment count", compactedSegmentNames.size() < sourceSegmentNames.size());
        }
    }

    /**
     * Two compactions with real writer activity in between must keep advancing the Lucene
     * generation and must never reuse a segment name either of them has already used. The verified
     * Lucene behaviour before this change was that compaction #1 produced {@code _3} and compaction
     * #2 produced {@code _0}, both committing {@code segments_1} -- so the second compaction's
     * {@code _0.si} landed straight on top of the writer's original {@code _0.si} in a reader's
     * shared directory.
     *
     * <p>Writer activity between the two rounds is the point, not scaffolding: without it the second
     * compaction has nothing to merge and correctly abandons (see the test below), so it would never
     * exercise the recycling question at all.
     */
    public void testSuccessiveCompactionsNeverRecycleGenerationsOrSegmentNames() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);
        ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(bundleStore);

        CommitManifest source = publishManySegments(commitPublisher, 5, 4);
        Set<String> seenSegmentNames = segmentNamesOf(source);
        long previousLuceneGeneration = SegmentInfos.generationFromSegmentsFileName(source.segmentsFileName());
        ShardHead head = new ShardHead(1, "node-1", Long.MAX_VALUE, source.generation());

        for (int round = 0; round < 2; round++) {
            if (round > 0) {
                // The writer keeps writing on top of whatever compaction last published -- the only
                // state in which a second compaction has real work to do.
                head = publishMoreSegmentsOnTopOf(manifestStore, materializer, commitPublisher, head, 3);
                seenSegmentNames.addAll(segmentNamesOf(manifestStore.readManifest(1, head.latestManifestGeneration())));
                previousLuceneGeneration = SegmentInfos.generationFromSegmentsFileName(
                    manifestStore.readManifest(1, head.latestManifestGeneration()).segmentsFileName()
                );
            }
            try (
                LuceneMergeCompactionPublisher publisher = new LuceneMergeCompactionPublisher(
                    INDEX_UUID,
                    SHARD_ID,
                    manifestStore,
                    materializer,
                    commitPublisher,
                    // Force a real merge every round rather than relying on thresholds.
                    new CompactionPolicy(2, CompactionPolicy.MAX_TARGET_BUNDLE_SIZE_BYTES, 0.0),
                    createTempDir()
                )
            ) {
                head = publisher.computeNewHead(head)
                    .orElseThrow(() -> new AssertionError("a shard with several segments must be worth compacting"));
            }
            CommitManifest compacted = manifestStore.readManifest(1, head.latestManifestGeneration());
            long luceneGeneration = SegmentInfos.generationFromSegmentsFileName(compacted.segmentsFileName());
            assertTrue(
                "round "
                    + round
                    + ": Lucene generation must keep advancing, got "
                    + luceneGeneration
                    + " after "
                    + previousLuceneGeneration,
                luceneGeneration > previousLuceneGeneration
            );
            previousLuceneGeneration = luceneGeneration;

            for (String name : segmentNamesOf(compacted)) {
                assertFalse("round " + round + ": segment name " + name + " was recycled", seenSegmentNames.contains(name));
            }
            seenSegmentNames.addAll(segmentNamesOf(compacted));
        }
    }

    /**
     * Compacting an already-compacted, unchanged shard must abandon, not republish.
     *
     * <p>{@code forceMerge} has nothing to do, so {@code IndexWriter#commit} writes no commit and the
     * "merged" infos is the source's own. Publishing it would re-upload the entire shard into a
     * fresh bundle, orphan the previous one and burn a manifest generation to produce a commit
     * identical to the one already published -- and would do it again on every tick.
     */
    public void testACompactionThatWouldChangeNothingAbandonsInsteadOfRepublishing() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);
        ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(bundleStore);

        CommitManifest source = publishManySegments(commitPublisher, 5, 4);
        ShardHead head = new ShardHead(1, "node-1", Long.MAX_VALUE, source.generation());
        CompactionPolicy alwaysCompact = new CompactionPolicy(2, CompactionPolicy.MAX_TARGET_BUNDLE_SIZE_BYTES, 0.0);

        try (
            LuceneMergeCompactionPublisher first = new LuceneMergeCompactionPublisher(
                INDEX_UUID,
                SHARD_ID,
                manifestStore,
                materializer,
                commitPublisher,
                alwaysCompact,
                createTempDir()
            )
        ) {
            head = first.computeNewHead(head).orElseThrow();
        }
        long generationAfterFirst = head.latestManifestGeneration();
        int bundlesAfterFirst = bundleStore.listBundleNames().size();

        try (
            LuceneMergeCompactionPublisher second = new LuceneMergeCompactionPublisher(
                INDEX_UUID,
                SHARD_ID,
                manifestStore,
                materializer,
                commitPublisher,
                alwaysCompact,
                createTempDir()
            )
        ) {
            assertTrue(
                "a compaction with nothing left to merge must abandon rather than republish an identical commit",
                second.computeNewHead(head).isEmpty()
            );
        }
        assertEquals("an abandoned compaction must not move the head", generationAfterFirst, head.latestManifestGeneration());
        assertEquals("and must not upload a bundle", bundlesAfterFirst, bundleStore.listBundleNames().size());
    }

    /**
     * Materializes {@code head}'s commit, adds {@code extraSegments} more single-document segments to
     * it exactly as a writer would, and publishes the result at the next generation.
     */
    private ShardHead publishMoreSegmentsOnTopOf(
        BlobContainerManifestStore manifestStore,
        ObjectStoreCommitMaterializer materializer,
        ObjectStoreCommitPublisher commitPublisher,
        ShardHead head,
        int extraSegments
    ) throws Exception {
        CommitManifest current = manifestStore.readManifest(head.primaryTerm(), head.latestManifestGeneration());
        try (Directory directory = new ByteBuffersDirectory()) {
            materializer.materialize(current, directory);
            IndexWriterConfig config = new IndexWriterConfig().setSoftDeletesField(org.opensearch.common.lucene.Lucene.SOFT_DELETES_FIELD);
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                for (int i = 0; i < extraSegments; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", "extra-" + head.latestManifestGeneration() + "-" + i, Field.Store.YES));
                    writer.addDocument(doc);
                    writer.flush();
                    writer.commit();
                }
            }
            SegmentInfos infos = SegmentInfos.readLatestCommit(directory);
            long newGeneration = head.latestManifestGeneration() + 1;
            commitPublisher.publishCommit(
                directory,
                infos,
                INDEX_UUID,
                SHARD_ID,
                head.primaryTerm(),
                newGeneration,
                current.maxSeqNo(),
                current.localCheckpoint(),
                current.walPosition(),
                current.mappingVersion(),
                current.pruningStats()
            );
            return head.withPublishedGeneration(newGeneration);
        }
    }
}
