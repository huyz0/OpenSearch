/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

/**
 * The real Lucene merge {@link CompactionPublisher} the compaction service was missing
 * (rfc-serverless-opensearch.md &sect;7.4): materializes the shard's current published commit,
 * folds its segments together via a real {@code IndexWriter} merge (not a re-index -- no document
 * is re-parsed, only segment files are combined), and republishes the merged result as a new
 * commit manifest.
 *
 * <p>The merge target is size-tiered, not always "one segment regardless of size"
 * (rfc-serverless-opensearch.md &sect;16 Phase 4.5's "still open" item): {@link
 * CompactionPolicy#targetSegmentCount} turns the source manifest's total byte size into a target
 * segment count such that each resulting segment stays under the policy's {@code
 * maxTargetBundleSizeBytes}, and {@link IndexWriter#forceMerge(int)} is asked to merge down to
 * that many segments rather than unconditionally down to one. A small shard still ends up as one
 * segment (its target segment count is 1 either way); a shard whose total size already exceeds
 * one target bundle now stays multiple segments after compaction instead of being forced into one
 * oversized segment.
 *
 * <p>Every {@link #computeNewHead} call (including rebase retries after losing a CAS race) redoes
 * the full materialize-merge-publish sequence against whatever {@code currentHead} it's handed;
 * {@link CompactionRebaseExecutor}'s own documentation notes that a cheaper implementation could
 * cache the merged Lucene files and bundle across retries and only rebuild the small manifest
 * object each time (the merge result is generation-independent). This implementation doesn't do
 * that yet -- correct under retries (each attempt is a fresh, independent merge-and-publish), just
 * not maximally cheap under contention, which only matters for a shard hot enough to be racing
 * compaction attempts in the first place.
 */
public final class LuceneMergeCompactionPublisher implements CompactionPublisher {

    private final String indexUuid;
    private final int shardId;
    private final BlobContainerManifestStore manifestStore;
    private final ObjectStoreCommitMaterializer materializer;
    private final ObjectStoreCommitPublisher commitPublisher;
    private final CompactionPolicy policy;

    public LuceneMergeCompactionPublisher(
        String indexUuid,
        int shardId,
        BlobContainerManifestStore manifestStore,
        ObjectStoreCommitMaterializer materializer,
        ObjectStoreCommitPublisher commitPublisher,
        CompactionPolicy policy
    ) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.manifestStore = manifestStore;
        this.materializer = materializer;
        this.commitPublisher = commitPublisher;
        this.policy = policy;
    }

    @Override
    public Optional<ShardHead> computeNewHead(ShardHead currentHead) {
        try {
            CommitManifest sourceManifest = manifestStore.readManifest(currentHead.primaryTerm(), currentHead.latestManifestGeneration());
            long totalBytes = sourceManifest.files().values().stream().mapToLong(FileReference::length).sum();
            int targetSegmentCount = policy.targetSegmentCount(totalBytes);

            try (Directory sourceDirectory = new ByteBuffersDirectory(); Directory mergedDirectory = new ByteBuffersDirectory()) {
                materializer.materialize(sourceManifest, sourceDirectory);

                try (IndexWriter writer = new IndexWriter(mergedDirectory, new IndexWriterConfig())) {
                    writer.addIndexes(sourceDirectory);
                    writer.forceMerge(targetSegmentCount);
                    writer.commit();
                }
                SegmentInfos mergedInfos = SegmentInfos.readLatestCommit(mergedDirectory);

                long newGeneration = currentHead.latestManifestGeneration() + 1;
                CommitManifest newManifest = commitPublisher.publishCommit(
                    mergedDirectory,
                    mergedInfos,
                    indexUuid,
                    shardId,
                    currentHead.primaryTerm(),
                    newGeneration,
                    sourceManifest.maxSeqNo(),
                    sourceManifest.localCheckpoint(),
                    sourceManifest.walPosition(),
                    sourceManifest.mappingVersion(),
                    sourceManifest.pruningStats()
                );

                return Optional.of(currentHead.withPublishedGeneration(newManifest.generation()));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("compaction merge failed for " + indexUuid + "/" + shardId, e);
        }
    }
}
