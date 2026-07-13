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
import org.opensearch.common.lucene.Lucene;
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
 * the full materialize-merge-publish sequence against whatever {@code currentHead} it's handed.
 * This is deliberate, not merely unoptimized: {@link CompactionRebaseExecutor} re-reads the live
 * head fresh on every retry, and a CAS only ever fails because the head's content genuinely
 * changed underneath it (a writer's publish, or another compactor's own merge landing first) --
 * so the source manifest being merged is, in the case that actually matters, different on every
 * retry, not merely renumbered. Caching the previous attempt's merged bundle and reusing it under
 * a new generation would silently republish stale content and discard whatever the other party
 * just wrote -- a correctness bug, not just a missed optimization. Redoing the full sequence each
 * time is what keeps every attempt an independent, correct merge-and-publish against the actual
 * current state.
 */
public final class LuceneMergeCompactionPublisher implements CompactionPublisher {

    private final String indexUuid;
    private final int shardId;
    private final BlobContainerManifestStore manifestStore;
    private final ObjectStoreCommitMaterializer materializer;
    private final ObjectStoreCommitPublisher commitPublisher;
    private final CompactionPolicy policy;

    /**
     * Creates a publisher that materializes, merges, and republishes the given shard's current commit.
     *
     * @param indexUuid       UUID of the index the shard belongs to
     * @param shardId         id of the shard within the index
     * @param manifestStore   store used to read the source commit manifest
     * @param materializer    materializes a commit manifest's segments into a real Lucene directory
     * @param commitPublisher publishes the merged result as a new commit manifest
     * @param policy          used to compute the target segment count for the merge
     */
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

                // The soft-deletes field must be configured here (matching the constant every real
                // OpenSearch index actually indexes with, per Lucene.SOFT_DELETES_FIELD) even though
                // this writer never itself soft-deletes anything -- addIndexes below throws
                // IllegalArgumentException ("this index has [...] as soft-deletes already but
                // soft-deletes field is not configured in IWC") the moment the source segments it's
                // copying in carry that field and this config doesn't acknowledge it. A real,
                // previously latent bug: no existing test exercised compaction against any
                // soft-deleted source content, so this had never been caught before.
                IndexWriterConfig mergeConfig = new IndexWriterConfig().setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
                try (IndexWriter writer = new IndexWriter(mergedDirectory, mergeConfig)) {
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
