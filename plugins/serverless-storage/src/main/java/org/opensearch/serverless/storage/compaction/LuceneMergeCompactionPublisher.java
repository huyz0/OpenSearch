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
import org.opensearch.common.Randomness;
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
 *
 * <p><b>A losing rebase attempt's already-uploaded bundle is genuinely orphaned, and this class
 * cannot clean it up itself.</b> {@code publishWithBundleNameCollisionRetry} below uploads the
 * merged bundle <em>before</em> {@link CompactionRebaseExecutor} attempts the head CAS that would
 * make it live -- the same bundle-before-manifest-before-head ordering used everywhere else in
 * this codebase, so a crash never leaves a published manifest pointing at bytes that aren't
 * durably there. If that CAS then loses the race, the bundle this attempt just uploaded is
 * unreferenced by any manifest, but this class's container is deliberately delete-denied
 * (rfc-serverless-opensearch.md &sect;15, see {@code RestrictingBlobContainer}'s own javadoc:
 * "the compaction service needs GET+PUT but no DELETE (deletion stays with GC)") -- reclaiming it
 * is {@code GcSchedulerTask}'s own per-shard bundle sweep's job, not this class's, exactly the same
 * way the bundle-name-collision "stuck leftover" case just above relies on that same sweep rather
 * than deleting the leftover directly. Both are bounded (at most {@code maxAttempts} orphaned
 * bundles per rebase-and-retry cycle, not unbounded), but only actually reclaimed if GC is enabled
 * on this shard -- see {@code GcSchedulerTask}'s own class javadoc for that setting.
 *
 * <p><b>Unsticks a permanently-colliding target generation on its own, without needing delete
 * permission or a manual operator action.</b> {@code ObjectStoreCommitPublisher#publishCommit}'s
 * bundle name is deterministic per {@code (indexUuid, shardId, primaryTerm, generation)}; on a
 * quiescent shard whose head never advances (no writer commit ever moves it past the collision),
 * every future compaction attempt recomputes the exact same target name. If an earlier attempt's
 * bundle upload succeeded but a later step (the manifest write, or the shard-head CAS) faulted
 * before that attempt could complete, {@code writeBundle}'s own collision guard correctly refuses
 * to trust or overwrite the mismatched leftover -- but on its own, that just converts the failure
 * from silent corruption to a loud, permanently-repeating one. This class now catches exactly that
 * collision and retries the upload (not the merge -- the already-merged bytes are reused as-is,
 * since a bundle-name retry has nothing to do with the shard head having changed) under a fresh
 * random suffix via {@code ObjectStoreCommitPublisher}'s {@code bundleNameSuffix} overload, up to
 * {@link #MAX_BUNDLE_NAME_COLLISION_RETRIES} times. This doesn't need compaction's own delete
 * permission (deliberately withheld, see rfc-serverless-opensearch.md &sect;15) because it never
 * touches the stuck leftover at all -- it just stops targeting it.
 */
public final class LuceneMergeCompactionPublisher implements CompactionPublisher {

    /**
     * Bounds the bundle-name-collision retry loop in {@link #computeNewHead}. A fresh random
     * 64-bit suffix colliding with a previous attempt's own random suffix is astronomically
     * unlikely; this bound exists only so a (hypothetical, never-observed) run of bad luck fails
     * loudly rather than looping forever.
     */
    private static final int MAX_BUNDLE_NAME_COLLISION_RETRIES = 5;

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

                CommitManifest newManifest = publishWithBundleNameCollisionRetry(
                    mergedDirectory,
                    mergedInfos,
                    currentHead.primaryTerm(),
                    newGeneration,
                    sourceManifest
                );

                return Optional.of(currentHead.withPublishedGeneration(newManifest.generation()));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("compaction merge failed for " + indexUuid + "/" + shardId, e);
        }
    }

    /**
     * Publishes the already-merged {@code mergedDirectory}/{@code mergedInfos} under the normal
     * deterministic bundle name first; if and only if that fails with {@code writeBundle}'s own
     * documented collision message (a previous attempt's mismatched leftover permanently occupying
     * that name), retries the exact same already-merged bytes under a fresh random suffix instead
     * of redoing the merge -- a bundle-name collision has nothing to do with the shard head having
     * changed, so there's nothing to re-materialize or re-merge. Any other {@link IOException}
     * (a genuine fault, not this specific collision) propagates immediately, unretried, exactly as
     * before this method existed.
     */
    private CommitManifest publishWithBundleNameCollisionRetry(
        Directory mergedDirectory,
        SegmentInfos mergedInfos,
        long primaryTerm,
        long newGeneration,
        CommitManifest sourceManifest
    ) throws IOException {
        String bundleNameSuffix = "";
        for (int attempt = 0; attempt <= MAX_BUNDLE_NAME_COLLISION_RETRIES; attempt++) {
            try {
                // verifyIdempotentContent=false: this class's own javadoc documents that redoing
                // the merge on every retry is not byte-deterministic, so a genuine prior attempt of
                // this class's own can legitimately land at the same generation with different
                // content -- content verification would wrongly reject that as a foreign write. See
                // ObjectStoreCommitPublisher#publishCommit's own verifyIdempotentContent-overload
                // javadoc for the full reasoning.
                return commitPublisher.publishCommit(
                    mergedDirectory,
                    mergedInfos,
                    indexUuid,
                    shardId,
                    primaryTerm,
                    newGeneration,
                    sourceManifest.maxSeqNo(),
                    sourceManifest.localCheckpoint(),
                    sourceManifest.walPosition(),
                    sourceManifest.mappingVersion(),
                    sourceManifest.pruningStats(),
                    false,
                    bundleNameSuffix,
                    false
                );
            } catch (IOException e) {
                if (attempt == MAX_BUNDLE_NAME_COLLISION_RETRIES || isBundleNameCollision(e) == false) {
                    throw e;
                }
                bundleNameSuffix = "-r" + Long.toHexString(Randomness.get().nextLong());
            }
        }
        // Unreachable: the loop above always either returns or throws.
        throw new IllegalStateException("unreachable");
    }

    /** Walks {@code error}'s cause chain looking for {@code BlobContainerBundleStore#writeBundle}'s own documented collision-safety message. */
    private static boolean isBundleNameCollision(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause.getMessage() != null && cause.getMessage().contains("already exists with different real content")) {
                return true;
            }
        }
        return false;
    }
}
