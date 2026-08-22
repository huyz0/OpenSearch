/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SlowCodecReaderWrapper;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Closes the "logical-first, physical-later" gap {@link ShardSplitter} deliberately leaves open:
 * given a split target's current published head, materializes the full pre-split document set,
 * filters it down to just this target's own partition (reusing {@link PartitionFilteringDirectoryReader}
 * -- the exact same filter a live reader engine already applies at query time, so the rewritten
 * bundle is guaranteed to contain precisely what queries were already seeing, never a separate,
 * potentially-drifted recomputation), and republishes the filtered result as a fresh, physical,
 * partition-only bundle under a new manifest generation.
 *
 * <p>Once this runs, {@link BlobContainerShardPartitionStore#readDescriptor()} for this target no
 * longer needs to return anything -- {@link #rewrite} deletes the descriptor as its last step, after
 * the new manifest and head are both durably published, same ordering reasoning {@code
 * GcSchedulerTask}'s own bundle-before-manifest deletion ordering uses: only once the new,
 * self-sufficient manifest is durably reachable is it safe to stop telling future engine opens to
 * apply a filter.
 *
 * <p><b>What this does not do</b>: an already-open {@link org.opensearch.serverless.storage.readerengine.ObjectStoreReaderEngine}
 * reads its partition descriptor once, at construction, and keeps applying {@link
 * PartitionFilteringDirectoryReader} for as long as it stays open -- exactly like every other
 * engine-construction-time configuration in this plugin (compaction/GC configs, admission
 * controllers, ...). A live engine picks up the rewrite's effect (and the resulting drop in
 * per-refresh I/O) the next time it's reopened -- shard relocation, node restart, or any other
 * ordinary re-creation -- not instantaneously. This mirrors {@code CompactionSchedulerTask}'s own
 * publish-a-new-generation shape, where a currently-open reader's next scheduled manifest poll (not
 * an immediate push) is what picks up newly published content.
 *
 * <p>The old, pre-rewrite bundle is not deleted by this class -- it simply becomes unreferenced by
 * any live manifest once the new one publishes, so {@code GcSchedulerTask}'s own already-tested
 * sweep reclaims it on its normal schedule, exactly like a compaction's superseded source bundle
 * already does. No new deletion path was needed.
 */
public final class PartitionRewritePublisher {

    private final String indexUuid;
    private final int shardId;
    private final ShardStateStore shardStateStore;
    private final BlobContainerManifestStore manifestStore;
    private final org.opensearch.serverless.storage.format.BlobContainerBundleStore bundleStore;
    private final ObjectStoreCommitMaterializer materializer;
    private final ObjectStoreCommitPublisher commitPublisher;
    private final BlobContainerShardPartitionStore partitionStore;

    /**
     * Creates a publisher for one split target shard.
     *
     * @param indexUuid the target shard's index UUID.
     * @param shardId the target shard's numeric id within {@code indexUuid}.
     * @param shardStateStore reads and CASes the target's own published head.
     * @param manifestStore reads the target's currently published manifest.
     * @param bundleStore the same store {@code commitPublisher} itself writes through, passed
     *                     separately only so a CAS-failure rollback (see this class's own {@link
     *                     #rewrite} body) can clean up the bundle it published alongside its manifest.
     * @param materializer materializes the target's current manifest into a real Lucene directory.
     * @param commitPublisher publishes the filtered result as a new commit manifest.
     * @param partitionStore reads (and, on success, clears) the target's {@link ShardPartitionDescriptor}.
     */
    public PartitionRewritePublisher(
        String indexUuid,
        int shardId,
        ShardStateStore shardStateStore,
        BlobContainerManifestStore manifestStore,
        org.opensearch.serverless.storage.format.BlobContainerBundleStore bundleStore,
        ObjectStoreCommitMaterializer materializer,
        ObjectStoreCommitPublisher commitPublisher,
        BlobContainerShardPartitionStore partitionStore
    ) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.shardStateStore = shardStateStore;
        this.manifestStore = manifestStore;
        this.bundleStore = bundleStore;
        this.materializer = materializer;
        this.commitPublisher = commitPublisher;
        this.partitionStore = partitionStore;
    }

    /**
     * Rewrites this target's currently published manifest down to just its own partition and
     * republishes it, clearing the partition descriptor on success. A no-op returning {@code false}
     * if this target has no partition descriptor at all (never split, or already physically
     * rewritten by a prior call), or has no published head yet -- always safe to call
     * speculatively, the same idempotent-no-op shape {@code ShardCloner#deleteClone} already
     * establishes for "nothing to do here."
     *
     * @return {@code true} if a rewrite was actually performed and published; {@code false} if there was nothing to rewrite.
     * @throws IOException if the rewrite's own head CAS loses a race against another actor -- this
     *                      target is never expected to have a concurrent writer, so this is treated
     *                      as a genuine failure to surface, not something to silently retry -- or if
     *                      the CAS call itself faults ambiguously and a fresh re-read shows it
     *                      genuinely didn't land (see this method's own body for how that case's own
     *                      orphaned manifest gets cleaned up before the exception propagates).
     */
    public boolean rewrite() throws IOException {
        ShardPartitionDescriptor descriptor = partitionStore.readDescriptor().orElse(null);
        if (descriptor == null) {
            return false;
        }
        Optional<VersionedShardHead> currentVersionedHead = shardStateStore.get(indexUuid, shardId);
        if (currentVersionedHead.isEmpty()) {
            return false;
        }
        ShardHead currentHead = currentVersionedHead.get().head();
        CommitManifest currentManifest = manifestStore.readManifest(currentHead.primaryTerm(), currentHead.latestManifestGeneration());

        try (Directory sourceDirectory = new ByteBuffersDirectory(); Directory rewrittenDirectory = new ByteBuffersDirectory()) {
            materializer.materialize(currentManifest, sourceDirectory);

            try (DirectoryReader rawReader = DirectoryReader.open(sourceDirectory)) {
                try (PartitionFilteringDirectoryReader filtered = new PartitionFilteringDirectoryReader(rawReader, descriptor)) {
                    List<CodecReader> codecReaders = new ArrayList<>(filtered.leaves().size());
                    for (LeafReaderContext leafContext : filtered.leaves()) {
                        codecReaders.add(SlowCodecReaderWrapper.wrap(leafContext.reader()));
                    }
                    try (IndexWriter writer = new IndexWriter(rewrittenDirectory, new IndexWriterConfig())) {
                        if (codecReaders.isEmpty() == false) {
                            writer.addIndexes(codecReaders.toArray(new CodecReader[0]));
                        }
                        writer.commit();
                    }
                }
            }

            SegmentInfos rewrittenInfos = SegmentInfos.readLatestCommit(rewrittenDirectory);
            long newGeneration = currentHead.latestManifestGeneration() + 1;
            CommitManifest newManifest = commitPublisher.publishCommit(
                rewrittenDirectory,
                rewrittenInfos,
                indexUuid,
                shardId,
                currentHead.primaryTerm(),
                newGeneration,
                currentManifest.maxSeqNo(),
                currentManifest.localCheckpoint(),
                currentManifest.walPosition(),
                currentManifest.mappingVersion(),
                currentManifest.pruningStats()
            );

            // The scheduler that drives this method (PartitionRewriteSchedulerTask) catches every
            // exception and retries on the next tick, re-reading the head fresh each time -- so a
            // genuine VERSION_CONFLICT (a real concurrent actor, this method's own javadoc's "never
            // expected" case) safely lands on a different, fresh target generation next time and is
            // simply reported here, same as before. An IOException from the CAS call itself is
            // ambiguous, though -- unlike VERSION_CONFLICT, it does not say whether the write landed
            // or not, and the manifest at newGeneration this attempt already durably published stays
            // written either way. Left alone, an ambiguous non-landing failure would leave that
            // exact generation permanently occupied for the NEXT tick's retry to collide with: its
            // own fresh rewrite (a new IndexWriter/addIndexes run) is not byte-deterministic, so
            // ObjectStoreCommitPublisher#publishCommit's content-verification guard would then
            // reject that genuine self-retry as if it were a foreign write. Re-reading the head
            // resolves the ambiguity directly instead of leaving it to fester: if this attempt's own
            // write actually landed, adopt it as success; otherwise best-effort delete the orphaned
            // manifest (this class's own container has real delete permission, unlike
            // ShardCloner/ShardShrinker's target containers -- see RestrictingBlobContainer's own
            // javadoc) so the next tick's retry finds nothing occupying newGeneration and republishes
            // cleanly, rather than colliding with its own abandoned attempt.
            CasResult result;
            try {
                result = shardStateStore.compareAndSet(
                    indexUuid,
                    shardId,
                    Optional.of(currentVersionedHead.get().version()),
                    currentHead.withPublishedGeneration(newManifest.generation())
                );
            } catch (IOException casFailure) {
                Optional<VersionedShardHead> headAfterFailure = shardStateStore.get(indexUuid, shardId);
                // Both primaryTerm and generation must match, not generation alone: this attempt's
                // own CAS (currentHead.withPublishedGeneration) never changes primaryTerm, so a
                // mismatched term here means the head moved for some other reason entirely (this
                // target's "never expected to have a concurrent writer" case actually happening),
                // not this attempt's own write landing late.
                boolean actuallyLanded = headAfterFailure.isPresent()
                    && headAfterFailure.get().head().primaryTerm() == newManifest.primaryTerm()
                    && headAfterFailure.get().head().latestManifestGeneration() == newManifest.generation();
                if (actuallyLanded) {
                    partitionStore.clearDescriptor();
                    return true;
                }
                rollbackOrphanedManifest(newManifest, casFailure);
                throw casFailure;
            }
            if (result != CasResult.SUCCESS) {
                IOException lostRace = new IOException(
                    "partition rewrite for " + indexUuid + "/" + shardId + " lost a head CAS race -- unexpected for a split target"
                );
                rollbackOrphanedManifest(newManifest, lostRace);
                throw lostRace;
            }
        }

        // Last step, and deliberately after the new manifest and head are both durably published --
        // see class javadoc for why this ordering, not the reverse, is what keeps a mid-rewrite
        // crash merely retry-safe (a crash before this line just means the next attempt re-rewrites
        // from scratch, still correctly, since the descriptor is still there to read).
        partitionStore.clearDescriptor();
        return true;
    }

    /**
     * Best-effort cleanup of {@code orphanedManifest} (and the bundle it references) after this
     * attempt's own head CAS is known to have failed to land -- see {@link #rewrite}'s own body for
     * why leaving it in place would collide with a genuine later retry. The bundle is deleted
     * before the manifest, not after: {@code BlobContainerManifestStore#deleteManifests}'s own
     * javadoc documents bundles-before-manifests as the crash-safe ordering everywhere else in this
     * codebase (e.g. {@code GcSchedulerTask}'s sweep) -- a crash between the two steps then leaves,
     * at worst, a still-listed manifest pointing at an already-gone bundle (already an unusable,
     * clearly-broken state nothing would ever mistake for live), never an orphaned bundle with
     * nothing left to reference it and revisit it later. Both deletes are attempted independently:
     * a failure in either is attached to {@code originalFailure} as a suppressed exception rather
     * than replacing it, matching {@code ShardCloner#clone}'s own rollback-must-never-mask-the-real-
     * failure reasoning. Real delete permission is available here (unlike {@code ShardCloner}/{@code
     * ShardShrinker}'s target containers -- see {@code RestrictingBlobContainer}'s own javadoc for
     * why this action is the one exception), so this is expected to actually succeed in the normal,
     * deployed wiring, not merely in tests.
     */
    private void rollbackOrphanedManifest(CommitManifest orphanedManifest, IOException originalFailure) {
        java.util.Set<String> bundleNames = new java.util.HashSet<>();
        orphanedManifest.files().values().forEach(ref -> bundleNames.add(ref.bundleName()));
        try {
            bundleStore.deleteBundles(bundleNames);
        } catch (IOException | RuntimeException rollbackFailure) {
            originalFailure.addSuppressed(rollbackFailure);
        }
        try {
            manifestStore.deleteManifests(List.of(orphanedManifest));
        } catch (IOException | RuntimeException rollbackFailure) {
            originalFailure.addSuppressed(rollbackFailure);
        }
    }
}
