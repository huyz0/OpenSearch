/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;

import java.io.IOException;
import java.util.Optional;

/**
 * The decision {@link ObjectStoreCommitPublisher} deliberately does not make: whether a just-packaged
 * commit is allowed to become the shard's officially visible head (rfc-serverless-metadata-plane.md
 * &sect;4/&sect;7). This is the term-fencing check that keeps a writer that has been superseded (its
 * lease expired and another node took over) from publishing a generation after the fact.
 *
 * <p>The target generation is <b>not</b> supplied by the caller's local Lucene state. It is always
 * computed live, inside the retry loop, as {@code currentHead.latestManifestGeneration() + 1} --
 * mirroring exactly how the compaction service (LuceneMergeCompactionPublisher) already numbers its
 * own publications. This closes a class of bug formally verified in
 * {@code plugins/serverless-storage/formal/ShardHead.tla} (`PublishDecoupled`/`SpecDecoupled`,
 * `ShardHeadDecoupled.cfg`): with a caller-supplied generation entangled with local Lucene state, a
 * writer resuming after a compactor advanced the head could either be wrongly fenced out (see the
 * git history of this class for the pre-decoupling version) or -- the narrower risk this decoupling
 * additionally closes -- have its own generation number coincidentally collide with a different
 * actor's, which the old caller-supplied-generation design could not distinguish since {@link
 * ShardHead} carries no manifest-identity field. With live computation there is no external
 * generation to collide: every successful publish, by construction, lands at the current head's
 * generation plus exactly one.
 */
public final class ObjectStoreCommitHeadPublisher {

    private final ObjectStoreCommitPublisher commitPublisher;
    private final ShardStateStore shardStateStore;

    /**
     * Creates a publisher that packages commits via {@code commitPublisher} and races to install
     * them as the shard's new head via CAS on {@code shardStateStore}.
     *
     * @param commitPublisher packages a local Lucene commit into a bundle/manifest pair
     * @param shardStateStore the CAS-backed store holding this shard's published head
     */
    public ObjectStoreCommitHeadPublisher(ObjectStoreCommitPublisher commitPublisher, ShardStateStore shardStateStore) {
        this.commitPublisher = commitPublisher;
        this.shardStateStore = shardStateStore;
    }

    /**
     * Packages {@code segmentInfos} into a bundle+manifest via {@link ObjectStoreCommitPublisher} at
     * a generation computed live from the shard's current head, then attempts to publish it as the
     * shard's new head under {@code primaryTerm}. On a lost CAS race (another compaction or another
     * publish attempt by this same writer winning first), the generation is recomputed from the
     * freshly re-read head and the commit is packaged again at the new generation -- so more than
     * one manifest/bundle pair may be written to object storage for a single call under contention.
     * Every manifest not referenced by the head that ultimately wins the CAS is simply unreferenced
     * garbage, eligible for the same GC path as any other orphaned bundle (see {@link
     * ObjectStoreCommitPublisher}'s own class javadoc) -- never a correctness issue, since bundles
     * and manifests are addressed only through a successfully published head.
     *
     * @param directory the local Lucene {@link Directory} holding the files referenced by {@code segmentInfos}
     * @param segmentInfos the local Lucene commit to package and attempt to publish
     * @param indexUuid the index this shard belongs to
     * @param shardId the shard being published
     * @param primaryTerm the primary term this writer believes it is currently active under
     * @param maxSeqNo the maximum sequence number covered by this commit
     * @param localCheckpoint the local checkpoint covered by this commit
     * @param walPosition the WAL position this commit's manifest should record
     * @param mappingVersion the mapping version in effect for this commit
     * @param pruningStats pruning statistics to record in the manifest
     * @return {@code true} if this commit's content is now reflected in the shard's published head;
     *         {@code false} if a different term currently holds the head, meaning this writer has
     *         been fenced out and must stop writing.
     */
    public boolean publishCommitAsHead(
        Directory directory,
        SegmentInfos segmentInfos,
        String indexUuid,
        int shardId,
        long primaryTerm,
        long maxSeqNo,
        long localCheckpoint,
        WalPosition walPosition,
        long mappingVersion,
        PruningStats pruningStats
    ) throws IOException {
        for (;;) {
            Optional<VersionedShardHead> current = shardStateStore.get(indexUuid, shardId);

            long currentGeneration;
            ShardHead currentHead;
            Optional<Long> currentVersion;
            if (current.isEmpty()) {
                currentGeneration = 0L;
                currentHead = null;
                currentVersion = Optional.empty();
            } else {
                VersionedShardHead versioned = current.get();
                currentHead = versioned.head();
                if (currentHead.primaryTerm() > primaryTerm) {
                    // A newer term already holds the head -- this writer has been superseded and
                    // must not publish.
                    return false;
                }
                // currentHead.primaryTerm() <= primaryTerm: either this writer is continuing under
                // the term already on the head, or it is the first commit of a newly-activated
                // writer under a term nothing has published under yet (see
                // acquireOrRenewLease's javadoc for why lease acquisition alone does not already
                // advance the head's term) -- either way this publish is what legitimately moves the
                // head's term forward to primaryTerm, via withPublishedGeneration(primaryTerm, ...)
                // below.
                currentGeneration = currentHead.latestManifestGeneration();
                currentVersion = Optional.of(versioned.version());
            }

            // Always the next slot after the *live* head, exactly like the compactor -- never
            // derived from this writer's own local Lucene generation counter, which is what let a
            // stale writer either get wrongly fenced out or (narrower risk) collide with a different
            // actor's generation number under the old design. See this class's own javadoc.
            long targetGeneration = currentGeneration + 1;

            CommitManifest manifest = commitPublisher.publishCommit(
                directory,
                segmentInfos,
                indexUuid,
                shardId,
                primaryTerm,
                targetGeneration,
                maxSeqNo,
                localCheckpoint,
                walPosition,
                mappingVersion,
                pruningStats
            );

            ShardHead newHead = currentHead == null
                ? new ShardHead(primaryTerm, null, 0L, manifest.generation())
                : currentHead.withPublishedGeneration(primaryTerm, manifest.generation());
            if (shardStateStore.compareAndSet(indexUuid, shardId, currentVersion, newHead) == CasResult.SUCCESS) {
                return true;
            }
            // Lost the race (another compaction or another publish attempt winning first) -- reread
            // the live head and retry with a freshly computed generation.
        }
    }

    /**
     * Acquires or renews this shard's writer lease (rfc-serverless-opensearch.md &sect;16 Phase
     * 4.5): without this, {@link ShardHead#leaseHolderNodeId()} is never set, so {@code
     * CompactionSchedulerTask}'s {@code isLeaseHeldAt} guard can never actually observe an active
     * writer and always treats the shard as available.
     *
     * <p>Deliberately does <b>not</b> write {@code primaryTerm} into the head -- only {@link
     * #publishCommitAsHead} legitimately advances the head's term, since that is the one place
     * {@code (primaryTerm, latestManifestGeneration)} is kept in sync as a valid pointer to the last
     * real manifest (see {@link ShardHead#withRenewedLease}'s javadoc). This means a writer that has
     * just activated under a newly bumped term but has not yet published anything can still call
     * this and have it succeed against the head's still-old term -- that is fine, since fencing
     * correctness itself lives entirely in {@link #publishCommitAsHead}, not here.
     *
     * <p>Retries on a lost CAS race by rereading the live head and retrying, same shape as {@link
     * #publishCommitAsHead}. Returns {@code false} (never retries past this) only when the live head
     * already reflects a term newer than {@code primaryTerm} -- real evidence (a publication) that
     * this node has been superseded, and it must not go on renewing as this shard's writer.
     *
     * @param indexUuid the index this shard belongs to
     * @param shardId the shard whose lease is being acquired or renewed
     * @param primaryTerm the primary term this writer believes it is currently active under
     * @param nodeId the id of the node acquiring or renewing the lease
     * @param leaseExpiryMillis the epoch millis at which this lease expires unless renewed again
     * @return {@code true} if the lease was successfully acquired or renewed; {@code false} if the
     *         live head already reflects a newer term, meaning this writer has been superseded
     */
    public boolean acquireOrRenewLease(String indexUuid, int shardId, long primaryTerm, String nodeId, long leaseExpiryMillis)
        throws IOException {
        for (;;) {
            Optional<VersionedShardHead> current = shardStateStore.get(indexUuid, shardId);
            ShardHead currentHead = current.map(VersionedShardHead::head).orElse(null);
            if (currentHead != null && currentHead.primaryTerm() > primaryTerm) {
                return false;
            }
            ShardHead newHead = currentHead == null
                ? new ShardHead(primaryTerm, nodeId, leaseExpiryMillis, 0)
                : currentHead.withRenewedLease(nodeId, leaseExpiryMillis);
            Optional<Long> expectedVersion = current.map(VersionedShardHead::version);
            if (shardStateStore.compareAndSet(indexUuid, shardId, expectedVersion, newHead) == CasResult.SUCCESS) {
                return true;
            }
            // Lost the race -- reread the live head and retry.
        }
    }

    /**
     * The latest durably-published manifest for this shard, if any -- what a writer activating
     * (under a new or resumed term) reads to determine where WAL replay must resume from (the
     * manifest's own {@link WalPosition}) and what Lucene generation local recovery should already
     * reflect. Empty both when this shard has never had a head at all, and when a head exists but
     * {@code latestManifestGeneration() == 0} -- the same "nothing published yet" sentinel {@link
     * ShardHead#initial()} and {@code CompactionSchedulerTask} already treat that way, which since
     * {@link #acquireOrRenewLease} can now put-if-absent a lease-only head with generation 0 ahead of
     * any real publish, is no longer implied by head presence alone.
     *
     * @param indexUuid the index this shard belongs to
     * @param shardId the shard whose latest published manifest is being read
     * @return the latest published manifest, or empty if nothing has been published yet
     */
    public Optional<CommitManifest> readLatestManifest(String indexUuid, int shardId) throws IOException {
        Optional<VersionedShardHead> current = shardStateStore.get(indexUuid, shardId);
        if (current.isEmpty()) {
            return Optional.empty();
        }
        ShardHead head = current.get().head();
        if (head.latestManifestGeneration() == 0) {
            return Optional.empty();
        }
        return Optional.of(commitPublisher.readManifest(head.primaryTerm(), head.latestManifestGeneration()));
    }
}
