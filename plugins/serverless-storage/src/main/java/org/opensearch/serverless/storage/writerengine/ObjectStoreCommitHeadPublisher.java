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
                if (currentHead.leaseTerm() > primaryTerm) {
                    // A newer term has already acquired or renewed the lease -- possibly without
                    // having published anything yet, which is exactly why this compares against
                    // leaseTerm and not primaryTerm (see ShardHead#leaseTerm's own javadoc): a
                    // primaryTerm-only comparison would let this writer, if it had been fenced out
                    // by a takeover it never learned about (e.g. partitioned from the cluster
                    // manager), keep publishing under its own stale term until the new writer's
                    // first commit happened to land.
                    return false;
                }
                // currentHead.leaseTerm() <= primaryTerm: either this writer is continuing under
                // the term already on the head, or it is the first commit of a newly-activated
                // writer under a term nothing has published under yet -- either way this publish is
                // what legitimately moves the head's term forward to primaryTerm, via
                // withPublishedGeneration(primaryTerm, ...) below.
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
     * 4.5): without this, {@link ShardHead#leaseHolderNodeId()} is never set, and other code that
     * inspects lease presence (e.g. observability/diagnostics) would never see an active writer.
     * {@code CompactionSchedulerTask} itself no longer gates on lease presence -- it compacts purely
     * off {@code CompactionPolicy#shouldCompact}, relying on generation-based CAS fencing (see
     * {@code CompactionRebaseExecutor}) rather than the lease for safety.
     *
     * <p>Deliberately does <b>not</b> write the acquiring writer's term into {@code primaryTerm} --
     * only {@link #publishCommitAsHead} legitimately advances that field, since that is the one
     * place {@code (primaryTerm, latestManifestGeneration)} is kept in sync as a valid pointer to
     * the last real manifest (see {@link ShardHead#withRenewedLease}'s javadoc). It DOES immediately
     * advance {@link ShardHead#leaseTerm}, though: a writer that has just activated under a newly
     * bumped term but has not yet published anything must still fence out any writer still operating
     * under the old term the instant it acquires the lease, not only once its own first commit
     * lands -- otherwise a writer partitioned from the cluster manager and unaware it was superseded
     * could keep publishing under its stale term for as long as the new writer takes to publish its
     * first commit. See {@link ShardHead#leaseTerm}'s own javadoc.
     *
     * <p>Retries on a lost CAS race by rereading the live head and retrying, same shape as {@link
     * #publishCommitAsHead}. Returns {@code false} (never retries past this) only when the live head
     * already reflects a lease term newer than {@code primaryTerm} -- real evidence that a different
     * node already holds a newer term's lease, and this node must not go on renewing as this shard's
     * writer.
     *
     * @param indexUuid the index this shard belongs to
     * @param shardId the shard whose lease is being acquired or renewed
     * @param primaryTerm the primary term this writer believes it is currently active under
     * @param nodeId the id of the node acquiring or renewing the lease
     * @param leaseExpiryMillis the epoch millis at which this lease expires unless renewed again
     * @return {@code true} if the lease was successfully acquired or renewed; {@code false} if the
     *         live head already reflects a newer lease term, meaning this writer has been superseded
     */
    public boolean acquireOrRenewLease(String indexUuid, int shardId, long primaryTerm, String nodeId, long leaseExpiryMillis)
        throws IOException {
        for (;;) {
            Optional<VersionedShardHead> current = shardStateStore.get(indexUuid, shardId);
            ShardHead currentHead = current.map(VersionedShardHead::head).orElse(null);
            if (currentHead != null && currentHead.leaseTerm() > primaryTerm) {
                return false;
            }
            ShardHead newHead = currentHead == null
                ? new ShardHead(primaryTerm, nodeId, leaseExpiryMillis, 0)
                : currentHead.withRenewedLease(nodeId, leaseExpiryMillis, primaryTerm);
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

    /**
     * Same as {@link #readLatestManifest}, except the current head's {@code (primaryTerm,
     * generation)} is pinned via {@code pinRegistry} under {@code pinId} <b>before</b> the manifest
     * itself is read -- the same pin-before-read ordering {@link
     * org.opensearch.serverless.storage.clone.ShardCloner#clone} uses and for the identical reason
     * (formally verified in {@code formal/CloneGc.tla}): pinning only after reading the manifest
     * would leave a window where a concurrent GC sweep could observe this generation as superseded
     * and unpinned, and delete it, between this method's own read of the head and its write of the
     * pin.
     *
     * <p>Uses {@link org.opensearch.serverless.storage.retention.DurablePinRegistry#replacePin},
     * not {@code addPin}: a retried request under the same {@code pinId} (e.g. a client retry after
     * a newer generation has since published) must make the new generation the <em>only</em> pin
     * under that {@code pinId}, not accumulate a second pin alongside the stale one -- {@code
     * addPin} treats pins with the same {@code pinId} but a different generation as distinct
     * entries, which would leak the stale generation's pin (protected from GC forever) until this
     * {@code pinId} is eventually released outright.
     *
     * @param pinRegistry this shard's own durable pin registry
     * @param pinId the pin identifier to register the generation under (e.g. a snapshot UUID)
     * @return the pinned, just-read manifest, or empty if nothing has been published yet (nothing
     *         to pin)
     */
    public Optional<CommitManifest> readLatestManifestWithPin(
        String indexUuid,
        int shardId,
        org.opensearch.serverless.storage.retention.DurablePinRegistry pinRegistry,
        String pinId
    ) throws IOException {
        Optional<VersionedShardHead> current = shardStateStore.get(indexUuid, shardId);
        if (current.isEmpty()) {
            return Optional.empty();
        }
        ShardHead head = current.get().head();
        if (head.latestManifestGeneration() == 0) {
            return Optional.empty();
        }
        pinRegistry.replacePin(
            indexUuid,
            shardId,
            new org.opensearch.serverless.storage.retention.PinRecord(pinId, head.primaryTerm(), head.latestManifestGeneration())
        );
        try {
            return Optional.of(commitPublisher.readManifest(head.primaryTerm(), head.latestManifestGeneration()));
        } catch (IOException e) {
            // The pin above is now durable, but this method is about to fail without ever handing
            // back a manifest/pointer for anything downstream to key a later release off of --
            // release it ourselves here, or it is orphaned forever (still protected from GC, but
            // with nothing left anywhere that will ever call removePin for it).
            try {
                pinRegistry.removePin(indexUuid, shardId, pinId);
            } catch (IOException releaseFailure) {
                e.addSuppressed(releaseFailure);
            }
            throw e;
        }
    }
}
