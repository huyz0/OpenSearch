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
    private final org.opensearch.serverless.storage.gc.BlobGcCandidateLog gcCandidateLog;

    /**
     * Creates a publisher that packages commits via {@code commitPublisher} and races to install
     * them as the shard's new head via CAS on {@code shardStateStore}. Equivalent to the
     * three-argument constructor with {@code gcCandidateLog} {@code null} -- see that constructor's
     * own javadoc for what supplying one buys.
     *
     * @param commitPublisher packages a local Lucene commit into a bundle/manifest pair
     * @param shardStateStore the CAS-backed store holding this shard's published head
     */
    public ObjectStoreCommitHeadPublisher(ObjectStoreCommitPublisher commitPublisher, ShardStateStore shardStateStore) {
        this(commitPublisher, shardStateStore, null);
    }

    /**
     * Creates a publisher that also records every supersession it causes, for {@code GcCandidateTailer} to
     * discover instead of {@code GcSchedulerTask} relisting this shard's own container to rediscover it
     * later. See {@code GcCandidate}'s own javadoc for the fuller reasoning.
     *
     * @param commitPublisher packages a local Lucene commit into a bundle/manifest pair
     * @param shardStateStore the CAS-backed store holding this shard's published head
     * @param gcCandidateLog where a successful publish's superseded generation is recorded, or {@code null}
     *                       to skip recording entirely -- the same "feature is off" shape every other
     *                       optional collaborator in this engine already uses, and safe to be null: nothing
     *                       here depends on the log existing, only on {@code GcSchedulerTask}'s own sweep,
     *                       which keeps running regardless and would simply take longer to find what a
     *                       missing log entry would otherwise have surfaced sooner.
     */
    public ObjectStoreCommitHeadPublisher(
        ObjectStoreCommitPublisher commitPublisher,
        ShardStateStore shardStateStore,
        org.opensearch.serverless.storage.gc.BlobGcCandidateLog gcCandidateLog
    ) {
        this.commitPublisher = commitPublisher;
        this.shardStateStore = shardStateStore;
        this.gcCandidateLog = gcCandidateLog;
    }

    /**
     * Packages {@code segmentInfos} into a bundle+manifest via {@link ObjectStoreCommitPublisher} at
     * a generation computed live from the shard's current head, then attempts to publish it as the
     * shard's new head under {@code primaryTerm}. On a lost CAS race (another compaction or another
     * publish attempt by this same writer winning first), the generation is recomputed from the
     * freshly re-read head and the commit is packaged again at the new generation -- so more than
     * one manifest/bundle pair may be written to object storage for a single call under contention.
     * A manifest this call wrote and then failed to install is removed on the next iteration, once
     * the freshly re-read head proves it did not become the head (best-effort -- some containers
     * deny deletes, and GC collects it either way); its bundle remains unreferenced garbage for the
     * same GC path as any other orphan (see {@link ObjectStoreCommitPublisher}'s own class javadoc).
     * Bundles and manifests are addressed only through a successfully published head, so nothing
     * about correctness depended on that cleanup -- but rfc-serverless-opensearch.md &sect;6.3 also
     * keeps "list manifest names, highest term then generation" as a bootstrap/fallback discovery
     * path, and a plain listing cannot tell a CAS loser from the winner, so leaving losers behind
     * made that fallback's correctness a matter of convention rather than construction.
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
        return publishCommitAsHeadReturningManifest(
            directory,
            segmentInfos,
            indexUuid,
            shardId,
            primaryTerm,
            maxSeqNo,
            localCheckpoint,
            walPosition,
            mappingVersion,
            pruningStats,
            null,
            null,
            -1L
        ).isPresent();
    }

    /**
     * How many times the CAS/collision loop below may re-attempt before giving up. It used to be an
     * unbounded {@code for(;;)}, which under sustained contention meant unbounded re-publication --
     * and, since a bundle name embeds the target generation, unbounded full-shard bundle uploads,
     * each one immediately orphaned. A bounded budget turns "spin and upload forever" into a
     * transient failure the caller's own retry-with-backoff handles. Sixteen, matching {@link
     * #MAX_LEASE_ACQUISITION_ATTEMPTS} and the same bound {@code DescriptorGate#applyToDescriptor}
     * and {@code MappingGenerationStore} already use.
     *
     * <p>Exhaustion throws rather than failing the engine. That is a deliberate divergence from the
     * "fail the engine on exhaustion" suggestion: losing sixteen compare-and-swaps in a row is
     * contention or a struggling store, not a fencing verdict, and only a fencing verdict may destroy
     * a shard whose commit is already durable locally -- see {@code
     * ObjectStoreWriterEngine#publishWithRetry}. Genuine fencing returns empty from this method
     * instead, and the engine does fail on that.
     */
    static final int MAX_PUBLISH_ATTEMPTS = 16;

    /**
     * Same as {@link #publishCommitAsHead}, returning the manifest that actually became the head
     * (empty means fenced out), and accepting a delta base for incremental bundling.
     *
     * <p><b>Lost generation races are now retried, not fatal.</b> Both this class and the compactor
     * target {@code currentHead.latestManifestGeneration() + 1} under the current term, i.e. the
     * identical {@code manifest-<term>-<gen>} name, so a writer flush racing a compaction publish is
     * an entirely expected event. The loop below always handled a lost <em>CAS</em>; what it did not
     * handle was the collision being discovered <em>earlier</em>, while packaging, where it surfaced
     * as a bare {@code IOException} that escaped this loop and reached {@code
     * ObjectStoreWriterEngine#commitIndexWriter}'s catch-all -- which failed the primary over a
     * benign race. {@link ManifestGenerationCollisionException} now names that condition, and this
     * loop treats it exactly like a lost CAS: re-read the head, recompute the target generation, try
     * again.
     *
     * <p>One extra subtlety the naive "just re-read the head" retry misses: if the colliding writer
     * has published its manifest but has <em>not yet won the head CAS</em>, the head has not moved,
     * so the recomputed target generation is the same colliding one and the retry spins. {@code
     * minimumTargetGeneration} therefore ratchets past every generation already observed to be
     * occupied. {@code ShardHead#withPublishedGeneration} only requires the new generation to be
     * strictly greater than the current one, not exactly one greater, so skipping a slot is
     * perfectly legal.
     *
     * @param deltaBase a manifest the caller itself published from its own live local index, whose
     *                  file references may be carried forward instead of re-uploading the whole
     *                  commit; {@code null} to package the full commit. Used only while the live head
     *                  still points at exactly that manifest -- if a compaction (or anyone else) has
     *                  advanced the head since, the base describes a different Lucene index for this
     *                  shard and is dropped, because a file name shared between two different indexes
     *                  is not a guarantee of shared bytes. See {@code
     *                  ObjectStoreCommitPublisher#publishCommit}'s delta-bundling javadoc.
     * @param myNodeId the publishing node's own id, or {@code null} to fall back to the legacy
     *                 term-only fencing comparison (which, for a gated index whose primary term is a
     *                 compile-time constant, can never tell two nodes apart -- see {@link
     *                 #acquireOrRenewLease(String, int, long, String, long, long)}). A writer engine
     *                 must always supply it.
     * @param acquiredLeaseTerm the fencing token this node's own lease acquisition returned; ignored
     *                          when {@code myNodeId} is {@code null}.
     * @return the manifest that became this shard's head, or empty if this writer has been fenced out
     *         (its tenancy is no longer the one this shard's head recognises).
     */
    public Optional<CommitManifest> publishCommitAsHeadReturningManifest(
        Directory directory,
        SegmentInfos segmentInfos,
        String indexUuid,
        int shardId,
        long primaryTerm,
        long maxSeqNo,
        long localCheckpoint,
        WalPosition walPosition,
        long mappingVersion,
        PruningStats pruningStats,
        CommitManifest deltaBase,
        String myNodeId,
        long acquiredLeaseTerm
    ) throws IOException {
        long minimumTargetGeneration = 0L;
        CommitManifest losingManifest = null;
        for (int attempt = 1; attempt <= MAX_PUBLISH_ATTEMPTS; attempt++) {
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
                if (myNodeId != null) {
                    // Token fencing (the real check): this publish is allowed only while the head
                    // still records THIS node holding THIS tenancy. Node id alone would let a writer
                    // that lost the lease, had it taken over, and then re-acquired it publish a commit
                    // packaged under its FIRST tenancy, whose segment set predates whatever the
                    // intervening writer published; lease term alone would let a writer publish under
                    // a term another node acquired. See ShardHead#isStillHeldBy.
                    if (currentHead.isStillHeldBy(myNodeId, acquiredLeaseTerm) == false) {
                        // Fenced. Not a retry: a lost lease is not transient, and republishing the
                        // whole commit against a head somebody else owns is how two lineages get
                        // interleaved.
                        return Optional.empty();
                    }
                } else if (currentHead.leaseTerm() > primaryTerm) {
                    // Legacy term-only fencing, for callers that hold no token (the ten-argument
                    // overload, and tests that predate the token). Necessary but, on its own, not
                    // sufficient: for a gated index every writer's primary term is the compile-time
                    // constant IndexDescriptor.FIRST_PRIMARY_TERM, so this comparison is 1 > 1 and can
                    // never tell two nodes apart. A writer engine must use the token-carrying overload.
                    return Optional.empty();
                }
                // Not fenced: either this writer's own tenancy is still the one the head records
                // (token path), or -- on the legacy path -- no newer term has taken the lease. Either
                // way this publish is what legitimately moves the head's term forward to primaryTerm,
                // via withPublishedGeneration(primaryTerm, ...) below, which leaves the lease holder
                // and lease term untouched so this writer's token survives its own publish.
                currentGeneration = currentHead.latestManifestGeneration();
                currentVersion = Optional.of(versioned.version());
            }

            // A manifest this call wrote on a previous iteration and then failed to install is
            // unreferenced garbage that a listing-based "latest" resolution could still select (see
            // this class's own javadoc, and the report's P3). Now that the live head has been
            // re-read, we know precisely whether it is the head -- it is not, if the head names a
            // different (term, generation) -- so remove it. Best-effort: some containers deliberately
            // deny deletes (rfc-serverless-opensearch.md §15), and GC would eventually collect it
            // anyway; failing the publish over cleanup would be strictly worse.
            if (losingManifest != null) {
                // The condition is "the head has moved strictly PAST this generation," not merely
                // "the head is not this manifest right now." That is the difference between a
                // point-in-time observation and a proof: ShardHead#withPublishedGeneration refuses
                // any generation not strictly greater than the current one, so once the head sits
                // above this manifest's generation, no future CAS by anyone can ever install it.
                // The weaker check would leave a window in which a compactor -- which adopts an
                // existing manifest at its target generation by identity (its publishCommit call
                // passes verifyIdempotentContent=false) -- could install this very manifest as the
                // head between our read and our delete, and we would then be deleting the live head.
                if (currentHead != null && currentHead.latestManifestGeneration() > losingManifest.generation()) {
                    try {
                        commitPublisher.deleteUnreferencedManifest(losingManifest);
                    } catch (IOException ignored) {
                        // Deliberately swallowed -- see the comment above.
                    }
                }
                losingManifest = null;
            }

            // Always the next slot after the *live* head, exactly like the compactor -- never
            // derived from this writer's own local Lucene generation counter, which is what let a
            // stale writer either get wrongly fenced out or (narrower risk) collide with a different
            // actor's generation number under the old design. See this class's own javadoc. The
            // max() is the collision ratchet described in this method's javadoc.
            long targetGeneration = Math.max(currentGeneration + 1, minimumTargetGeneration);

            // Only usable while the head still names exactly this manifest -- see the deltaBase
            // parameter's own javadoc for why anything else is unsound rather than merely stale.
            CommitManifest usableDeltaBase = null;
            if (deltaBase != null
                && currentHead != null
                && currentHead.primaryTerm() == deltaBase.primaryTerm()
                && currentHead.latestManifestGeneration() == deltaBase.generation()) {
                usableDeltaBase = deltaBase;
            }

            CommitManifest manifest;
            try {
                manifest = commitPublisher.publishCommit(
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
                    pruningStats,
                    "",
                    true,
                    usableDeltaBase
                );
            } catch (ManifestGenerationCollisionException e) {
                // A lost race, not a failure: something else already owns this generation slot.
                // Ratchet past it and go round again.
                minimumTargetGeneration = targetGeneration + 1;
                if (attempt == MAX_PUBLISH_ATTEMPTS) {
                    throw e;
                }
                continue;
            }

            ShardHead newHead = currentHead == null
                ? new ShardHead(primaryTerm, null, 0L, manifest.generation())
                : currentHead.withPublishedGeneration(primaryTerm, manifest.generation());
            if (shardStateStore.compareAndSet(indexUuid, shardId, currentVersion, newHead) == CasResult.SUCCESS) {
                // currentHead is exactly what this publish just superseded -- known precisely, for free,
                // right here, which is the whole point of GcCandidate existing: nothing downstream has to
                // relist this shard's container to rediscover what this call already knows. Recorded only
                // on the winning attempt, not a losing CAS retry's currentHead reread above -- a retry's
                // manifest never became head at all, which is a different kind of garbage GcSchedulerTask's
                // own sweep remains responsible for (see GcCandidate's own javadoc).
                //
                // currentGeneration > 0, not merely currentHead != null: acquireOrRenewLease can
                // put-if-absent a lease-only head at generation 0 ahead of any real publish (see that
                // method's own javadoc, and readLatestManifest's identical "generation 0 means nothing
                // published yet" check). currentHead != null alone would append a candidate naming a
                // manifest that was never written -- deleting it is harmless (readManifest below simply
                // finds nothing), but it is not what this publish actually superseded, and polluting the
                // queue with an entry for a manifest that never existed is not "the queue is a little
                // noisier," it is wrong.
                if (gcCandidateLog != null && currentHead != null && currentGeneration > 0) {
                    gcCandidateLog.append(
                        new org.opensearch.serverless.storage.gc.GcCandidate(
                            indexUuid,
                            shardId,
                            currentHead.primaryTerm(),
                            currentGeneration
                        )
                    );
                }
                return Optional.of(manifest);
            }
            // Lost the race (another compaction or another publish attempt winning first) -- reread
            // the live head and retry with a freshly computed generation. Remember the manifest we
            // just wrote and could not install, so the top of the next iteration can remove it once
            // the fresh head read proves it did not become the head (see there).
            losingManifest = manifest;
            minimumTargetGeneration = Math.max(minimumTargetGeneration, manifest.generation() + 1);
        }
        throw new IOException(
            "failed to publish a commit as the head of shard ["
                + indexUuid
                + "]["
                + shardId
                + "] after "
                + MAX_PUBLISH_ATTEMPTS
                + " attempts under sustained contention"
        );
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
     * #publishCommitAsHead}, bounded at {@link #MAX_LEASE_ACQUISITION_ATTEMPTS}.
     *
     * @param indexUuid the index this shard belongs to
     * @param shardId the shard whose lease is being acquired or renewed
     * @param primaryTerm the primary term this writer believes it is currently active under
     * @param nodeId the id of the node acquiring or renewing the lease
     * @param leaseExpiryMillis the epoch millis at which this lease expires unless renewed again
     * @return {@code true} if the lease was successfully acquired or renewed; {@code false} if a live
     *         lease is held by another node, meaning this writer has been superseded. Kept for callers
     *         (and tests) that do not need the fencing token itself; the token-returning overload
     *         below is what a writer engine must use.
     */
    public boolean acquireOrRenewLease(String indexUuid, int shardId, long primaryTerm, String nodeId, long leaseExpiryMillis)
        throws IOException {
        return acquireOrRenewLease(indexUuid, shardId, primaryTerm, nodeId, leaseExpiryMillis, System.currentTimeMillis()).isPresent();
    }

    /**
     * The bound on both CAS loops in this class. It used to be {@code for (;;)} in both places, which
     * under sustained contention meant spinning silently rather than failing loudly -- and, on the
     * publish path, re-packaging a whole bundle+manifest on every turn, each one immediately orphaned.
     * Sixteen matches the bound {@code DescriptorGate#applyToDescriptor} and {@code
     * MappingGenerationStore} already use for the same reason.
     */
    static final int MAX_LEASE_ACQUISITION_ATTEMPTS = 16;

    /**
     * Acquires or renews this shard's writer lease and returns the <b>fencing token</b> -- the {@link
     * ShardHead#leaseTerm()} this acquisition installed -- which the caller must hold and present on
     * every subsequent publish. Empty means refused: a live lease is held by a different node.
     *
     * <p><b>Refusal is on node identity, not on a term comparison, and that is the whole point.</b>
     * Fencing here used to be written entirely as {@code currentHead.leaseTerm() > primaryTerm}. For a
     * gated index every shard's primary term is {@code IndexDescriptor.FIRST_PRIMARY_TERM = 1} and
     * nothing advances it -- there is no cluster-manager step to bump it -- and {@link
     * ShardHead#withRenewedLease} computes {@code max(leaseTerm, acquiringTerm)}, so against a constant
     * term it is the identity function and {@code leaseTerm} stayed pinned at 1 forever. The comparison
     * was {@code 1 > 1}: false, always. The refusal branch was dead code, {@code leaseHolderNodeId} was
     * simply overwritten by whoever asked last and never compared against the asker, and two nodes could
     * both be told "acquired" and both go on publishing full manifests derived from two different local
     * Lucene commits -- the head alternating between two lineages, each generation internally complete
     * and each missing the other's acknowledged documents, with the CAS reporting success to both
     * because a compare-and-swap on a counter serialises writes without saying anything about who is
     * entitled to make them.
     *
     * <p>A node id is the one thing about a writer that is always distinct and always known at both
     * ends, and needs no cluster-manager step to advance -- so acquisition refuses on {@link
     * ShardHead#isLeaseHeldByAnotherNodeAt}, and the monotonic token comes from {@link
     * ShardHead#withTakenOverLease}, which advances {@code leaseTerm} strictly on a takeover and never
     * on the holder's own heartbeat (bumping on every heartbeat would fence the only legitimate writer
     * out against the token it is holding, one renewal interval after activating).
     *
     * @param indexUuid the index this shard belongs to
     * @param shardId the shard whose lease is being acquired or renewed
     * @param primaryTerm the primary term this writer believes it is currently active under
     * @param nodeId the id of the node acquiring or renewing the lease
     * @param leaseExpiryMillis the epoch millis at which this lease expires unless renewed again
     * @param nowMillis the current epoch millis, used only to decide whether an existing lease is still live
     * @return the installed lease term (this writer's fencing token), or empty if another node holds a live lease
     */
    public java.util.OptionalLong acquireOrRenewLease(
        String indexUuid,
        int shardId,
        long primaryTerm,
        String nodeId,
        long leaseExpiryMillis,
        long nowMillis
    ) throws IOException {
        for (int attempt = 0; attempt < MAX_LEASE_ACQUISITION_ATTEMPTS; attempt++) {
            Optional<VersionedShardHead> current = shardStateStore.get(indexUuid, shardId);
            ShardHead currentHead = current.map(VersionedShardHead::head).orElse(null);
            if (currentHead != null && currentHead.isLeaseHeldByAnotherNodeAt(nodeId, nowMillis)) {
                // Refused on identity, not on term. For a gated index every writer's term is 1, so a
                // term comparison can never tell two nodes apart; an unexpired lease held by someone
                // else is an observation that does not depend on terms at all.
                return java.util.OptionalLong.empty();
            }
            ShardHead newHead;
            if (currentHead == null) {
                newHead = new ShardHead(primaryTerm, nodeId, leaseExpiryMillis, 0L);
            } else if (nodeId.equals(currentHead.leaseHolderNodeId())) {
                // A heartbeat by the holder. Must not advance the lease term, or a writer fences
                // itself out against the token it is holding, ten seconds after activating.
                newHead = currentHead.withRenewedLease(nodeId, leaseExpiryMillis, primaryTerm);
            } else {
                // A takeover of a free or lapsed lease -- the only thing that advances the token, and
                // it advances it strictly, so the displaced writer is fenced from this instant rather
                // than from whenever the new writer's first commit happens to land.
                newHead = currentHead.withTakenOverLease(nodeId, leaseExpiryMillis, primaryTerm);
            }
            Optional<Long> expectedVersion = current.map(VersionedShardHead::version);
            if (shardStateStore.compareAndSet(indexUuid, shardId, expectedVersion, newHead) == CasResult.SUCCESS) {
                return java.util.OptionalLong.of(newHead.leaseTerm());
            }
            // Lost the CAS -- reread and retry. Bounded, unlike the previous for(;;): a lease
            // acquisition that loses sixteen times running is a shard with more contenders than it
            // should have, and a silent infinite loop hides that.
        }
        return java.util.OptionalLong.empty();
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
