/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.clone;

import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinRecord;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The control-plane half of zero-copy clone (rfc-serverless-opensearch.md &sect;4.6/&sect;14): a
 * brand-new index/shard whose very first manifest references an existing shard's bundles instead
 * of writing any new segment bytes. {@link #clone} does exactly two durable writes -- a pin on the
 * source generation, then the target's manifest and head -- and nothing else; it never touches or
 * copies any bundle bytes.
 *
 * <p><b>Why a pin, not {@link org.opensearch.serverless.storage.gc.BundleReferenceCounter}'s
 * documented cross-index scan</b>: that class's own javadoc says liveness must be evaluated
 * globally so a clone's source deletion can't corrupt the clone, but {@code GcSchedulerTask} today
 * only ever scans its own shard's manifests -- making that cross-index scan real would mean every
 * shard's GC sweep listing every other index's manifests too, an unbounded, expensive change.
 * {@link DurablePinRegistry} already exists precisely to keep a specific manifest generation (and
 * therefore everything it references, since bundle files are never mutated in place -- see {@code
 * ObjectStoreCommitMaterializer}'s own additive-write javadoc) alive regardless of what a normal
 * sweep would otherwise conclude, the same mechanism a snapshot or PITR window already uses. Pinning
 * the exact source generation this clone was made from is precise (unlike a broad cross-index scan,
 * it protects exactly the bytes actually referenced, nothing more) and requires zero change to
 * {@code GcSchedulerTask}/{@code BundleReferenceCounter} -- the existing per-shard sweep on the
 * <em>source</em> shard already consults {@link DurablePinRegistry#getPinnedManifestIds} on every
 * tick.
 *
 * <p>{@link #clone} also records a {@link CloneLineage} in the target's own container so a later
 * {@link #deleteClone} can find its way back to the source's pin without anything else needing to
 * remember the relationship -- see {@link #deleteClone}'s own javadoc for what it does and does
 * not do. That same {@link CloneLineage} record is also what lets a cloned shard's reader-only
 * lazy directory resolve inherited bundles, via {@code ServerlessStorageLazyDirectoryFactory
 * #resolveStreamReader} and {@link FallbackStreamReader} -- the {@code
 * TransferManager}/{@code LazyBundleIndexInput} counterpart of {@link FallbackBundleFileReader}'s
 * support for the eager materializer path; both read paths are wired for a cloned shard today.
 *
 * <p><b>Pin ordering is load-bearing, formally verified</b> (rfc-serverless-opensearch.md
 * &sect;18.5's "coordination-free GC" correctness surface): {@link #clone} adds the pin using the
 * generation number from the {@link ShardHead} read alone, <em>before</em> ever calling {@link
 * BlobContainerManifestStore#readManifest}, not after. An earlier pin-after-read ordering had a
 * genuine TOCTOU race with {@code GcSchedulerTask}'s independent sweep -- see {@code
 * formal/CloneGc.tla}, whose {@code NoCloneEverReferencesADeletedGenerationBuggy} property is
 * VIOLATED with a concrete counterexample for that ordering (a newer commit supersedes the
 * just-read generation, a sweep deletes it, and the pin that arrives moments later protects
 * bundles already gone) and whose {@code ...Fixed} property HOLDS exhaustively for this ordering.
 *
 * <p>{@link #deleteClone} also now fires automatically: {@code
 * ServerlessStoragePlugin#onIndexModule} registers an {@code IndexEventListener} that calls it on
 * {@code afterIndexRemoved(..., IndexRemovalReason.DELETED)} -- see that method's own javadoc for
 * why that hook, not the shard-level {@code afterIndexShardDeleted} (which also fires on plain
 * relocation), and for why {@link #deleteClone}'s idempotency under redundant/concurrent calls is
 * load-bearing there, not just a nicety.
 */
public final class ShardCloner {

    private ShardCloner() {}

    /**
     * Clones a source shard's current published manifest into a brand-new target shard identity.
     *
     * @param sourceIndexUuid the index being cloned from; must already have a published manifest.
     * @param sourceShardId the shard number within {@code sourceIndexUuid}.
     * @param sourceManifestStore where the source shard's manifests live.
     * @param sourceShardStateStore where the source shard's head lives.
     * @param sourcePinRegistry the source shard's durable pin registry, protected via this call.
     * @param targetIndexUuid the brand-new index the clone creates; must not already have a
     *                        published head -- cloning onto an already-active shard is refused,
     *                        the same put-if-absent contract {@link ShardStateStore#compareAndSet}
     *                        already gives every first activation.
     * @param targetShardId the shard number within {@code targetIndexUuid}.
     * @param targetManifestStore where the target shard's new manifest is written.
     * @param targetShardStateStore where the target shard's new head is published.
     * @param targetLineageStore where the target shard's {@link CloneLineage} is recorded.
     * @param nowMillis the target manifest's {@link CommitManifest#createdAtMillis()} -- passed in
     *                  rather than read internally so this class stays trivially deterministic to
     *                  test.
     * @throws IOException if the source has no published manifest, or the target already does --
     *                      the first refusal happens before any durable write at all, and the
     *                      second (and any other failure between the source pin and the head CAS)
     *                      rolls back every durable write this call itself made before rethrowing,
     *                      so neither ever leaves the target, or the source's pin, in a half-written
     *                      state (see this method's own body for exactly what that rollback does and
     *                      why it's safe).
     */
    public static void clone(
        String sourceIndexUuid,
        int sourceShardId,
        BlobContainerManifestStore sourceManifestStore,
        ShardStateStore sourceShardStateStore,
        DurablePinRegistry sourcePinRegistry,
        String targetIndexUuid,
        int targetShardId,
        BlobContainerManifestStore targetManifestStore,
        ShardStateStore targetShardStateStore,
        BlobContainerCloneLineageStore targetLineageStore,
        long nowMillis
    ) throws IOException {
        clone(
            sourceIndexUuid,
            sourceShardId,
            sourceManifestStore,
            sourceShardStateStore,
            sourcePinRegistry,
            targetIndexUuid,
            targetShardId,
            targetManifestStore,
            targetShardStateStore,
            targetLineageStore,
            nowMillis,
            null
        );
    }

    /**
     * Same as the eleven-argument overload, with one addition: {@code beforeActivation}, run after
     * every durable write up to and including the target's manifest, but strictly before the head
     * {@link ShardStateStore#compareAndSet} below that is what actually makes the target shard
     * visible/openable. {@code null} is a no-op, matching the eleven-argument overload's behavior
     * exactly. Delegates to the thirteen-argument overload with no compensating rollback for {@code
     * beforeActivation} -- equivalent to passing {@code null} there too.
     *
     * <p>This exists for {@link org.opensearch.serverless.storage.resharding.ShardSplitter#split},
     * which needs to durably write a {@code ShardPartitionDescriptor} before the target becomes
     * openable -- not after, the way an earlier version of that class did it. Writing the
     * descriptor after this method's own head CAS left a real window where an engine-open retry
     * landing between the CAS succeeding and the descriptor write completing would cache "no
     * partition filter" for that engine's entire lifetime, silently serving the full pre-split
     * document set instead of just the target's own slice -- exactly the kind of TOCTOU this
     * class's own pin-before-read fix (see this class's own javadoc, {@code formal/CloneGc.tla})
     * already established the pattern for solving: do the extra durable write before the
     * visibility-creating step, not after it.
     *
     * @param sourceIndexUuid the index being cloned from; must already have a published manifest.
     * @param sourceShardId the shard number within {@code sourceIndexUuid}.
     * @param sourceManifestStore where the source shard's manifests live.
     * @param sourceShardStateStore where the source shard's head lives.
     * @param sourcePinRegistry the source shard's durable pin registry, protected via this call.
     * @param targetIndexUuid the brand-new index the clone creates; must not already have a
     *                        published head -- cloning onto an already-active shard is refused,
     *                        the same put-if-absent contract {@link ShardStateStore#compareAndSet}
     *                        already gives every first activation.
     * @param targetShardId the shard number within {@code targetIndexUuid}.
     * @param targetManifestStore where the target shard's new manifest is written.
     * @param targetShardStateStore where the target shard's new head is published.
     * @param targetLineageStore where the target shard's {@link CloneLineage} is recorded.
     * @param nowMillis the target manifest's {@link CommitManifest#createdAtMillis()} -- passed in
     *                  rather than read internally so this class stays trivially deterministic to
     *                  test.
     * @param beforeActivation run after the manifest write, before the head CAS; {@code null} for none.
     * @throws IOException if the source has no published manifest, or the target already does.
     */
    public static void clone(
        String sourceIndexUuid,
        int sourceShardId,
        BlobContainerManifestStore sourceManifestStore,
        ShardStateStore sourceShardStateStore,
        DurablePinRegistry sourcePinRegistry,
        String targetIndexUuid,
        int targetShardId,
        BlobContainerManifestStore targetManifestStore,
        ShardStateStore targetShardStateStore,
        BlobContainerCloneLineageStore targetLineageStore,
        long nowMillis,
        CheckedRunnable<IOException> beforeActivation
    ) throws IOException {
        clone(
            sourceIndexUuid,
            sourceShardId,
            sourceManifestStore,
            sourceShardStateStore,
            sourcePinRegistry,
            targetIndexUuid,
            targetShardId,
            targetManifestStore,
            targetShardStateStore,
            targetLineageStore,
            nowMillis,
            beforeActivation,
            null
        );
    }

    /**
     * Same as the twelve-argument overload, with one addition: {@code rollbackBeforeActivation},
     * this method's own compensating action for {@code beforeActivation} -- run, best-effort, from
     * this method's own failure-rollback handling (see this method's own body) if and only if {@code
     * beforeActivation} itself is known to have completed successfully. {@code null} is a no-op,
     * matching the twelve-argument overload's behavior exactly: {@link
     * org.opensearch.serverless.storage.resharding.ShardSplitter#split} is the only caller that
     * supplies one, to clear the {@code ShardPartitionDescriptor} {@code beforeActivation} wrote
     * (see {@code BlobContainerShardPartitionStore#clearDescriptor}) if this attempt fails at the
     * head CAS afterward -- this method has no way to know what an opaque {@code beforeActivation}
     * callback did, so it relies entirely on the caller's own matching compensating action here,
     * the same shape {@code beforeActivation} itself already uses.
     *
     * @param sourceIndexUuid the index being cloned from; must already have a published manifest.
     * @param sourceShardId the shard number within {@code sourceIndexUuid}.
     * @param sourceManifestStore where the source shard's manifests live.
     * @param sourceShardStateStore where the source shard's head lives.
     * @param sourcePinRegistry the source shard's durable pin registry, protected via this call.
     * @param targetIndexUuid the brand-new index the clone creates; must not already have a
     *                        published head -- cloning onto an already-active shard is refused,
     *                        the same put-if-absent contract {@link ShardStateStore#compareAndSet}
     *                        already gives every first activation.
     * @param targetShardId the shard number within {@code targetIndexUuid}.
     * @param targetManifestStore where the target shard's new manifest is written.
     * @param targetShardStateStore where the target shard's new head is published.
     * @param targetLineageStore where the target shard's {@link CloneLineage} is recorded.
     * @param nowMillis the target manifest's {@link CommitManifest#createdAtMillis()} -- passed in
     *                  rather than read internally so this class stays trivially deterministic to
     *                  test.
     * @param beforeActivation run after the manifest write, before the head CAS; {@code null} for none.
     * @param rollbackBeforeActivation this attempt's own undo of {@code beforeActivation}, run only
     *                                 if {@code beforeActivation} itself completed; {@code null} for none.
     * @throws IOException if the source has no published manifest, or the target already does.
     */
    public static void clone(
        String sourceIndexUuid,
        int sourceShardId,
        BlobContainerManifestStore sourceManifestStore,
        ShardStateStore sourceShardStateStore,
        DurablePinRegistry sourcePinRegistry,
        String targetIndexUuid,
        int targetShardId,
        BlobContainerManifestStore targetManifestStore,
        ShardStateStore targetShardStateStore,
        BlobContainerCloneLineageStore targetLineageStore,
        long nowMillis,
        CheckedRunnable<IOException> beforeActivation,
        CheckedRunnable<IOException> rollbackBeforeActivation
    ) throws IOException {
        // Fails with a specific, actionable message for a PRIOR, different-source clone attempt's
        // still-present lineage (e.g. one that pinned its source but then lost the head CAS below
        // and was never cleaned up -- see this method's own catch block below, which now handles
        // that specific case automatically; this check remains for lineage left by an OLDER build
        // that predates that cleanup, or any other reason a stale record survives). Without this
        // check, execution would reach writeLineage below and get a much less actionable failure
        // there instead: BlobContainerCloneLineageStore#writeLineage is write-once (its underlying
        // writeBlobAtomic call fails if the blob already exists, it never silently overwrites), so a
        // different source's lineage record already occupying this target would make that write
        // fail regardless -- this check exists to fail with the specific, actionable message below,
        // not to prevent an overwrite that couldn't happen anyway. A retry with the SAME source is
        // still a harmless no-op (addPin below is idempotent for an identical PinRecord, and
        // writeLineage below succeeds trivially since the existing blob's content already matches
        // exactly what it would write), matching this method's own documented retry-safety; only a
        // genuinely different source is refused.
        Optional<CloneLineage> existingLineage = targetLineageStore.readLineage();
        if (existingLineage.isPresent()
            && (existingLineage.get().sourceIndexUuid().equals(sourceIndexUuid) == false
                || existingLineage.get().sourceShardId() != sourceShardId)) {
            throw new IOException(
                "target shard "
                    + targetIndexUuid
                    + "/"
                    + targetShardId
                    + " already has clone lineage pointing at "
                    + existingLineage.get().sourceIndexUuid()
                    + "/"
                    + existingLineage.get().sourceShardId()
                    + " from a prior attempt -- call deleteClone to release that pin before retrying with a different source"
            );
        }

        Optional<VersionedShardHead> sourceHead = sourceShardStateStore.get(sourceIndexUuid, sourceShardId);
        if (sourceHead.isEmpty() || sourceHead.get().head().latestManifestGeneration() == 0) {
            throw new IOException("source shard " + sourceIndexUuid + "/" + sourceShardId + " has no published manifest to clone from");
        }
        ShardHead head = sourceHead.get().head();

        // Pin BEFORE reading the manifest, using the generation number already known from the
        // ShardHead read above -- not after, and not derived from the manifest read below. A
        // pin-after-read ordering has a genuine TOCTOU race: GcSchedulerTask's sweep could delete
        // this exact generation in the window between the manifest read succeeding and the pin
        // landing, if a newer commit had meanwhile superseded it, leaving the pin protecting
        // bundles that are already gone. Formally verified in formal/CloneGc.tla -- the pin-after-
        // read ordering is shown VIOLATED with a concrete counterexample, this pin-before-read
        // ordering is shown to hold across the complete reachable state space for that model.
        PinRecord pin = new PinRecord(clonePinId(targetIndexUuid, targetShardId), head.primaryTerm(), head.latestManifestGeneration());
        sourcePinRegistry.addPin(sourceIndexUuid, sourceShardId, pin);

        // Every step from here through the head CAS is wrapped in one try/catch: a failure anywhere
        // in this range rolls back everything THIS call itself durably wrote, tracked explicitly per
        // step below rather than inferred from reaching a later line -- each of writeLineage,
        // writeManifest, and beforeActivation can throw either because nothing was written yet (a
        // transient I/O error) or because it's write-once and something else already durably
        // occupies that exact name (e.g. the target is already active from something other than
        // this clone). In neither case did THIS call's own write succeed, so the catch block below
        // must never attempt to undo a step whose own flag says it didn't actually land.
        boolean lineageWritten = false;
        boolean manifestWritten = false;
        boolean beforeActivationRan = false;
        CommitManifest targetManifest = null;
        try {
            CommitManifest sourceManifest = sourceManifestStore.readManifest(head.primaryTerm(), head.latestManifestGeneration());
            // Lineage before the head CAS below makes the clone visible/active -- so whenever a
            // clone is visible, deleteClone can already find its way back to the pin it must remove.
            targetLineageStore.writeLineage(new CloneLineage(sourceIndexUuid, sourceShardId));
            lineageWritten = true;

            targetManifest = new CommitManifest(
                targetIndexUuid,
                targetShardId,
                1,
                1,
                sourceManifest.segmentsFileName(),
                sourceManifest.files(),
                sourceManifest.maxSeqNo(),
                sourceManifest.localCheckpoint(),
                null,
                sourceManifest.mappingVersion(),
                PruningStats.empty(),
                nowMillis
            );
            targetManifestStore.writeManifest(targetManifest);
            manifestWritten = true;

            if (beforeActivation != null) {
                beforeActivation.run();
                beforeActivationRan = true;
            }

            CasResult result = targetShardStateStore.compareAndSet(
                targetIndexUuid,
                targetShardId,
                Optional.empty(),
                new ShardHead(1, null, 0L, 1)
            );
            if (result != CasResult.SUCCESS) {
                throw new IOException(
                    "target shard " + targetIndexUuid + "/" + targetShardId + " already has a published head; refusing to clone onto it"
                );
            }
        } catch (IOException | RuntimeException e) {
            // This specific attempt will never get a later chance to complete: a retry with the same
            // target either hits this same failure again (the target was already active for an
            // unrelated reason) or is a genuinely idempotent no-op (the target is this exact attempt
            // succeeding on a later try, which never reaches this catch). Rolling back here, rather
            // than leaving durable state permanently unreclaimed or wrong, matters most for the case
            // a plain retry can never fix: the target was already active from something other than
            // this clone (e.g. an ordinary index creation racing this call, or a stale request
            // against an already-cloned target). Left in place, a failed attempt's lineage record
            // would misdescribe that unrelated, legitimate shard's origin forever, its pin would
            // block the source shard's GC forever for a clone that will never exist, and -- the most
            // dangerous of the three -- its stray manifest at (1, 1) would sit there ready for
            // ObjectStoreCommitPublisher#publishCommit's own "manifest already exists at this (term,
            // generation), return it unchanged" guard to silently hand this attempt's SOURCE shard's
            // file references back to whatever later, genuinely first commit that unrelated target
            // shard eventually makes at (1, 1) -- silent misdirection to the wrong segment files, not
            // just a failed retry.
            //
            // Every rollback step below is best-effort and independently guarded: a failure in ANY
            // one of them must never mask e, the real reason this attempt failed and the only thing
            // the caller actually needs to see -- a rollback failure is attached to it as a
            // suppressed exception (surfaced to anything that inspects the thrown exception in
            // detail) rather than replacing it, and every other rollback step still runs regardless
            // of whether an earlier one failed.
            //
            // The pin is always attempted, regardless of which later step (if any) actually ran:
            // removePin(String, int, PinRecord) is an exact-match compare-and-remove (see that
            // method's own javadoc), so it can never remove a different attempt's pin even under a
            // genuinely concurrent, different-source clone() call racing for the same target --
            // unlike lineage/manifest below, there is no ambiguity to guard against here at all.
            try {
                sourcePinRegistry.removePin(sourceIndexUuid, sourceShardId, pin);
            } catch (IOException | RuntimeException rollbackFailure) {
                e.addSuppressed(rollbackFailure);
            }
            // Rolls back the manifest unconditionally once manifestWritten is true: since
            // writeManifest is write-once (same as writeLineage), this call succeeding at all means
            // no one else's content could be at that exact name, so nothing else could need
            // protecting from this delete. Unlike lineage below, there is no exposed "clear this
            // manifest and let something else immediately reuse the same (term, generation)"
            // primitive in normal operation, so the narrow re-read-and-compare guard lineage needs
            // doesn't apply here.
            if (manifestWritten) {
                try {
                    targetManifestStore.deleteManifests(java.util.List.of(targetManifest));
                } catch (IOException | RuntimeException rollbackFailure) {
                    e.addSuppressed(rollbackFailure);
                }
            }
            // Same "run only if it definitely happened" reasoning as the manifest above, but the
            // undo itself is the caller's own responsibility (see this method's own javadoc for
            // rollbackBeforeActivation): this method has no way to inspect what an opaque
            // beforeActivation callback actually wrote.
            if (beforeActivationRan && rollbackBeforeActivation != null) {
                try {
                    rollbackBeforeActivation.run();
                } catch (IOException | RuntimeException rollbackFailure) {
                    e.addSuppressed(rollbackFailure);
                }
            }
            // Re-reads the lineage rather than blindly deleting the one just written, and only rolls
            // back if it still names exactly this attempt's source. writeLineage itself is write-once
            // (its own writeBlobAtomic call fails outright if the blob already exists -- it never
            // silently overwrites), so an ordinary concurrent clone() call racing for the same target
            // cannot have replaced this attempt's lineage out from under it. The only way the record
            // here could be someone else's is an explicit deleteLineage (e.g. an operator-triggered
            // deleteClone) followed by a different attempt's write, both landing in the narrow window
            // between this write and this catch -- astronomically unlikely, but the guard costs one
            // extra read and removes any doubt: this attempt's failure must never touch state it can
            // positively tell is no longer its own.
            if (lineageWritten) {
                try {
                    Optional<CloneLineage> currentLineage = targetLineageStore.readLineage();
                    if (currentLineage.isPresent()
                        && currentLineage.get().sourceIndexUuid().equals(sourceIndexUuid)
                        && currentLineage.get().sourceShardId() == sourceShardId) {
                        targetLineageStore.deleteLineage();
                    }
                } catch (IOException | RuntimeException rollbackFailure) {
                    e.addSuppressed(rollbackFailure);
                }
            }
            throw e;
        }
    }

    /**
     * The {@link PinRecord#pinId()} a clone of {@code (targetIndexUuid, targetShardId)} registers
     * on its source -- stable and derivable from the target's identity alone, so {@link
     * #deleteClone} can remove exactly this pin without needing anything separate recorded on the
     * target beyond {@link CloneLineage} itself.
     *
     * @param targetIndexUuid the clone's index.
     * @param targetShardId the shard number within {@code targetIndexUuid}.
     */
    public static String clonePinId(String targetIndexUuid, int targetShardId) {
        return "clone:" + targetIndexUuid + ":" + targetShardId;
    }

    /**
     * Releases exactly the pin {@link #clone} placed on the source shard for this clone, using
     * {@code targetIndexUuid}/{@code targetShardId}'s own {@link CloneLineage} to find the source
     * without the caller needing to already know it. A no-op if {@code targetLineageStore} has no
     * lineage recorded (the target was never a clone, or its lineage was already deleted by a
     * previous call) -- idempotent, safe to call more than once or speculatively.
     *
     * <p>Does not touch the target's own manifest or head -- deleting <em>those</em> (the actual
     * index deletion) is the caller's separate responsibility; this method only ever releases the
     * source-side pin and the lineage record that pointed to it. Deliberately not wired to any
     * automatic index-deletion hook yet -- see this class's own javadoc.
     *
     * @param targetIndexUuid the (possibly former) clone's index.
     * @param targetShardId the shard number within {@code targetIndexUuid}.
     * @param targetLineageStore where {@code targetIndexUuid}/{@code targetShardId}'s {@link CloneLineage} would be recorded, if any.
     * @param sourcePinRegistryResolver given the lineage's {@code (sourceIndexUuid, sourceShardId)},
     *                                  returns that source shard's own {@link DurablePinRegistry}
     *                                  -- a resolver rather than a direct parameter because, unlike
     *                                  {@link #clone} (where the caller already has the source
     *                                  shard open), a caller invoking this later, out of band, only
     *                                  knows the target and must construct the source's registry
     *                                  from whatever lineage turns out to say.
     */
    public static void deleteClone(
        String targetIndexUuid,
        int targetShardId,
        BlobContainerCloneLineageStore targetLineageStore,
        java.util.function.BiFunction<String, Integer, DurablePinRegistry> sourcePinRegistryResolver
    ) throws IOException {
        Optional<CloneLineage> lineage = targetLineageStore.readLineage();
        if (lineage.isEmpty()) {
            return;
        }
        DurablePinRegistry sourcePinRegistry = sourcePinRegistryResolver.apply(
            lineage.get().sourceIndexUuid(),
            lineage.get().sourceShardId()
        );
        sourcePinRegistry.removePin(
            lineage.get().sourceIndexUuid(),
            lineage.get().sourceShardId(),
            clonePinId(targetIndexUuid, targetShardId)
        );
        targetLineageStore.deleteLineage();
    }

    /** A defensive bound on how many hops {@link #resolveLineageChain} will follow, guarding against a corrupted/cyclic lineage record. */
    private static final int MAX_LINEAGE_CHAIN_DEPTH = 32;

    /**
     * Walks a shard's {@link CloneLineage} chain as far back as it goes, returning every container
     * in the chain in read-priority order: {@code startContainer} first, then its immediate clone
     * source, then that source's own clone source, and so on until a shard with no lineage record
     * (a genuine original, never itself a clone) is reached.
     *
     * <p>Needed because a clone's lineage only ever records its immediate parent -- nothing stops
     * cloning from an already-cloned shard (a "clone of a clone"), and neither {@code
     * org.opensearch.serverless.storage.resharding.ShardSplitter#split} (which reuses this same
     * {@link #clone} method) refuses it either. A caller that only checked one hop of lineage,
     * the way this class's read-path callers used to,
     * would silently fail to resolve a bundle that only physically exists in the *original*
     * shard -- two or more clone hops back, not just one -- throwing {@code NoSuchFileException}
     * on a read that a fully-chained fallback would have served correctly.
     *
     * @param startContainer the shard's own container, tried first.
     * @param startIndexUuid the shard's own index UUID, to read its lineage from.
     * @param startShardId the shard's own shard number.
     * @param containerResolver given a (indexUuid, shardId), resolves that shard's own {@link BlobContainer}.
     * @return every container in the chain, {@code startContainer} first; always at least one element.
     * @throws IOException if reading any lineage record along the way fails, or the chain exceeds
     *                      {@link #MAX_LINEAGE_CHAIN_DEPTH} hops (a corrupted or cyclic lineage
     *                      record -- lineage is otherwise write-once per shard and can never
     *                      legitimately form a cycle).
     */
    public static List<BlobContainer> resolveLineageChain(
        BlobContainer startContainer,
        String startIndexUuid,
        int startShardId,
        ContainerResolver containerResolver
    ) throws IOException {
        List<BlobContainer> chain = new ArrayList<>();
        chain.add(startContainer);
        BlobContainer currentContainer = startContainer;
        for (int hop = 0; hop < MAX_LINEAGE_CHAIN_DEPTH; hop++) {
            Optional<CloneLineage> lineage = new BlobContainerCloneLineageStore(currentContainer).readLineage();
            if (lineage.isEmpty()) {
                return chain;
            }
            currentContainer = containerResolver.resolve(lineage.get().sourceIndexUuid(), lineage.get().sourceShardId());
            chain.add(currentContainer);
        }
        throw new IOException(
            "clone lineage chain starting at "
                + startIndexUuid
                + "/"
                + startShardId
                + " exceeded "
                + MAX_LINEAGE_CHAIN_DEPTH
                + " hops -- refusing to follow a likely-corrupted or cyclic lineage record"
        );
    }

    /** Resolves a shard's own {@link BlobContainer} -- {@code java.util.function.BiFunction} can't be used here since resolution does real I/O and throws {@link IOException}. */
    @FunctionalInterface
    public interface ContainerResolver {
        /**
         * @param indexUuid the index UUID to resolve.
         * @param shardId the shard number within {@code indexUuid}.
         */
        BlobContainer resolve(String indexUuid, int shardId) throws IOException;
    }
}
