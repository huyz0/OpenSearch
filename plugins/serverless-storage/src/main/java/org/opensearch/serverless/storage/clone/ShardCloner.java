/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.clone;

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
     * @param sourceIndexUuid the index being cloned from; must already have a published manifest.
     * @param targetIndexUuid the brand-new index the clone creates; must not already have a
     *                        published head -- cloning onto an already-active shard is refused,
     *                        the same put-if-absent contract {@link ShardStateStore#compareAndSet}
     *                        already gives every first activation.
     * @param nowMillis the target manifest's {@link CommitManifest#createdAtMillis()} -- passed in
     *                  rather than read internally so this class stays trivially deterministic to
     *                  test.
     * @throws IOException if the source has no published manifest, or the target already does
     *                      (both refusals are safe to retry after the caller resolves them, and
     *                      neither leaves the target in a half-written state -- the pin added on
     *                      the source before either check's target-side failure is harmless
     *                      extra retention, not a correctness problem, exactly as {@code
     *                      DurablePinRegistry#addPin}'s own idempotency is designed for).
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
        Optional<VersionedShardHead> sourceHead = sourceShardStateStore.get(sourceIndexUuid, sourceShardId);
        if (sourceHead.isEmpty() || sourceHead.get().head().latestManifestGeneration() == 0) {
            throw new IOException("source shard " + sourceIndexUuid + "/" + sourceShardId + " has no published manifest to clone from");
        }
        ShardHead head = sourceHead.get().head();
        CommitManifest sourceManifest = sourceManifestStore.readManifest(head.primaryTerm(), head.latestManifestGeneration());

        // Pin before publishing anything that could ever be read as referencing it -- a crash or
        // failure after this line just leaves an unused pin (see class javadoc), never a published
        // reference to an unprotected generation.
        sourcePinRegistry.addPin(
            sourceIndexUuid,
            sourceShardId,
            new PinRecord(clonePinId(targetIndexUuid, targetShardId), sourceManifest.primaryTerm(), sourceManifest.generation())
        );
        // Lineage before the head CAS below makes the clone visible/active -- so whenever a clone
        // is visible, deleteClone can already find its way back to the pin it must remove.
        targetLineageStore.writeLineage(new CloneLineage(sourceIndexUuid, sourceShardId));

        CommitManifest targetManifest = new CommitManifest(
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
    }

    /**
     * The {@link PinRecord#pinId()} a clone of {@code (targetIndexUuid, targetShardId)} registers
     * on its source -- stable and derivable from the target's identity alone, so {@link
     * #deleteClone} can remove exactly this pin without needing anything separate recorded on the
     * target beyond {@link CloneLineage} itself.
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
}
