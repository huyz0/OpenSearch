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
 * <p><b>Deliberately out of scope for this first slice</b>: removing the pin when a clone is later
 * deleted (today a clone pin is permanent once created -- an accepted, documented leak until a
 * "delete clone" lifecycle exists), and the lazy-directory (reader-shard) read path, which resolves
 * bundle reads through {@code TransferManager}/{@code LazyBundleIndexInput}, not {@link
 * org.opensearch.serverless.storage.format.BundleFileReader} -- only the eager materializer path
 * (via {@link FallbackBundleFileReader}) is wired for a cloned shard today.
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
     * on its source -- stable and derivable from the target's identity alone, so a caller can look
     * up or (once a "delete clone" lifecycle exists) remove exactly this pin without needing to
     * have recorded it separately.
     */
    public static String clonePinId(String targetIndexUuid, int targetShardId) {
        return "clone:" + targetIndexUuid + ":" + targetShardId;
    }
}
