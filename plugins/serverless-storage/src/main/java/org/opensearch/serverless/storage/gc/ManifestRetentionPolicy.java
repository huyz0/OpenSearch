/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import org.opensearch.serverless.storage.manifest.CommitManifest;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Decides which manifests <em>of a single shard</em> are safe to delete, per the rules in
 * rfc-serverless-opensearch.md &sect;6.5: a manifest is deletable only when (a) a newer manifest
 * exists, (b) its retention window has passed, (c) no lease pins it, and (d) no durable pin
 * (snapshot/PITR) names it. All four must hold; the current-latest manifest can therefore never
 * be deleted (nothing is newer than it), independent of every other condition.
 *
 * <h2>Why "newer" must mean the head, and never a blob listing</h2>
 *
 * The four-argument overloads below derive "latest" by taking the maximum over the manifests they were
 * handed, which is a listing. That is <b>not</b> the RFC's rule and it is not safe to delete on, because a
 * manifest blob is written <em>before</em> the head compare-and-swap that makes it real
 * ({@code ObjectStoreCommitPublisher#publishCommit} writes the bundle then the manifest;
 * {@code ObjectStoreCommitHeadPublisher} only then CASes the head). A writer killed in that window leaves a
 * manifest blob that was never published. Judged by listing, that orphan <em>is</em> the latest -- so the
 * shard's genuine live head becomes "superseded", ages past retention, and is deleted along with every
 * bundle only it referenced. The shard then cannot open, and its segments are gone: an ordinary crash
 * during publish turned into unrecoverable data loss.
 *
 * <p>The five-argument overloads take the head's own {@link ManifestId} as the anchor instead, which is the
 * only value that actually means "published". Against a head anchor:
 *
 * <ul>
 * <li>a manifest strictly older than the head is genuinely superseded and follows the ordinary four
 *     conditions;</li>
 * <li>the head itself is retained unconditionally -- deleting the live head is never correct at any age;</li>
 * <li>a manifest strictly <em>newer</em> than the head is an unpublished orphan. It is not "latest" and must
 *     never be treated as one; it is retained until it is far older than the ordinary window (see
 *     {@code orphanRetentionCutoffMillis}), because the writer that wrote it may still be alive and about to
 *     CAS the head onto it, and because its bundles are exactly the bundles that same publish is about to
 *     make live.</li>
 * </ul>
 *
 * <p>The four-argument overloads are kept for read-only reporting ({@code TransportShardRetentionStatsAction})
 * and for tests that reason about the policy in isolation. Nothing that deletes may use them.
 */
public final class ManifestRetentionPolicy {

    private ManifestRetentionPolicy() {}

    /**
     * Computes which manifests of one shard must be retained under the four-condition rule (see class javadoc).
     *
     * @param manifestsForOneShard all known manifests belonging to exactly one (index, shard) &mdash;
     *                             mixing shards is a caller bug and rejected defensively.
     * @param retentionCutoffMillis manifests created strictly before this instant have passed
     *                              their retention window (condition b); pass {@code Long.MAX_VALUE}
     *                              to disable time-based retention entirely.
     * @param leasePinnedManifests manifests pinned by a live lease (open searchers, PIT/scroll
     *                             contexts &mdash; condition c).
     * @param durablyPinnedManifests manifests pinned by an explicit retention record (snapshot or
     *                               PITR policy &mdash; condition d).
     * @return the manifests that must be retained (i.e. the complement of what's deletable).
     */
    public static List<CommitManifest> computeRetainedManifests(
        List<CommitManifest> manifestsForOneShard,
        long retentionCutoffMillis,
        Set<ManifestId> leasePinnedManifests,
        Set<ManifestId> durablyPinnedManifests
    ) {
        if (manifestsForOneShard.isEmpty()) {
            return List.of();
        }
        requireSingleShard(manifestsForOneShard);

        CommitManifest latest = findLatest(manifestsForOneShard);

        List<CommitManifest> retained = new ArrayList<>();
        for (CommitManifest manifest : manifestsForOneShard) {
            if (manifest == latest || !isDeletable(manifest, latest, retentionCutoffMillis, leasePinnedManifests, durablyPinnedManifests)) {
                retained.add(manifest);
            }
        }
        return retained;
    }

    /**
     * The complement of {@link #computeRetainedManifests}: manifests safe to delete right now.
     *
     * @param manifestsForOneShard every known manifest for exactly one shard; mixing shards is a caller bug and rejected defensively.
     * @param retentionCutoffMillis manifests created strictly before this instant have passed their retention window.
     * @param leasePinnedManifests manifests pinned by a live lease.
     * @param durablyPinnedManifests manifests pinned by an explicit retention record.
     * @return the manifests safe to delete right now.
     */
    public static List<CommitManifest> computeDeletableManifests(
        List<CommitManifest> manifestsForOneShard,
        long retentionCutoffMillis,
        Set<ManifestId> leasePinnedManifests,
        Set<ManifestId> durablyPinnedManifests
    ) {
        if (manifestsForOneShard.isEmpty()) {
            return List.of();
        }
        requireSingleShard(manifestsForOneShard);

        CommitManifest latest = findLatest(manifestsForOneShard);
        List<CommitManifest> deletable = new ArrayList<>();
        for (CommitManifest manifest : manifestsForOneShard) {
            if (manifest != latest && isDeletable(manifest, latest, retentionCutoffMillis, leasePinnedManifests, durablyPinnedManifests)) {
                deletable.add(manifest);
            }
        }
        return deletable;
    }

    /**
     * The head-anchored form of {@link #computeRetainedManifests(List, long, Set, Set)}: everything that
     * must survive a sweep whose notion of "published" is the shard head, not a listing.
     *
     * @param manifestsForOneShard all known manifests belonging to exactly one (index, shard).
     * @param liveHead the {@code (primaryTerm, generation)} the shard head currently names -- the only
     *                 manifest identity that is known to have been published.
     * @param retentionCutoffMillis manifests created strictly before this instant have passed their window.
     * @param orphanRetentionCutoffMillis the (older) instant an unpublished orphan -- a manifest strictly
     *                                    newer than {@code liveHead} -- must predate before it may be
     *                                    deleted. Must be &lt;= {@code retentionCutoffMillis}.
     * @param leasePinnedManifests manifests pinned by a live lease.
     * @param durablyPinnedManifests manifests pinned by an explicit retention record.
     * @return the manifests that must be retained.
     */
    public static List<CommitManifest> computeRetainedManifests(
        List<CommitManifest> manifestsForOneShard,
        ManifestId liveHead,
        long retentionCutoffMillis,
        long orphanRetentionCutoffMillis,
        Set<ManifestId> leasePinnedManifests,
        Set<ManifestId> durablyPinnedManifests
    ) {
        if (manifestsForOneShard.isEmpty()) {
            return List.of();
        }
        requireSingleShard(manifestsForOneShard);
        List<CommitManifest> retained = new ArrayList<>();
        for (CommitManifest manifest : manifestsForOneShard) {
            if (isDeletableAgainstHead(
                manifest,
                liveHead,
                retentionCutoffMillis,
                orphanRetentionCutoffMillis,
                leasePinnedManifests,
                durablyPinnedManifests
            ) == false) {
                retained.add(manifest);
            }
        }
        return retained;
    }

    /**
     * The head-anchored form of {@link #computeDeletableManifests(List, long, Set, Set)} and the only form
     * anything that actually deletes may call.
     *
     * @param manifestsForOneShard all known manifests belonging to exactly one (index, shard).
     * @param liveHead the {@code (primaryTerm, generation)} the shard head currently names.
     * @param retentionCutoffMillis manifests created strictly before this instant have passed their window.
     * @param orphanRetentionCutoffMillis the (older) instant an unpublished orphan must predate.
     * @param leasePinnedManifests manifests pinned by a live lease.
     * @param durablyPinnedManifests manifests pinned by an explicit retention record.
     * @return the manifests safe to delete right now.
     */
    public static List<CommitManifest> computeDeletableManifests(
        List<CommitManifest> manifestsForOneShard,
        ManifestId liveHead,
        long retentionCutoffMillis,
        long orphanRetentionCutoffMillis,
        Set<ManifestId> leasePinnedManifests,
        Set<ManifestId> durablyPinnedManifests
    ) {
        if (manifestsForOneShard.isEmpty()) {
            return List.of();
        }
        requireSingleShard(manifestsForOneShard);
        List<CommitManifest> deletable = new ArrayList<>();
        for (CommitManifest manifest : manifestsForOneShard) {
            if (isDeletableAgainstHead(
                manifest,
                liveHead,
                retentionCutoffMillis,
                orphanRetentionCutoffMillis,
                leasePinnedManifests,
                durablyPinnedManifests
            )) {
                deletable.add(manifest);
            }
        }
        return deletable;
    }

    private static boolean isDeletableAgainstHead(
        CommitManifest manifest,
        ManifestId liveHead,
        long retentionCutoffMillis,
        long orphanRetentionCutoffMillis,
        Set<ManifestId> leasePinnedManifests,
        Set<ManifestId> durablyPinnedManifests
    ) {
        ManifestId id = ManifestId.of(manifest);
        if (leasePinnedManifests.contains(id) || durablyPinnedManifests.contains(id)) {
            return false;
        }
        if (id.equals(liveHead)) {
            // The live head is never deletable at any age, which is the whole point of anchoring here:
            // the four-argument form could not express this, because to a listing the head is just
            // whichever manifest happened to sort highest.
            return false;
        }
        if (id.isNewerThan(liveHead)) {
            // An unpublished orphan. Deleting it on the ordinary window would race a writer that is
            // simply slow between its manifest write and its head CAS, so it gets a deliberately longer
            // one; until then it is retained, and so are the bundles it references.
            return manifest.createdAtMillis() < orphanRetentionCutoffMillis;
        }
        return manifest.createdAtMillis() < retentionCutoffMillis;
    }

    private static boolean isDeletable(
        CommitManifest manifest,
        CommitManifest latest,
        long retentionCutoffMillis,
        Set<ManifestId> leasePinnedManifests,
        Set<ManifestId> durablyPinnedManifests
    ) {
        boolean newerExists = latest.isNewerThan(manifest);
        boolean pastRetention = manifest.createdAtMillis() < retentionCutoffMillis;
        boolean leasePinned = leasePinnedManifests.contains(ManifestId.of(manifest));
        boolean durablyPinned = durablyPinnedManifests.contains(ManifestId.of(manifest));
        return newerExists && pastRetention && !leasePinned && !durablyPinned;
    }

    private static CommitManifest findLatest(List<CommitManifest> manifests) {
        CommitManifest latest = manifests.get(0);
        for (CommitManifest manifest : manifests) {
            if (manifest.isNewerThan(latest)) {
                latest = manifest;
            }
        }
        return latest;
    }

    private static void requireSingleShard(List<CommitManifest> manifests) {
        String indexUuid = manifests.get(0).indexUuid();
        int shardId = manifests.get(0).shardId();
        for (CommitManifest manifest : manifests) {
            if (!manifest.indexUuid().equals(indexUuid) || manifest.shardId() != shardId) {
                throw new IllegalArgumentException(
                    "computeRetainedManifests/computeDeletableManifests require all manifests to belong to the same "
                        + "(index, shard); got at least ("
                        + indexUuid
                        + ","
                        + shardId
                        + ") and ("
                        + manifest.indexUuid()
                        + ","
                        + manifest.shardId()
                        + ")"
                );
            }
        }
    }
}
