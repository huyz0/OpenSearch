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
 */
public final class ManifestRetentionPolicy {

    private ManifestRetentionPolicy() {}

    /**
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

    /** The complement of {@link #computeRetainedManifests}: manifests safe to delete right now. */
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
