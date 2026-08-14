/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Plan item E5 (plan-100m-index-implementation.md, Area E; C3b in rfc-manifest-sharding-design.md): the
 * pure decision of which manifest shards a given cluster-state version actually needs to rewrite, kept
 * free of any blob-store I/O so it is testable without a repository.
 *
 * <p>The four-step algorithm the RFC's "Write path (C3)" section describes: partition every currently
 * live index by {@link ManifestShardFunction#shardFor}, mark a shard dirty if any index that changed or
 * was deleted this version hashes into it (or if the declared shard count itself changed, in which case
 * every shard is dirty -- see {@link #plan}'s own doc), rewrite only the dirty shards, and carry every
 * other shard forward by referencing its existing blob name verbatim. That last part is where the whole
 * saving comes from: an unchanged shard costs one map lookup here, not a re-upload.
 */
final class IndexMetadataManifestSharder {

    private IndexMetadataManifestSharder() {}

    /**
     * The result of {@link #plan}: which existing shard references survive unchanged, and which shard
     * ids need a fresh blob written with the given content before a {@link UploadedManifestShard} for
     * them can be added to the new manifest.
     */
    static final class Plan {
        private final List<UploadedManifestShard> carriedForward;
        private final Map<Integer, List<UploadedIndexMetadata>> shardsToWrite;

        Plan(List<UploadedManifestShard> carriedForward, Map<Integer, List<UploadedIndexMetadata>> shardsToWrite) {
            this.carriedForward = Collections.unmodifiableList(carriedForward);
            this.shardsToWrite = Collections.unmodifiableMap(shardsToWrite);
        }

        /** Shard references reused verbatim from the previous manifest -- nothing to write for these. */
        List<UploadedManifestShard> getCarriedForward() {
            return carriedForward;
        }

        /**
         * Shard id to the full, current content that shard must hold. The caller writes each of these
         * as a blob and turns the result into a {@link UploadedManifestShard} for the new manifest --
         * this class does not perform I/O, so it cannot produce those references itself.
         */
        Map<Integer, List<UploadedIndexMetadata>> getShardsToWrite() {
            return shardsToWrite;
        }
    }

    /**
     * @param currentIndices every index entry that belongs in the manifest being built, post-update and
     *                       post-deletion -- the same list a today's unsharded manifest would put
     *                       directly into {@code indices}.
     * @param changedOrDeletedIndexUUIDs the UUIDs of indices that were newly uploaded, updated, or
     *                                   deleted this cluster-state version. A deleted index's UUID
     *                                   belongs here even though it no longer appears in
     *                                   {@code currentIndices} -- that is what tells this method its old
     *                                   shard needs rewriting without that entry, rather than being
     *                                   carried forward stale.
     * @param previousShards the previous manifest's shard references, or empty if there is none (a
     *                       fresh cluster, or the first version written since sharding was turned on).
     * @param previousShardCount the shard count the previous manifest was partitioned under, or 0 if
     *                           there is no previous sharded manifest. A mismatch with {@code
     *                           shardCount} means every existing shard reference is meaningless under
     *                           the new partitioning -- rendezvous-style "minimal reshuffling" is
     *                           deliberately not attempted here (see {@code rfc-manifest-sharding-design.md}'s
     *                           "Shard count" section: the count is fixed and not meant to change under
     *                           normal operation), so a change degrades safely to a full rewrite rather
     *                           than silently mixing two partitionings.
     * @param shardCount the shard count this new manifest declares; must be positive.
     */
    static Plan plan(
        List<UploadedIndexMetadata> currentIndices,
        Set<String> changedOrDeletedIndexUUIDs,
        List<UploadedManifestShard> previousShards,
        int previousShardCount,
        int shardCount
    ) {
        Objects.requireNonNull(currentIndices, "currentIndices must not be null");
        Objects.requireNonNull(changedOrDeletedIndexUUIDs, "changedOrDeletedIndexUUIDs must not be null");
        Objects.requireNonNull(previousShards, "previousShards must not be null");
        if (shardCount <= 0) {
            throw new IllegalArgumentException("manifest shard count must be positive, got [" + shardCount + "]");
        }

        Map<Integer, List<UploadedIndexMetadata>> byShard = new HashMap<>();
        for (UploadedIndexMetadata index : currentIndices) {
            int shardId = ManifestShardFunction.shardFor(index.getIndexUUID(), shardCount);
            byShard.computeIfAbsent(shardId, unused -> new ArrayList<>()).add(index);
        }

        boolean shardCountChanged = previousShardCount != shardCount;
        Set<Integer> dirtyShardIds = new HashSet<>();
        if (shardCountChanged) {
            // Every partition's membership is suspect under a different shard count -- an index that
            // hashed into shard 3 under 64 shards can hash anywhere under 256. Nothing can be safely
            // carried forward, so everything with current content is rewritten this one time.
            dirtyShardIds.addAll(byShard.keySet());
        } else {
            for (String indexUUID : changedOrDeletedIndexUUIDs) {
                dirtyShardIds.add(ManifestShardFunction.shardFor(indexUUID, shardCount));
            }
        }

        Map<Integer, UploadedManifestShard> previousByShardId = previousShards.stream()
            .collect(Collectors.toMap(UploadedManifestShard::getShardId, Function.identity()));

        List<UploadedManifestShard> carriedForward = new ArrayList<>();
        Map<Integer, List<UploadedIndexMetadata>> shardsToWrite = new HashMap<>();

        for (Map.Entry<Integer, List<UploadedIndexMetadata>> entry : byShard.entrySet()) {
            int shardId = entry.getKey();
            UploadedManifestShard previous = shardCountChanged ? null : previousByShardId.get(shardId);
            if (dirtyShardIds.contains(shardId) || previous == null) {
                // previous == null with the shard not flagged dirty means a shard gained content this
                // version without any of its indices appearing in changedOrDeletedIndexUUIDs -- that
                // should not happen if the caller's changed-set is accurate, but writing it rather than
                // silently carrying forward nothing is the safe direction to be wrong in: a missed
                // "dirty" costs an extra write, a wrongly-skipped one loses index entries.
                shardsToWrite.put(shardId, entry.getValue());
            } else {
                carriedForward.add(previous);
            }
        }
        // A previous shard whose entire partition is empty now (every index that hashed into it was
        // deleted) simply never appears in byShard, so it is dropped from both lists here -- correctly:
        // the new manifest references nothing there, and the old blob becomes cleanup-eligible once the
        // manifest versions that still reference it age out, exactly like any other now-unreferenced
        // blob.

        return new Plan(carriedForward, shardsToWrite);
    }
}
