/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.allocation;

import org.opensearch.cluster.metadata.IndexMetadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Reads/writes the set of a serverless-storage index's currently-suspended writer shard ids, stored
 * as ordinary {@link IndexMetadata} custom data (rfc-serverless-opensearch.md &sect;7.3) -- the same
 * {@code Map<String, String>}-backed extension point {@code IndexMetadata.Builder#putCustom} already
 * exists for, requiring no new {@code ClusterState.Custom}/diffable type and getting persistence,
 * cluster-manager-failover survival, and wire/xcontent serialization entirely for free from core.
 *
 * <p>Suspension is deliberately whole-index granular for reactivation (see {@code
 * ShardReactivationActionFilter}) but per-shard for the actual suspend decision (see {@code
 * ShardSuspensionCoordinator}) and for {@link SuspendedShardAllocationDecider}'s own check --
 * clearing reads "every suspended shard of this index," matching how a write/search request
 * generally can't cheaply know in advance exactly which shard(s) of an index it will resolve to
 * before core's own routing does that work.
 */
public final class SuspendedShardsMetadata {

    private SuspendedShardsMetadata() {}

    /** The {@link IndexMetadata} custom-data key this class reads/writes under. */
    public static final String CUSTOM_TYPE = "serverless_storage_suspended_shards";

    private static final String SUSPENDED_VALUE = "true";

    /**
     * Whether {@code shardId} of {@code indexMetadata} is currently marked suspended.
     *
     * @param indexMetadata the index to check.
     * @param shardId the shard number within {@code indexMetadata}.
     */
    public static boolean isSuspended(IndexMetadata indexMetadata, int shardId) {
        Map<String, String> custom = indexMetadata.getCustomData(CUSTOM_TYPE);
        return custom != null && SUSPENDED_VALUE.equals(custom.get(Integer.toString(shardId)));
    }

    /**
     * Every shard id of {@code indexMetadata} currently marked suspended; empty if none are.
     *
     * @param indexMetadata the index to check.
     */
    public static Set<Integer> suspendedShardIds(IndexMetadata indexMetadata) {
        Map<String, String> custom = indexMetadata.getCustomData(CUSTOM_TYPE);
        if (custom == null || custom.isEmpty()) {
            return Collections.emptySet();
        }
        Set<Integer> shardIds = new HashSet<>();
        for (String key : custom.keySet()) {
            shardIds.add(Integer.valueOf(key));
        }
        return shardIds;
    }

    /**
     * Returns a copy of {@code indexMetadata} with {@code shardId} added to its suspended set,
     * bumping {@link IndexMetadata#getVersion()} exactly the way any other {@code IndexMetadata}
     * mutation does. A no-op (returns {@code indexMetadata} unchanged) if {@code shardId} is
     * already suspended, so callers don't need to check first.
     *
     * @param indexMetadata the index to update.
     * @param shardId the shard number to mark suspended.
     */
    public static IndexMetadata withShardSuspended(IndexMetadata indexMetadata, int shardId) {
        if (isSuspended(indexMetadata, shardId)) {
            return indexMetadata;
        }
        Map<String, String> updated = new HashMap<>(suspendedAsStringMap(indexMetadata));
        updated.put(Integer.toString(shardId), SUSPENDED_VALUE);
        return IndexMetadata.builder(indexMetadata).putCustom(CUSTOM_TYPE, updated).build();
    }

    /**
     * Returns a copy of {@code indexMetadata} with every shard reactivated (its suspended set
     * cleared). A no-op if nothing was suspended.
     *
     * @param indexMetadata the index to update.
     */
    public static IndexMetadata withAllShardsReactivated(IndexMetadata indexMetadata) {
        if (suspendedShardIds(indexMetadata).isEmpty()) {
            return indexMetadata;
        }
        return IndexMetadata.builder(indexMetadata).putCustom(CUSTOM_TYPE, Collections.emptyMap()).build();
    }

    private static Map<String, String> suspendedAsStringMap(IndexMetadata indexMetadata) {
        Map<String, String> custom = indexMetadata.getCustomData(CUSTOM_TYPE);
        return custom == null ? Collections.emptyMap() : custom;
    }
}
