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
 * Reads/writes the sets of a serverless-storage index's currently-suspended writer and reader
 * (search-only) shard ids, plus each role's last-reactivation timestamp, stored as ordinary {@link
 * IndexMetadata} custom data (rfc-serverless-opensearch.md &sect;7.3) -- the same {@code
 * Map<String, String>}-backed extension point {@code IndexMetadata.Builder#putCustom} already
 * exists for, requiring no new {@code ClusterState.Custom}/diffable type and getting persistence,
 * cluster-manager-failover survival, and wire/xcontent serialization entirely for free from core.
 *
 * <p>Writer and reader suspension (and reactivation timestamps) are tracked under entirely separate
 * custom-data keys ({@link #WRITER_CUSTOM_TYPE}/{@link #READER_CUSTOM_TYPE} and their {@code
 * *_LAST_REACTIVATED_CUSTOM_TYPE} counterparts), not one role-tagged map: a shard's writer copy and
 * its reader (search-only) copy(ies) are suspended and reactivated completely independently of each
 * other (a busy writer with an idle reader, or vice versa, is the common case, not the exception --
 * see &sect;7.3's own "reader shards scale to zero the same way" phrasing, distinct from writer
 * suspension), so conflating them into one key would make "is shard 0's writer suspended" and "is
 * shard 0's reader suspended" mutually destructive writes under the same {@code IndexMetadata}
 * custom-data entry. The writer suspend key/behavior is unchanged from this class's original,
 * writer-only version -- no wire/back-compat break for a cluster mid-upgrade.
 *
 * <p><b>Last-reactivation timestamps exist to give {@code ShardSuspensionCoordinator} hysteresis</b>
 * (rfc-serverless-opensearch.md &sect;16 Phase 4's "balancer hysteresis" milestone item): without a
 * cooldown, a shard that idles just past the threshold, gets a single request, reactivates, and
 * immediately idles again would suspend and reactivate every single evaluation tick -- real,
 * wasted allocation churn (a reroute + `CancelAllocationCommand` per cycle) for no benefit. {@link
 * #withAllShardsReactivated}/{@link #withAllReaderShardsReactivated} stamp "now" for exactly the
 * shard ids they actually clear, and {@link #isSuspensionAllowed} is the single place that reads it
 * back.
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

    /** The {@link IndexMetadata} custom-data key writer-shard suspension is read/written under. */
    public static final String CUSTOM_TYPE = "serverless_storage_suspended_shards";

    /** Alias for {@link #CUSTOM_TYPE}, named to read clearly alongside {@link #READER_CUSTOM_TYPE}. */
    public static final String WRITER_CUSTOM_TYPE = CUSTOM_TYPE;

    /** The {@link IndexMetadata} custom-data key reader (search-only) shard suspension is read/written under. */
    public static final String READER_CUSTOM_TYPE = "serverless_storage_suspended_reader_shards";

    /** The {@link IndexMetadata} custom-data key writer-shard last-reactivation timestamps are read/written under. */
    public static final String WRITER_LAST_REACTIVATED_CUSTOM_TYPE = "serverless_storage_writer_last_reactivated";

    /** The {@link IndexMetadata} custom-data key reader-shard last-reactivation timestamps are read/written under. */
    public static final String READER_LAST_REACTIVATED_CUSTOM_TYPE = "serverless_storage_reader_last_reactivated";

    private static final String SUSPENDED_VALUE = "true";

    /**
     * Whether {@code shardId}'s writer copy of {@code indexMetadata} is currently marked suspended.
     *
     * @param indexMetadata the index to check.
     * @param shardId the shard number within {@code indexMetadata}.
     */
    public static boolean isSuspended(IndexMetadata indexMetadata, int shardId) {
        return isSuspended(indexMetadata, shardId, WRITER_CUSTOM_TYPE);
    }

    /**
     * Whether {@code shardId}'s reader (search-only) copy of {@code indexMetadata} is currently
     * marked suspended.
     *
     * @param indexMetadata the index to check.
     * @param shardId the shard number within {@code indexMetadata}.
     */
    public static boolean isReaderSuspended(IndexMetadata indexMetadata, int shardId) {
        return isSuspended(indexMetadata, shardId, READER_CUSTOM_TYPE);
    }

    /**
     * Every shard id of {@code indexMetadata} whose writer copy is currently marked suspended;
     * empty if none are.
     *
     * @param indexMetadata the index to check.
     */
    public static Set<Integer> suspendedShardIds(IndexMetadata indexMetadata) {
        return suspendedShardIds(indexMetadata, WRITER_CUSTOM_TYPE);
    }

    /**
     * Every shard id of {@code indexMetadata} whose reader (search-only) copy is currently marked
     * suspended; empty if none are.
     *
     * @param indexMetadata the index to check.
     */
    public static Set<Integer> suspendedReaderShardIds(IndexMetadata indexMetadata) {
        return suspendedShardIds(indexMetadata, READER_CUSTOM_TYPE);
    }

    /**
     * Returns a copy of {@code indexMetadata} with {@code shardId}'s writer copy added to its
     * suspended set, bumping {@link IndexMetadata#getVersion()} exactly the way any other {@code
     * IndexMetadata} mutation does. A no-op (returns {@code indexMetadata} unchanged) if {@code
     * shardId} is already suspended, so callers don't need to check first.
     *
     * @param indexMetadata the index to update.
     * @param shardId the shard number to mark suspended.
     */
    public static IndexMetadata withShardSuspended(IndexMetadata indexMetadata, int shardId) {
        return withShardSuspended(indexMetadata, shardId, WRITER_CUSTOM_TYPE);
    }

    /**
     * Returns a copy of {@code indexMetadata} with {@code shardId}'s reader (search-only) copy
     * added to its suspended set. A no-op if already suspended.
     *
     * @param indexMetadata the index to update.
     * @param shardId the shard number to mark suspended.
     */
    public static IndexMetadata withReaderShardSuspended(IndexMetadata indexMetadata, int shardId) {
        return withShardSuspended(indexMetadata, shardId, READER_CUSTOM_TYPE);
    }

    /**
     * Returns a copy of {@code indexMetadata} with every writer shard reactivated (its suspended
     * set cleared), stamping {@code nowMillis} as the last-reactivation time for exactly the shard
     * ids that were actually cleared. A no-op if nothing was suspended.
     *
     * @param indexMetadata the index to update.
     * @param nowMillis the timestamp to stamp as this reactivation's time.
     */
    public static IndexMetadata withAllShardsReactivated(IndexMetadata indexMetadata, long nowMillis) {
        return withAllShardsReactivated(indexMetadata, WRITER_CUSTOM_TYPE, WRITER_LAST_REACTIVATED_CUSTOM_TYPE, nowMillis);
    }

    /**
     * Returns a copy of {@code indexMetadata} with every reader (search-only) shard reactivated
     * (its suspended set cleared), stamping {@code nowMillis} as the last-reactivation time for
     * exactly the shard ids that were actually cleared. A no-op if nothing was suspended.
     *
     * @param indexMetadata the index to update.
     * @param nowMillis the timestamp to stamp as this reactivation's time.
     */
    public static IndexMetadata withAllReaderShardsReactivated(IndexMetadata indexMetadata, long nowMillis) {
        return withAllShardsReactivated(indexMetadata, READER_CUSTOM_TYPE, READER_LAST_REACTIVATED_CUSTOM_TYPE, nowMillis);
    }

    /**
     * The last time {@code shardId}'s writer (or reader, if {@code reader}) copy of {@code
     * indexMetadata} was reactivated, or {@code -1} if it never has been (including a shard that
     * has never been suspended at all -- there's no meaningful reactivation time to report).
     *
     * @param indexMetadata the index to check.
     * @param shardId the shard number within {@code indexMetadata}.
     * @param reader {@code true} to check the reader copy's timestamp, {@code false} for the writer copy's.
     */
    public static long lastReactivatedAtMillis(IndexMetadata indexMetadata, int shardId, boolean reader) {
        Map<String, String> custom = indexMetadata.getCustomData(
            reader ? READER_LAST_REACTIVATED_CUSTOM_TYPE : WRITER_LAST_REACTIVATED_CUSTOM_TYPE
        );
        if (custom == null) {
            return -1L;
        }
        String value = custom.get(Integer.toString(shardId));
        return value == null ? -1L : Long.parseLong(value);
    }

    /**
     * Whether {@code shardId}'s writer (or reader, if {@code reader}) copy of {@code indexMetadata}
     * is allowed to be suspended right now -- {@code false} only when it was reactivated more
     * recently than {@code cooldownMillis} ago, the hysteresis guard against suspend/reactivate
     * flapping this class's own javadoc describes. A shard never reactivated (or {@code
     * cooldownMillis <= 0}, disabling the guard) is always allowed.
     *
     * @param indexMetadata the index to check.
     * @param shardId the shard number within {@code indexMetadata}.
     * @param reader {@code true} to check the reader copy, {@code false} for the writer copy.
     * @param nowMillis the current time, passed in rather than read internally so this stays trivially deterministic to test.
     * @param cooldownMillis the minimum time that must have passed since the last reactivation.
     */
    public static boolean isSuspensionAllowed(
        IndexMetadata indexMetadata,
        int shardId,
        boolean reader,
        long nowMillis,
        long cooldownMillis
    ) {
        if (cooldownMillis <= 0) {
            return true;
        }
        long lastReactivatedAt = lastReactivatedAtMillis(indexMetadata, shardId, reader);
        return lastReactivatedAt < 0 || nowMillis - lastReactivatedAt >= cooldownMillis;
    }

    private static boolean isSuspended(IndexMetadata indexMetadata, int shardId, String customType) {
        Map<String, String> custom = indexMetadata.getCustomData(customType);
        return custom != null && SUSPENDED_VALUE.equals(custom.get(Integer.toString(shardId)));
    }

    private static Set<Integer> suspendedShardIds(IndexMetadata indexMetadata, String customType) {
        Map<String, String> custom = indexMetadata.getCustomData(customType);
        if (custom == null || custom.isEmpty()) {
            return Collections.emptySet();
        }
        Set<Integer> shardIds = new HashSet<>();
        for (String key : custom.keySet()) {
            shardIds.add(Integer.valueOf(key));
        }
        return shardIds;
    }

    private static IndexMetadata withShardSuspended(IndexMetadata indexMetadata, int shardId, String customType) {
        if (isSuspended(indexMetadata, shardId, customType)) {
            return indexMetadata;
        }
        Map<String, String> updated = new HashMap<>(suspendedAsStringMap(indexMetadata, customType));
        updated.put(Integer.toString(shardId), SUSPENDED_VALUE);
        return IndexMetadata.builder(indexMetadata).putCustom(customType, updated).build();
    }

    private static IndexMetadata withAllShardsReactivated(
        IndexMetadata indexMetadata,
        String suspendedCustomType,
        String lastReactivatedCustomType,
        long nowMillis
    ) {
        Set<Integer> shardIds = suspendedShardIds(indexMetadata, suspendedCustomType);
        if (shardIds.isEmpty()) {
            return indexMetadata;
        }
        Map<String, String> lastReactivated = new HashMap<>(suspendedAsStringMap(indexMetadata, lastReactivatedCustomType));
        for (Integer shardId : shardIds) {
            lastReactivated.put(Integer.toString(shardId), Long.toString(nowMillis));
        }
        return IndexMetadata.builder(indexMetadata)
            .putCustom(suspendedCustomType, Collections.emptyMap())
            .putCustom(lastReactivatedCustomType, lastReactivated)
            .build();
    }

    private static Map<String, String> suspendedAsStringMap(IndexMetadata indexMetadata, String customType) {
        Map<String, String> custom = indexMetadata.getCustomData(customType);
        return custom == null ? Collections.emptyMap() : custom;
    }
}
