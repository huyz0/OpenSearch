/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.allocation;

import org.opensearch.cluster.metadata.IndexMetadata;

import java.util.HashMap;
import java.util.Map;

/**
 * Reads/writes, per reader (search-only) shard, the node id that most recently had a live {@code
 * ObjectStoreReaderEngine} for it plus when that was recorded -- stored as ordinary {@link
 * IndexMetadata} custom data (rfc-serverless-opensearch.md &sect;10, "cost model / cache-locality
 * hysteresis"), the same free extension point {@link SuspendedShardsMetadata} already uses.
 *
 * <p>This is the durable half of the cache-locality signal: a reader shard's lazy bundle
 * {@code Directory} (&sect;7.2) keeps a node-local LRU block cache that is only warm on the node
 * that most recently served queries for it. When that shard later becomes unassigned (a node
 * restart, a transient failure, scale-to-zero reactivation) core has to pick a node for it again;
 * reallocating it to a cold node throws that warm cache away for no reason. {@link
 * ServerlessStorageExistingShardsAllocator} is the consumer -- it prefers the node recorded here,
 * when that node is still decider-approved and the record hasn't gone stale, over an arbitrary
 * first-approved node.
 *
 * <p><b>Staleness, not permanence, is the point.</b> A node id recorded here is only ever a
 * <em>preference</em>, never a requirement -- {@link #isAffinityFresh} makes old records
 * (recorded-node has been gone long enough that its cache is presumed cold anyway, or simply
 * hasn't been refreshed by a real shard start in a while) stop being honored, so a permanently
 * departed node can never pin a shard's allocation forever.
 */
public final class ReaderCacheAffinityMetadata {

    private ReaderCacheAffinityMetadata() {}

    /** The {@link IndexMetadata} custom-data key the preferred node id per shard is stored under. */
    public static final String NODE_CUSTOM_TYPE = "serverless_storage_reader_cache_affinity_node";

    /** The {@link IndexMetadata} custom-data key the recorded-at timestamp per shard is stored under. */
    public static final String RECORDED_AT_CUSTOM_TYPE = "serverless_storage_reader_cache_affinity_recorded_at";

    /**
     * Minimum real time between two recorded-at rewrites for the same (shard, node) pair -- see
     * {@link #withShardCacheAffinity}'s own javadoc for why a fixed interval, not an exact
     * timestamp comparison, is what actually makes repeated same-node calls a genuine no-op.
     */
    private static final long MIN_REWRITE_INTERVAL_MILLIS = 60_000L;

    /**
     * The node id most recently recorded as having a live reader engine for {@code shardId}, or
     * {@code null} if none has ever been recorded.
     *
     * @param indexMetadata the index to check.
     * @param shardId the shard number within {@code indexMetadata}.
     */
    public static String preferredNodeId(IndexMetadata indexMetadata, int shardId) {
        Map<String, String> custom = indexMetadata.getCustomData(NODE_CUSTOM_TYPE);
        return custom == null ? null : custom.get(Integer.toString(shardId));
    }

    /**
     * Whether {@code shardId}'s recorded cache-affinity node (if any) should still be honored --
     * {@code true} only when a node id is on record and it was recorded no longer than {@code
     * ttlMillis} ago. A shard with no record at all is never "fresh."
     *
     * @param indexMetadata the index to check.
     * @param shardId the shard number within {@code indexMetadata}.
     * @param nowMillis the current time, passed in rather than read internally so this stays trivially deterministic to test.
     * @param ttlMillis how long a record remains honorable after being recorded; non-positive disables affinity entirely.
     */
    public static boolean isAffinityFresh(IndexMetadata indexMetadata, int shardId, long nowMillis, long ttlMillis) {
        if (ttlMillis <= 0 || preferredNodeId(indexMetadata, shardId) == null) {
            return false;
        }
        Map<String, String> custom = indexMetadata.getCustomData(RECORDED_AT_CUSTOM_TYPE);
        String value = custom == null ? null : custom.get(Integer.toString(shardId));
        if (value == null) {
            return false;
        }
        long recordedAt = Long.parseLong(value);
        return nowMillis - recordedAt <= ttlMillis;
    }

    /**
     * Returns a copy of {@code indexMetadata} with {@code shardId}'s cache-affinity node set to
     * {@code nodeId}, stamped {@code nowMillis}. A no-op (returns {@code indexMetadata} unchanged)
     * if {@code nodeId} is already the recorded node and it was recorded less than {@link
     * #MIN_REWRITE_INTERVAL_MILLIS} ago, so repeated calls for the same steady-state node -- e.g.
     * a reader shard flapping between suspend/reactivate on the same node -- don't churn cluster
     * state on every single call.
     *
     * <p><b>A fixed interval, not "is the new timestamp older/equal," is the point.</b> An earlier
     * version of this guard compared the new timestamp against the existing one with {@code >=},
     * intending to skip real steady-state repeats -- but since callers always pass a freshly
     * captured, strictly later "now," that comparison was true only for out-of-order/stale calls,
     * never for the genuine repeated-same-node case the javadoc claimed to optimize, so it was
     * dead code in practice: caught by code review, confirmed by the test suite only ever
     * exercising the out-of-order case. This interval-based guard is what a real steady-state
     * caller actually hits, while still refreshing the timestamp (and so the freshness window)
     * often enough that a long-lived, repeatedly-reactivated-on-the-same-node shard never goes
     * stale between real reactivations more than a minute apart.
     *
     * @param indexMetadata the index to update.
     * @param shardId the shard number to record affinity for.
     * @param nodeId the node id to record.
     * @param nowMillis the timestamp to stamp as this record's time.
     */
    public static IndexMetadata withShardCacheAffinity(IndexMetadata indexMetadata, int shardId, String nodeId, long nowMillis) {
        String existingNode = preferredNodeId(indexMetadata, shardId);
        Map<String, String> recordedAtCustom = indexMetadata.getCustomData(RECORDED_AT_CUSTOM_TYPE);
        String existingRecordedAtValue = recordedAtCustom == null ? null : recordedAtCustom.get(Integer.toString(shardId));
        if (nodeId.equals(existingNode) && existingRecordedAtValue != null) {
            long millisSinceLastRecorded = nowMillis - Long.parseLong(existingRecordedAtValue);
            if (millisSinceLastRecorded > -MIN_REWRITE_INTERVAL_MILLIS && millisSinceLastRecorded < MIN_REWRITE_INTERVAL_MILLIS) {
                return indexMetadata;
            }
        }
        Map<String, String> updatedNodes = new HashMap<>(asStringMap(indexMetadata, NODE_CUSTOM_TYPE));
        updatedNodes.put(Integer.toString(shardId), nodeId);
        Map<String, String> updatedRecordedAt = new HashMap<>(asStringMap(indexMetadata, RECORDED_AT_CUSTOM_TYPE));
        updatedRecordedAt.put(Integer.toString(shardId), Long.toString(nowMillis));
        return IndexMetadata.builder(indexMetadata)
            .putCustom(NODE_CUSTOM_TYPE, updatedNodes)
            .putCustom(RECORDED_AT_CUSTOM_TYPE, updatedRecordedAt)
            .build();
    }

    private static Map<String, String> asStringMap(IndexMetadata indexMetadata, String customType) {
        Map<String, String> custom = indexMetadata.getCustomData(customType);
        return custom == null ? Map.of() : custom;
    }
}
