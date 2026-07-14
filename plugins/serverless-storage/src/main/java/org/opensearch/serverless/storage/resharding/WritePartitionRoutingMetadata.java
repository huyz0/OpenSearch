/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.opensearch.cluster.metadata.IndexMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;

/**
 * Reads/writes a split target index's write-routing assignment, stored as ordinary {@link
 * IndexMetadata} custom data -- the same {@code Map<String, String>}-backed extension point {@link
 * org.opensearch.serverless.storage.allocation.SuspendedShardsMetadata} already uses, for the same
 * reason: no new {@code ClusterState.Custom}/diffable type needed, and persistence,
 * cluster-manager-failover survival, and wire/xcontent serialization all come free from core.
 *
 * <p><b>Why this belongs in cluster-state metadata rather than a fresh remote lookup per write</b>:
 * a coordinating node resolving where to route one document's write must not add a blocking remote
 * call to the hot indexing path. Cluster state is already replicated to every node and read
 * synchronously today for ordinary index/alias resolution, so attaching the write-partition
 * assignment there means {@link org.opensearch.serverless.storage.resharding.RoutingPartitionFilter}'s
 * existing document-id hash (already used read-side, to decide which partition a reader shard's
 * document belongs to) can be reused write-side too, entirely locally, with the target-index name
 * to route to already sitting in the same {@code ClusterState} the coordinating node already holds.
 *
 * <p><b>Deliberately separate from {@link ShardPartitionDescriptor}</b>: that record is per-shard,
 * write-once, object-store-resident state that anchors the actual read-time partition filter and
 * physical bundle rewrite. This class is cluster-state-resident and only records which alias a
 * target index's writes should be reachable through and which partition slot it occupies for that
 * alias's write-routing purposes -- the operator (or, eventually, an auto-split controller) is
 * responsible for keeping the two in agreement by naming targets in matching partition order to
 * both {@link org.opensearch.serverless.storage.resharding.action.ShardPartitionRewriteRequest}-style
 * calls and {@link org.opensearch.serverless.storage.resharding.action.EnableWritePartitionRoutingRequest}.
 * Conflating them would mean every write-routing lookup paying the cost of reading per-shard
 * object-store state on the hot path, exactly what this design avoids.
 */
public final class WritePartitionRoutingMetadata {

    private WritePartitionRoutingMetadata() {}

    /** The {@link IndexMetadata} custom-data key this target's owning write-routing alias is stored under. */
    public static final String ALIAS_CUSTOM_TYPE = "serverless_storage_write_routing_alias";

    /** The {@link IndexMetadata} custom-data key this target's partition index is stored under. */
    public static final String PARTITION_INDEX_CUSTOM_TYPE = "serverless_storage_write_routing_partition_index";

    /** The {@link IndexMetadata} custom-data key this target's partition count is stored under. */
    public static final String NUM_PARTITIONS_CUSTOM_TYPE = "serverless_storage_write_routing_num_partitions";

    private static final String ALIAS_MAP_KEY = "alias";
    private static final String PARTITION_INDEX_MAP_KEY = "partition_index";
    private static final String NUM_PARTITIONS_MAP_KEY = "num_partitions";

    /**
     * Returns {@code indexMetadata} with its write-routing assignment set (or overwritten).
     *
     * @param indexMetadata the split-target index to assign.
     * @param aliasName the write-routing alias this target is reachable through.
     * @param partitionIndex which of {@code numPartitions} partitions this target serves.
     * @param numPartitions how many partitions {@code aliasName}'s write-routing is divided into.
     */
    public static IndexMetadata withAssignment(IndexMetadata indexMetadata, String aliasName, int partitionIndex, int numPartitions) {
        IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata);
        builder.putCustom(ALIAS_CUSTOM_TYPE, singleValueMap(ALIAS_MAP_KEY, aliasName));
        builder.putCustom(PARTITION_INDEX_CUSTOM_TYPE, singleValueMap(PARTITION_INDEX_MAP_KEY, Integer.toString(partitionIndex)));
        builder.putCustom(NUM_PARTITIONS_CUSTOM_TYPE, singleValueMap(NUM_PARTITIONS_MAP_KEY, Integer.toString(numPartitions)));
        return builder.build();
    }

    /**
     * Returns {@code indexMetadata} with its write-routing assignment cleared, if it had one -- the
     * rollback primitive {@link org.opensearch.serverless.storage.resharding.action.DisableWritePartitionRoutingAction}
     * uses. Once cleared, {@link #isWriteRoutingTarget} is {@code false} again, and {@code
     * WritePartitionRoutingActionFilter} naturally stops rewriting or fencing against this index --
     * it simply has no assignment left to match.
     *
     * @param indexMetadata the target index to unassign; a no-op (returns the same instance) if
     *                       it carries no write-routing assignment already.
     */
    public static IndexMetadata withoutAssignment(IndexMetadata indexMetadata) {
        if (isWriteRoutingTarget(indexMetadata) == false) {
            return indexMetadata;
        }
        IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata);
        builder.removeCustom(ALIAS_CUSTOM_TYPE);
        builder.removeCustom(PARTITION_INDEX_CUSTOM_TYPE);
        builder.removeCustom(NUM_PARTITIONS_CUSTOM_TYPE);
        return builder.build();
    }

    /** The write-routing alias {@code indexMetadata} is assigned to, or {@code null} if unassigned. */
    public static String writeRoutingAlias(IndexMetadata indexMetadata) {
        Map<String, String> custom = indexMetadata.getCustomData(ALIAS_CUSTOM_TYPE);
        return custom == null ? null : custom.get(ALIAS_MAP_KEY);
    }

    /** Which partition {@code indexMetadata} serves for its write-routing alias, if assigned. */
    public static OptionalInt partitionIndex(IndexMetadata indexMetadata) {
        return readInt(indexMetadata, PARTITION_INDEX_CUSTOM_TYPE, PARTITION_INDEX_MAP_KEY);
    }

    /** How many partitions {@code indexMetadata}'s write-routing alias is divided into, if assigned. */
    public static OptionalInt numPartitions(IndexMetadata indexMetadata) {
        return readInt(indexMetadata, NUM_PARTITIONS_CUSTOM_TYPE, NUM_PARTITIONS_MAP_KEY);
    }

    /** Whether {@code indexMetadata} currently carries a write-routing assignment at all. */
    public static boolean isWriteRoutingTarget(IndexMetadata indexMetadata) {
        return writeRoutingAlias(indexMetadata) != null;
    }

    private static OptionalInt readInt(IndexMetadata indexMetadata, String customType, String mapKey) {
        Map<String, String> custom = indexMetadata.getCustomData(customType);
        if (custom == null) {
            return OptionalInt.empty();
        }
        String value = custom.get(mapKey);
        return value == null ? OptionalInt.empty() : OptionalInt.of(Integer.parseInt(value));
    }

    private static Map<String, String> singleValueMap(String key, String value) {
        Map<String, String> map = new HashMap<>();
        map.put(key, value);
        return map;
    }
}
