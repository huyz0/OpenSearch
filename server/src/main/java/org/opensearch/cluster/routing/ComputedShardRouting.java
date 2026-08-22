/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.core.index.shard.ShardId;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Builds the routing entry for a shard whose placement is computed rather than allocated.
 *
 * <p>Both views of a computed shard are built here, and that is the point of the class rather than an
 * incidental convenience. A coordinator asks {@link AbsentIndexRoutingSuppliers#supply} where an index
 * lives and needs a STARTED entry to route to; the data node, opening the same shard on demand through
 * {@code IndicesClusterStateService}, needs an INITIALIZING entry, because that service fails any active
 * shard it does not already have. The two answers differ in state and must agree on everything else.
 *
 * <h2>Why the allocation id cannot be random</h2>
 *
 * {@link ShardRouting#initialize} mints a fresh allocation id when none is given, and an entry rebuilt
 * on every applied cluster state would therefore carry a new one every time. {@code IndexShard} rejects
 * that outright: {@code updateShardState} throws when the new routing is not the same allocation as the
 * current one, and {@code removeShards} tears down a shard whose allocation id has changed. A computed
 * shard would be destroyed and rebuilt on every cluster state update.
 *
 * <p>So the allocation id is derived from the inputs the placement itself is derived from. The same
 * reasoning that lets placement be computed rather than agreed applies to identity: two nodes computing
 * from the same inputs produce the same answer, and there is nothing to publish.
 */
public final class ComputedShardRouting {

    private ComputedShardRouting() {}

    /**
     * A stable allocation id for a computed shard.
     *
     * <p>Base64 of the inputs rather than a hash, because this has to be reversible by a human reading a
     * log line. An allocation id is an opaque string to everything that consumes it, and the cost of a
     * slightly longer one is nothing next to being able to tell which shard a log line is about.
     */
    public static String allocationId(String indexUuid, int shardId, String nodeId) {
        String raw = indexUuid + "/" + shardId + "/" + nodeId;
        return Base64.getUrlEncoder().withoutPadding().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * The entry a data node uses to decide whether to open the shard: INITIALIZING, so that
     * {@code failMissingShards} leaves it alone and {@code createOrUpdateShards} creates it.
     *
     * @param recoverySource never derived. The A5 trap is that a source taken from absent
     *                       {@code inSyncAllocationIds} silently becomes {@code EmptyStoreRecoverySource}
     *                       and a live index recovers blank, so the caller states it.
     */
    public static ShardRouting initializing(ShardId shardId, String nodeId, RecoverySource recoverySource) {
        return ShardRouting.newUnassigned(
            shardId,
            true,
            recoverySource,
            new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, "computed placement")
        )
            .initialize(
                nodeId,
                allocationId(shardId.getIndex().getUUID(), shardId.id(), nodeId),
                ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE
            );
    }

    /** The entry a coordinator routes through: the same shard, already started. */
    public static ShardRouting started(ShardId shardId, String nodeId, RecoverySource recoverySource) {
        return initializing(shardId, nodeId, recoverySource).moveToStarted();
    }
}
