/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

/**
 * Answers whether a node is still alive, without asking its shard-heads.
 *
 * <p>This is the seam that makes {@code rfc-serverless-metadata-plane.md} §7's batching possible:
 * <em>"batch to one lease object per node with shard-heads referencing the node lease"</em>. Phase 8
 * measured why it matters — a steady-state reconciliation tick cost 3 object-store operations per
 * shard, of which the single write was a per-shard lease renewal. A node holding a hundred shards paid
 * a hundred writes per TTL to say one thing: that it was still there.
 *
 * <p>With an oracle, a shard-head stops carrying its own expiry and liveness becomes a property of its
 * owner. One lease renewal covers every shard the node holds.
 *
 * <p>The ephemeral id is part of the question on purpose. A node that restarted has the same node id
 * and a new ephemeral id, and it is emphatically not the same process that took the shard — treating it
 * as alive would hand ownership back to something with no idea it ever had it.
 */
@FunctionalInterface
public interface LivenessOracle {

    /**
     * Reports whether the named node is currently alive.
     *
     * @param nodeId the node's stable id
     * @param ephemeralId the id it had when it took the shard, or null if unknown
     * @return true when that exact process is still holding a live lease
     */
    boolean isLive(String nodeId, String ephemeralId);
}
