/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.reconcile;

import org.opensearch.core.index.shard.ShardId;

/**
 * The events that make reconciliation happen sooner than the clock would.
 *
 * <p>Before this existed, one timer drove everything: leases, publication, activation and hints all
 * moved at whatever cadence the tick ran at. That coupled latencies that have nothing to do with each
 * other. A shard's durability window became the tick interval even though the node knew the instant a
 * write landed; a dead owner went unnoticed for a full interval even though the very next write to that
 * shard proved it.
 *
 * <p>So the drivers split by what actually triggers them:
 *
 * <ul>
 *   <li><b>Lease renewal</b> stays on a timer, because a lease is defined in wall-clock terms and
 *       nothing but the clock can tell you it is about to lapse.</li>
 *   <li><b>Publication</b> is an edge: {@link #wrote} fires when a document lands, and there is no
 *       reason to wait for a timer to learn what the node just did itself.</li>
 *   <li><b>Activation</b> is a failure: {@link #ownershipDoubted} fires when something the system tried
 *       to do proves ownership is wrong — a publish fenced by a newer term, a forward to an owner with
 *       no live lease, a write to a shard nobody holds.</li>
 * </ul>
 *
 * <p>Both edges are <em>hints that make things faster, never the only path</em>. A slow backstop pass
 * still does the full reconciliation, so a dropped or missed signal costs latency and never correctness.
 * That is deliberate: level-triggered convergence with edges as accelerators is the shape that survives
 * lost events, and edge-only reconciliation is the shape that silently wedges.
 */
public interface ReconcileSignals {

    /** Signals that ignore everything; the default when nothing is scheduling. */
    ReconcileSignals NONE = new ReconcileSignals() {
        @Override
        public void wrote(ShardId shardId) {}

        @Override
        public void ownershipDoubted(String indexName, int shardNumber) {}
    };

    /**
     * Reports that a document was applied to a shard this node owns.
     *
     * <p>Called on the write path, so it must not block: the implementation's job is to mark and
     * return, never to publish inline. A publish is an object-store round trip and belongs nowhere near
     * the latency of an acknowledged write.
     *
     * @param shardId the shard written to
     */
    void wrote(ShardId shardId);

    /**
     * Reports evidence that the recorded owner of a shard is not serving it.
     *
     * <p>This is a suspicion, not a finding. The caller has seen something consistent with a stale head
     * — an unreachable owner, an unowned shard, a fenced publish — and the correct response is to go
     * look, not to act. Whatever acts on this still goes through the same compare-and-swap as everyone
     * else, so a wrong suspicion costs one wasted read.
     *
     * @param indexName the index
     * @param shardNumber the shard
     */
    void ownershipDoubted(String indexName, int shardNumber);
}
