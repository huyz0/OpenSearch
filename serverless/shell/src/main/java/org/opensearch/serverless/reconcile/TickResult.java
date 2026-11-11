/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.reconcile;

import org.opensearch.core.index.shard.ShardId;

import java.util.Set;

/** What one reconciliation pass did. */
public final class TickResult {

    private final Set<ShardId> released;
    private final Set<ShardId> activated;
    private final Set<ShardId> published;
    private final int hintCount;

    TickResult(Set<ShardId> released, Set<ShardId> activated, Set<ShardId> published, int hintCount) {
        this.released = Set.copyOf(released);
        this.activated = Set.copyOf(activated);
        this.published = Set.copyOf(published);
        this.hintCount = hintCount;
    }

    /**
     * Returns shards whose commits this pass published to the object store.
     *
     * @return the published shards
     */
    public Set<ShardId> published() {
        return published;
    }

    /**
     * Returns shards released because this node lost them.
     *
     * @return the released shards
     */
    public Set<ShardId> released() {
        return released;
    }

    /**
     * Returns shards newly acquired by this pass.
     *
     * @return the activated shards
     */
    public Set<ShardId> activated() {
        return activated;
    }

    /**
     * Returns how many routing hints are held after this pass.
     *
     * @return the hint count
     */
    public int hintCount() {
        return hintCount;
    }

    @Override
    public String toString() {
        return "TickResult[released="
            + released.size()
            + ", activated="
            + activated.size()
            + ", published="
            + published.size()
            + ", hints="
            + hintCount
            + "]";
    }
}
