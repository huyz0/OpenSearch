/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.cluster;

import java.util.Objects;

/**
 * A claim that this node owns one shard, at a given term.
 *
 * <p>This is what a shard-head says after a successful compare-and-swap: who owns the shard, and the
 * term that ownership was acquired at. It is the <em>only</em> thing that may cause a shard to be
 * closed. A projected view that fails to mention a shard says nothing about ownership — see
 * {@code ShardReconciler} and {@code s1-findings.md}.
 */
public final class ShardAssignment {

    private final String indexName;
    private final int shardId;
    private final long term;

    /**
     * Creates an assignment.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @param term the shard-head term this ownership was acquired at; must be positive
     */
    public ShardAssignment(String indexName, int shardId, long term) {
        this.indexName = Objects.requireNonNull(indexName);
        this.shardId = shardId;
        if (term < 1) {
            throw new IllegalArgumentException("shard-head term must be positive, got " + term + " (see s0-findings.md F4)");
        }
        this.term = term;
    }

    /**
     * Returns the index name.
     *
     * @return the index name
     */
    public String indexName() {
        return indexName;
    }

    /**
     * Returns the shard number.
     *
     * @return the shard id
     */
    public int shardId() {
        return shardId;
    }

    /**
     * Returns the term ownership was acquired at.
     *
     * @return the shard-head term
     */
    public long term() {
        return term;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ShardAssignment other) {
            return shardId == other.shardId && term == other.term && indexName.equals(other.indexName);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, shardId, term);
    }

    @Override
    public String toString() {
        return "ShardAssignment[" + indexName + "][" + shardId + "] term=" + term;
    }
}
