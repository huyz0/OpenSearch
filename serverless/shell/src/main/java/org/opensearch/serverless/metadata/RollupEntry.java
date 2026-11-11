/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.index.query.QueryBuilder;

import java.util.List;

/**
 * What a bucket knows of one index.
 *
 * @param uuid the incarnation the entry was written for
 * @param shards the incarnation's shard count
 * @param states one per shard
 */
public record RollupEntry(String uuid, int shards, List<RollupShardState> states, boolean alias) {

    /**
     * An index's entry.
     *
     * @param uuid the incarnation the entry was written for
     * @param shards the incarnation's shard count
     * @param states one per shard
     */
    public RollupEntry(String uuid, int shards, List<RollupShardState> states) {
        this(uuid, shards, states, false);
    }

    /**
     * Whether this entry settles that the index may match: every shard unowned and published, and not all of
     * them ruled out. A shard's own digest is never wider than its entry, so reading it could rule out nothing
     * more -- the per-shard check can be skipped.
     *
     * @param query the query
     * @param nowMillis the request's single now
     * @return true when the entry alone has decided the index is to be searched
     */
    public boolean judgedMatchable(QueryBuilder query, long nowMillis) {
        if (alias || states.size() != shards) {
            return false;
        }
        for (RollupShardState state : states) {
            if (state.ownerTerm() != 0L || state.digest() == null) {
                return false;
            }
        }
        return rulesOut(query, nowMillis) == false;
    }

    /**
     * Whether this entry proves the query can match nothing the index holds.
     *
     * @param query the query
     * @param nowMillis the request's single now
     * @return true only when every shard is unowned, published and ruled out by its digest
     */
    public boolean rulesOut(QueryBuilder query, long nowMillis) {
        // An alias resolves to indices the pattern may not name; what they hold is not here to judge.
        if (alias || states.size() != shards) {
            return false;
        }
        for (RollupShardState state : states) {
            if (state.ownerTerm() != 0L || state.digest() == null || state.digest().canMatch(query, nowMillis)) {
                return false;
            }
        }
        return true;
    }
}
