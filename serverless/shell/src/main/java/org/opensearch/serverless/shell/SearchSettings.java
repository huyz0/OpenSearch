/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shell;

import org.opensearch.common.settings.Settings;

/**
 * How a node's searches fan out and resolve patterns, read from the node's settings.
 */
public final class SearchSettings {

    private final Settings settings;

    /**
     * Reads search settings from a node's settings.
     *
     * @param settings the node's settings
     */
    SearchSettings(Settings settings) {
        this.settings = settings;
    }

    /**
     * The most indices a prefix pattern may match before the request is refused.
     *
     * <p>Configurable because it is a policy about how much fan-out a caller may ask for in one request,
     * not a fact about the store — and because a default of ten thousand is unreachable in a test, which
     * would leave the refusal path unexercised.
     *
     * @return the cap
     */
    public int patternCap() {
        return settings.getAsInt(
            "serverless.search.pattern.max_indices",
            org.opensearch.serverless.metadata.MetadataPlane.DEFAULT_PATTERN_CAP
        );
    }

    /** How many shards a search works on at once by default. */
    public static final int DEFAULT_SEARCH_FANOUT_CONCURRENCY = 16;

    /**
     * How many shards a search fans out to at once: the width of its sliding window.
     *
     * <p>A search waits on object-store round trips, not on CPU, so its width is set by how many requests in
     * flight a coordinator should hold rather than by cores. Configurable because that depends on the store.
     *
     * @return the width, at least one
     */
    public int searchFanoutConcurrency() {
        return Math.max(1, settings.getAsInt("serverless.search.fanout.concurrency", DEFAULT_SEARCH_FANOUT_CONCURRENCY));
    }

    /** How many indices a prefix pattern must match before a search reads its group's digest rollups. */
    public static final int DEFAULT_ROLLUP_MIN_INDICES = 32;

    /**
     * Whether a prefix pattern long enough to name one rollup group is resolved from the group's entries rather
     * than by listing every name under the prefix. On by default: a listing costs a page per thousand names, and a
     * store holding a million under one prefix times out before the first page.
     *
     * @return true to resolve from the rollups
     */
    public boolean resolveFromRollups() {
        return settings.getAsBoolean("serverless.search.resolve_from_rollups", true);
    }

    /**
     * How old this node's copy of a rollup group may be and still resolve a pattern. Zero by default -- the group
     * is read for every search -- because a copy that predates an index's activation leaves the index off the
     * search, and a search on its owner would otherwise have seen its refreshed documents. A deployment that
     * accepts that window for fewer reads can set it.
     *
     * @return the age in milliseconds; zero to read the group every time
     */
    public long rollupCacheMillis() {
        return Math.max(0L, settings.getAsLong("serverless.search.rollup.cache_millis", 0L));
    }

    /**
     * How many indices a pattern must match before the digest rollups are consulted: below it, the per-index
     * checks cost less than the group's registers.
     *
     * @return the threshold
     */
    public int rollupMinIndices() {
        return Math.max(1, settings.getAsInt("serverless.search.rollup.min_indices", DEFAULT_ROLLUP_MIN_INDICES));
    }

    /** How many cold shards one search may activate by default. */
    public static final int DEFAULT_SEARCH_ACTIVATION_BUDGET = 1024;

    /**
     * How many shards not open on this node one search may ask for: each may cost a cold open somewhere.
     *
     * <p>A search past it is refused, or with {@code allow_partial_activation} answered from the first shards
     * within it. What the pruning digest skips does not count, so the budget bounds what a query actually
     * wakes, not how many indices its pattern names.
     *
     * @return the budget, at least one
     */
    public int searchActivationBudget() {
        return Math.max(1, settings.getAsInt("serverless.search.max_activations_per_query", DEFAULT_SEARCH_ACTIVATION_BUDGET));
    }

    /**
     * Whether a search skips the shards whose pruning digest rules the query out. On unless
     * {@code serverless.search.prune} says otherwise, which exists to measure what pruning saves.
     *
     * @return true when pruning is on
     */
    public boolean searchPruning() {
        return settings.getAsBoolean("serverless.search.prune", true);
    }
}
