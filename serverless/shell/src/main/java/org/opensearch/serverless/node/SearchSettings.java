/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.node;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.settings.SettingsProvider;

import java.util.List;

/**
 * How a node's searches fan out and resolve patterns, read from the node's settings.
 */
public final class SearchSettings implements SettingsProvider {

    public static final TimeValue DRAIN_DEFAULT_MAX_WAIT = TimeValue.timeValueMinutes(2);

    /** The default share: nine tenths. */
    public static final double WARMUP_DEFAULT_READY_FRACTION = 0.9;

    /** The default bound: long enough to copy a few gigabytes at the default pace. */
    public static final TimeValue WARMUP_DEFAULT_MAX_WAIT = TimeValue.timeValueMinutes(3);

    /** The {@code serverless.search.pattern.max_indices} setting. */
    public static final Setting<Integer> PATTERN_MAX_INDICES = Setting.intSetting(
        "serverless.search.pattern.max_indices",
        MetadataPlane.DEFAULT_PATTERN_CAP,
        Property.NodeScope
    );

    /** The {@code serverless.search.fanout.concurrency} setting. */
    public static final Setting<Integer> FANOUT_CONCURRENCY = Setting.intSetting(
        "serverless.search.fanout.concurrency",
        SearchSettings.DEFAULT_SEARCH_FANOUT_CONCURRENCY,
        Property.NodeScope
    );

    /** The {@code serverless.search.cold_open.wait_millis} setting. */
    public static final Setting<Long> COLD_OPEN_WAIT_MILLIS = Setting.longSetting(
        "serverless.search.cold_open.wait_millis",
        10_000L,
        0L,
        Property.NodeScope
    );

    /** The {@code serverless.search.rollup.cache_millis} setting. */
    public static final Setting<Long> ROLLUP_CACHE_MILLIS = Setting.longSetting(
        "serverless.search.rollup.cache_millis",
        0L,
        Property.NodeScope
    );

    /** The {@code serverless.search.rollup.min_indices} setting. */
    public static final Setting<Integer> ROLLUP_MIN_INDICES = Setting.intSetting(
        "serverless.search.rollup.min_indices",
        SearchSettings.DEFAULT_ROLLUP_MIN_INDICES,
        Property.NodeScope
    );

    /** The {@code serverless.search.max_activations_per_query} setting. */
    public static final Setting<Integer> MAX_ACTIVATIONS_PER_QUERY = Setting.intSetting(
        "serverless.search.max_activations_per_query",
        SearchSettings.DEFAULT_SEARCH_ACTIVATION_BUDGET,
        Property.NodeScope
    );

    /** Whether a search node joins unready and turns ready only once it has warmed. */
    public static final Setting<Boolean> WARM_ON_JOIN = Setting.boolSetting("serverless.search.warm_on_join", false, Property.NodeScope);

    /** The longest a joining node stays warming before it turns ready regardless. */
    public static final Setting<TimeValue> WARM_ON_JOIN_MAX_WAIT = Setting.timeSetting(
        "serverless.search.warm_on_join.max_wait",
        WARMUP_DEFAULT_MAX_WAIT,
        Property.NodeScope
    );

    /**
     * The share of its peers' named warm blocks a joiner holds before it turns ready, from 0 to 1. The rest keeps
     * copying after: the coldest tenth of a hot set is worth less than the minutes it can take, especially from a slow
     * peer.
     */
    public static final Setting<Double> WARM_ON_JOIN_READY_FRACTION = Setting.doubleSetting(
        "serverless.search.warm_on_join.ready_fraction",
        WARMUP_DEFAULT_READY_FRACTION,
        -Double.MAX_VALUE,
        Property.NodeScope
    );

    /** The longest a drain waits for its peers to take over. */
    public static final Setting<TimeValue> DRAIN_MAX_WAIT = Setting.timeSetting(
        "serverless.search.drain.max_wait",
        DRAIN_DEFAULT_MAX_WAIT,
        Property.NodeScope
    );

    /** How long a draining node waits for coordinators to see its role gone; by default two thirds of the lease TTL. */
    public static final Setting<String> DRAIN_SETTLE = Setting.simpleString("serverless.search.drain.settle", Property.NodeScope);

    /** Whether shutting down drains first. Defaults to {@link #WARM_ON_JOIN}: the same deployments want both. */
    public static final Setting<Boolean> DRAIN_ON_SHUTDOWN = Setting.boolSetting(
        "serverless.search.drain.on_shutdown",
        WARM_ON_JOIN,
        Property.NodeScope
    );

    /** How long a check that found a live writer stands, in milliseconds, while the membership shows it still live. */
    public static final Setting<Long> READER_CHECK_LIVE_OWNER_MILLIS = Setting.longSetting(
        "serverless.search.reader_check.live_owner_millis",
        ReaderValidation.LIVE_OWNER_CHECK_MILLIS,
        Property.NodeScope
    );

    private final Settings settings;

    /** Used by {@link java.util.ServiceLoader}, which only reads the settings this class defines. */
    public SearchSettings() {
        this(Settings.EMPTY);
    }

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
        return SearchSettings.PATTERN_MAX_INDICES.get(settings);
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
        return Math.max(1, SearchSettings.FANOUT_CONCURRENCY.get(settings));
    }

    /**
     * How long a search waits for this node to open a reader before it asks another copy, in milliseconds.
     *
     * <p>Zero waits for the open as before. The open is not cancelled when the wait ends: it finishes in the
     * background and the next search finds it. If no other copy answers, the search returns to wait for it.
     *
     * @return the wait
     */
    public long coldOpenWaitMillis() {
        return Math.max(0L, SearchSettings.COLD_OPEN_WAIT_MILLIS.get(settings));
    }

    /** How many indices a prefix pattern must match before a search reads its group's digest rollups. */
    public static final int DEFAULT_ROLLUP_MIN_INDICES = 32;

    /**
     * How old this node's copy of a rollup group may be and still resolve a pattern. Zero by default -- the group
     * is read for every search -- because a copy that predates an index's activation leaves the index off the
     * search, and a search on its owner would otherwise have seen its refreshed documents. A deployment that
     * accepts that window for fewer reads can set it.
     *
     * @return the age in milliseconds; zero to read the group every time
     */
    public long rollupCacheMillis() {
        return Math.max(0L, SearchSettings.ROLLUP_CACHE_MILLIS.get(settings));
    }

    /**
     * How many indices a pattern must match before the digest rollups are consulted: below it, the per-index
     * checks cost less than the group's registers.
     *
     * @return the threshold
     */
    public int rollupMinIndices() {
        return Math.max(1, SearchSettings.ROLLUP_MIN_INDICES.get(settings));
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
        return Math.max(1, SearchSettings.MAX_ACTIVATIONS_PER_QUERY.get(settings));
    }

    @Override
    public List<Setting<?>> settings() {
        return List.of(
            PATTERN_MAX_INDICES,
            FANOUT_CONCURRENCY,
            COLD_OPEN_WAIT_MILLIS,
            ROLLUP_CACHE_MILLIS,
            ROLLUP_MIN_INDICES,
            MAX_ACTIVATIONS_PER_QUERY,
            WARM_ON_JOIN,
            WARM_ON_JOIN_MAX_WAIT,
            WARM_ON_JOIN_READY_FRACTION,
            DRAIN_MAX_WAIT,
            DRAIN_SETTLE,
            DRAIN_ON_SHUTDOWN,
            READER_CHECK_LIVE_OWNER_MILLIS
        );
    }
}
