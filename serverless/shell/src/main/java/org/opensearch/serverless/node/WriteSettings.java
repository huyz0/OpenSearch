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
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.serverless.settings.SettingsProvider;
import org.opensearch.serverless.store.CommitStaging;

import java.util.List;

/**
 * Settings of the write path: back-pressure, fences, pools, segment packing and publishing.
 *
 * <p>Each setting is defined here once, with its type and default, and found through {@link SettingsProvider}.
 */
public final class WriteSettings implements SettingsProvider {

    /**
     * How many operations a shard may hold beyond its last publish before it is published inside the minimum
     * interval, which bounds what a successor replays from the log at any write rate.
     */
    public static final long DEFAULT_PUBLISH_MAX_UNPUBLISHED_OPERATIONS = 10_000L;

    /**
     * The least time between two publishes of one shard that a node started by {@code ServerlessBootstrap} keeps.
     *
     * <p>A publish of a commit that adds one small segment costs five or more PUTs. Readers look for a new commit
     * only every {@code ReconcileScheduler#READER_REFRESH_EVERY_RENEWALS} renewals, so publishing a busy shard more often than every
     * few seconds pays those PUTs for freshness no reader sees. A shard left alone for longer than this is still
     * published one debounce window after its next write.
     */
    public static final org.opensearch.common.unit.TimeValue DEFAULT_PUBLISH_MIN_INTERVAL = org.opensearch.common.unit.TimeValue
        .timeValueSeconds(5);

    /** The default {@link #NODE_LOG_PUBLISH_MAX_AGE_MILLIS}: a second, as core refreshes an index that sets no interval. */
    public static final long DEFAULT_NODE_LOG_PUBLISH_MAX_AGE_MILLIS = 1_000L;

    /** The {@code serverless.write.backpressure.target_millis} setting. */
    public static final Setting<Long> BACKPRESSURE_TARGET_MILLIS = Setting.longSetting(
        "serverless.write.backpressure.target_millis",
        1_000L,
        Property.NodeScope
    );

    /** The {@code serverless.write.backpressure.budget_millis} setting. */
    public static final Setting<Long> BACKPRESSURE_BUDGET_MILLIS = Setting.longSetting(
        "serverless.write.backpressure.budget_millis",
        5_000L,
        Property.NodeScope
    );

    /** The {@code serverless.write.backpressure.min_in_flight} setting. */
    public static final Setting<Integer> BACKPRESSURE_MIN_IN_FLIGHT = Setting.intSetting(
        "serverless.write.backpressure.min_in_flight",
        4,
        Property.NodeScope
    );

    /**
     * How many writes must have asked for a stripe within its last two background periods (about four fifths of a fence
     * window) for the background to keep reading it; node setting, default 2. Below that, a write reads its stripe itself
     * when its last read is older than the window, which a stripe written that rarely needed anyway: a read the
     * background made for a lone write was never used by another one, and a lone write cost five reads that way rather
     * than one. 0 keeps every stripe asked within two windows read in the background, as before.
     */
    public static final Setting<Integer> STRIPE_REFRESH_MIN_ASKS = Setting.intSetting(
        "serverless.write.stripe_refresh_min_asks",
        2,
        Property.NodeScope
    );

    /** Threads per core of {@link WritePools#POOL}, and of the forwarded-write pool. */
    public static final Setting<Integer> WAIT_POOL_THREADS_PER_CORE = Setting.intSetting(
        "serverless.write.wait_pool.threads_per_core",
        WritePools.DEFAULT_THREADS_PER_CORE,
        Property.NodeScope
    );

    /**
     * Writes a node admits at once before it pushes back; by default sized from the write pools. Declared after the
     * wait-pool settings because its default reads them.
     */
    public static final Setting<Integer> BACKPRESSURE_MAX_IN_FLIGHT = new Setting<>(
        "serverless.write.backpressure.max_in_flight",
        s -> Integer.toString(WritePools.defaultMaxInFlight(s)),
        s -> Setting.parseInt(s, Integer.MIN_VALUE, "serverless.write.backpressure.max_in_flight"),
        Property.NodeScope
    );

    /** The {@code serverless.segments.node_pack.enabled} setting. */
    public static final Setting<Boolean> NODE_PACK_ENABLED = Setting.boolSetting(
        "serverless.segments.node_pack.enabled",
        NodePackWiring.ENABLED_DEFAULT,
        Property.NodeScope
    );

    /** The {@code serverless.segments.node_pack.max_blob_bytes} setting. */
    public static final Setting<ByteSizeValue> NODE_PACK_MAX_BLOB_BYTES = Setting.byteSizeSetting(
        "serverless.segments.node_pack.max_blob_bytes",
        new ByteSizeValue(NodePackWiring.DEFAULT_MAX_BLOB_BYTES),
        Property.NodeScope
    );

    /** The {@code serverless.segments.node_pack.linger_millis} setting. */
    public static final Setting<Long> NODE_PACK_LINGER_MILLIS = Setting.longSetting(
        "serverless.segments.node_pack.linger_millis",
        NodePackWiring.DEFAULT_LINGER_MILLIS,
        Property.NodeScope
    );

    /** The {@code serverless.segments.node_pack.collect_interval_millis} setting. */
    public static final Setting<Long> NODE_PACK_COLLECT_INTERVAL_MILLIS = Setting.longSetting(
        "serverless.segments.node_pack.collect_interval_millis",
        NodePackWiring.DEFAULT_COLLECT_INTERVAL_MILLIS,
        Property.NodeScope
    );

    /** The setting, on the node and in the cluster's config register. */
    public static final Setting<Long> LOG_COMMITS_CHECKPOINT_MILLIS = Setting.longSetting(
        "serverless.wal.node_log.commits.checkpoint_millis",
        CommitStaging.DEFAULT_MAX_STAGED_MILLIS,
        Property.NodeScope
    );

    /**
     * With one write log per node, how long a write may wait to be published: the longest a write takes to be
     * searchable, and what bounds how much of the log is kept. Zero publishes as without the log. See
     * {@code BackgroundReconciler#setLazyPublish}.
     */
    public static final Setting<Long> NODE_LOG_PUBLISH_MAX_AGE_MILLIS = Setting.longSetting(
        "serverless.wal.node_log.publish_max_age_millis",
        WriteSettings.DEFAULT_NODE_LOG_PUBLISH_MAX_AGE_MILLIS,
        Property.NodeScope
    );

    /** How long a publish waits for more writes; by default the node log's window when the node log is on. */
    public static final Setting<String> PUBLISH_DEBOUNCE_MILLIS = Setting.simpleString(
        "serverless.publish.debounce_millis",
        Property.NodeScope
    );

    /**
     * The least time between two write-triggered publishes of one shard. Each publish of a small commit costs five or
     * more PUTs, so this is the knob that decides how much a busy shard spends on publishing; zero or less publishes
     * every debounce window. See {@link WriteSettings#DEFAULT_PUBLISH_MIN_INTERVAL}.
     */
    public static final Setting<Long> PUBLISH_MIN_INTERVAL_MILLIS = Setting.longSetting(
        "serverless.publish.min_interval_millis",
        WriteSettings.DEFAULT_PUBLISH_MIN_INTERVAL.millis(),
        Property.NodeScope
    );

    /**
     * How many operations beyond its last publish make a shard due for a publish inside the minimum interval; zero
     * for never early. Bounds what a successor replays.
     */
    public static final Setting<Long> PUBLISH_MAX_UNPUBLISHED_OPERATIONS = Setting.longSetting(
        "serverless.publish.max_unpublished_operations",
        WriteSettings.DEFAULT_PUBLISH_MAX_UNPUBLISHED_OPERATIONS,
        Property.NodeScope
    );

    /** Used by {@link java.util.ServiceLoader}. */
    public WriteSettings() {}

    @Override
    public List<Setting<?>> settings() {
        return List.of(
            BACKPRESSURE_MAX_IN_FLIGHT,
            BACKPRESSURE_TARGET_MILLIS,
            BACKPRESSURE_BUDGET_MILLIS,
            BACKPRESSURE_MIN_IN_FLIGHT,
            STRIPE_REFRESH_MIN_ASKS,
            WAIT_POOL_THREADS_PER_CORE,
            NODE_PACK_ENABLED,
            NODE_PACK_MAX_BLOB_BYTES,
            NODE_PACK_LINGER_MILLIS,
            NODE_PACK_COLLECT_INTERVAL_MILLIS,
            LOG_COMMITS_CHECKPOINT_MILLIS,
            NODE_LOG_PUBLISH_MAX_AGE_MILLIS,
            PUBLISH_DEBOUNCE_MILLIS,
            PUBLISH_MIN_INTERVAL_MILLIS,
            PUBLISH_MAX_UNPUBLISHED_OPERATIONS
        );
    }
}
