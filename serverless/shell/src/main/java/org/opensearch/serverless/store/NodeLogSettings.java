/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.serverless.settings.SettingsProvider;

import java.util.List;

/**
 * Settings of the node write log and its group commit.
 *
 * <p>Each setting is defined here once, with its type and default, and found through {@link SettingsProvider}.
 */
public final class NodeLogSettings implements SettingsProvider {

    /** The {@code serverless.wal.node_log.enabled} setting. */
    public static final Setting<Boolean> ENABLED = Setting.boolSetting("serverless.wal.node_log.enabled", true, Property.NodeScope);

    /** How long a node's write-log window stays open: one of {@link NodeLogCommitter#ALLOWED_WINDOW_MILLIS}. */
    public static final Setting<Long> WINDOW_MILLIS = Setting.longSetting(
        "serverless.wal.node_log.window_millis",
        NodeLogCommitter.DEFAULT_WINDOW_MILLIS,
        Property.NodeScope
    );

    /** The {@code serverless.wal.node_log.max_bytes} setting. */
    public static final Setting<Long> MAX_BYTES = Setting.longSetting(
        "serverless.wal.node_log.max_bytes",
        NodeLogCommitter.DEFAULT_MAX_BYTES,
        Property.NodeScope
    );

    /** The {@code serverless.wal.node_log.max_records} setting. */
    public static final Setting<Integer> MAX_RECORDS = Setting.intSetting(
        "serverless.wal.node_log.max_records",
        NodeLogCommitter.DEFAULT_MAX_RECORDS,
        Property.NodeScope
    );

    /**
     * How long a window's PUT may go unanswered before it is sent again, in milliseconds; zero or less never sends a
     * second. See {@link NodeLogCommitter#hedgeAppends}.
     */
    public static final Setting<Long> HEDGE_AFTER_MILLIS = Setting.longSetting(
        "serverless.wal.node_log.hedge_after_millis",
        NodeLogCommitter.DEFAULT_HEDGE_AFTER_MILLIS,
        Property.NodeScope
    );

    /** Whether a record that finds the log quiet for a window is written at once; see {@link NodeLogCommitter#flushWhenIdle}. */
    public static final Setting<Boolean> IDLE_FLUSH = Setting.boolSetting("serverless.wal.node_log.idle_flush", false, Property.NodeScope);

    /** Whether writers that join a window someone already drives wait for it without the lock; see {@link NodeLogCommitter#followUnlocked}. */
    public static final Setting<Boolean> FOLLOW_UNLOCKED = Setting.boolSetting(
        "serverless.wal.node_log.follow_unlocked",
        NodeLogCommitter.DEFAULT_FOLLOW_UNLOCKED,
        Property.NodeScope
    );

    /**
     * How many shards new to an epoch are checked at once before the window carrying their first records is written; one
     * checks them one after another. See {@link NodeLogCommitter#admitConcurrently}.
     */
    public static final Setting<Integer> ADMISSION_CONCURRENCY = Setting.intSetting(
        "serverless.wal.node_log.admission_concurrency",
        NodeLogCommitter.DEFAULT_ADMISSION_CONCURRENCY,
        Property.NodeScope
    );

    /**
     * Whether one shard's records that would carry a window past its byte or record bound are split across consecutive
     * windows, so that no blob grows far past the bound; off, they join one window whole, however many there are.
     */
    public static final Setting<Boolean> SPLIT_LARGE_BATCHES = Setting.boolSetting(
        "serverless.wal.node_log.split_large_batches",
        true,
        Property.NodeScope
    );

    /**
     * Whether a window's hedge waits longer in proportion to how much larger its blob is than the recent ones; off, every
     * blob waits the same, and a blob many times the usual size is nearly always sent twice.
     */
    public static final Setting<Boolean> HEDGE_BY_SIZE = Setting.boolSetting(
        "serverless.wal.node_log.hedge_by_size",
        true,
        Property.NodeScope
    );

    /** The {@code serverless.wal.group_commit.max_records} setting. */
    public static final Setting<Integer> GROUP_COMMIT_MAX_RECORDS = Setting.intSetting(
        "serverless.wal.group_commit.max_records",
        WalGroupCommitter.DEFAULT_MAX_RECORDS,
        Property.NodeScope
    );

    /** The {@code serverless.wal.group_commit.max_bytes} setting. */
    public static final Setting<Long> GROUP_COMMIT_MAX_BYTES = Setting.longSetting(
        "serverless.wal.group_commit.max_bytes",
        WalGroupCommitter.DEFAULT_MAX_BYTES,
        Property.NodeScope
    );

    /** Used by {@link java.util.ServiceLoader}. */
    public NodeLogSettings() {}

    @Override
    public List<Setting<?>> settings() {
        return List.of(
            ENABLED,
            WINDOW_MILLIS,
            MAX_BYTES,
            MAX_RECORDS,
            HEDGE_AFTER_MILLIS,
            IDLE_FLUSH,
            FOLLOW_UNLOCKED,
            ADMISSION_CONCURRENCY,
            SPLIT_LARGE_BATCHES,
            HEDGE_BY_SIZE,
            GROUP_COMMIT_MAX_RECORDS,
            GROUP_COMMIT_MAX_BYTES
        );
    }
}
