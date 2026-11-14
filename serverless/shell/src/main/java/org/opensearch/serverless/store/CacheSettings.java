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
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.serverless.settings.SettingsProvider;

import java.util.List;

/**
 * Settings of the node's block cache and local segment tier.
 *
 * <p>Each setting is defined here once, with its type and default, and found through {@link SettingsProvider}.
 */
public final class CacheSettings implements SettingsProvider {

    /** The {@code serverless.block_cache.block_size} setting. */
    public static final Setting<Integer> BLOCK_SIZE = Setting.intSetting(
        "serverless.block_cache.block_size",
        BlockCache.DEFAULT_BLOCK_SIZE,
        Property.NodeScope
    );

    /** Blocks the node's block cache holds; by default a tenth of the heap. */
    public static final Setting<Integer> MAX_BLOCKS = new Setting<>(
        "serverless.block_cache.max_blocks",
        s -> Integer.toString((int) Math.max(256L, Runtime.getRuntime().maxMemory() / 10 / BLOCK_SIZE.get(s))),
        s -> Setting.parseInt(s, Integer.MIN_VALUE, "serverless.block_cache.max_blocks"),
        Property.NodeScope
    );

    /** The {@code serverless.block_cache.prefetch_on_open} setting. */
    public static final Setting<Boolean> PREFETCH_ON_OPEN = Setting.boolSetting(
        "serverless.block_cache.prefetch_on_open",
        true,
        Property.NodeScope
    );

    /** Whether writers hold freshly written segment files in memory. */
    public static final Setting<Boolean> LOCAL_MEMORY_ENABLED = Setting.boolSetting(
        "serverless.local_segments.memory.enabled",
        LocalSegmentTier.DEFAULT_ENABLED,
        Property.NodeScope
    );

    /** The node's budget for them, in bytes or as a share of the heap. */
    public static final Setting<String> LOCAL_MEMORY_MAX_BYTES = Setting.simpleString(
        "serverless.local_segments.memory.max_bytes",
        LocalSegmentTier.DEFAULT_MAX_BYTES,
        Property.NodeScope
    );

    /** The largest file held in memory; a bigger one moves to disk as it passes this. */
    public static final Setting<ByteSizeValue> LOCAL_MEMORY_MAX_FILE_BYTES = Setting.byteSizeSetting(
        "serverless.local_segments.memory.max_file_bytes",
        s -> LocalSegmentTier.DEFAULT_MAX_FILE_BYTES,
        Property.NodeScope
    );

    /** Used by {@link java.util.ServiceLoader}. */
    public CacheSettings() {}

    @Override
    public List<Setting<?>> settings() {
        return List.of(BLOCK_SIZE, MAX_BLOCKS, PREFETCH_ON_OPEN, LOCAL_MEMORY_ENABLED, LOCAL_MEMORY_MAX_BYTES, LOCAL_MEMORY_MAX_FILE_BYTES);
    }
}
