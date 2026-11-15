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
    /**
     * Whether a read acts on Lucene's prefetch hints, fetching the announced blocks on the generic pool so a query over
     * many segments waits for their round trips together rather than one after another.
     */
    public static final Setting<Boolean> READ_PREFETCH = Setting.boolSetting(
        "serverless.block_cache.read_prefetch",
        true,
        Property.NodeScope
    );

    public CacheSettings() {}

    /** Names the index whose storage a shard should read, when that is not the index it belongs to. */
    public static final Setting<String> STORAGE_UUID = Setting.simpleString(
        "index.serverless.storage_uuid",
        Property.IndexScope,
        Property.PrivateIndex
    );

    /**
     * Where a shard's bytes live, which is not always where its index identity says.
     *
     * <p>A frozen view is opened as an index of its own, because a node cannot hold two shards with the
     * same identity and it may well already be serving the live one. That synthetic identity has a uuid
     * nothing ever wrote under, so the path has to come from the index it is a view <em>of</em> — carried
     * as a setting on the shard the reader opens.
     *
     * <p>Absent for every ordinary shard, which is the overwhelming majority, and then this is the index's
     * own uuid exactly as before.
     *
     * @param indexSettings the shard's index settings
     * @return the uuid its bytes are stored under
     */
    public static String storageUuidOf(org.opensearch.index.IndexSettings indexSettings) {
        final String source = indexSettings.getSettings().get(STORAGE_UUID.getKey());
        return source == null ? indexSettings.getIndex().getUUID() : source;
    }

    /**
     * The frozen view a shard is serving, if it is serving one.
    *
     * <p>A view is opened as an index of its own whose uuid is the view's identifier, and it is the only
     * kind of shard that carries {@link #STORAGE_UUID} — so the presence of that setting is what
     * distinguishes one, and its own uuid is the identifier.
     *
     * @param indexSettings the shard's index settings
     * @return the view's identifier, or null for an ordinary shard
     */
    public static String frozenViewIdOf(org.opensearch.index.IndexSettings indexSettings) {
        return indexSettings.getSettings().get(STORAGE_UUID.getKey()) == null ? null : indexSettings.getIndex().getUUID();
    }

    @Override
    public List<Setting<?>> settings() {
        return List.of(
            STORAGE_UUID,
            BLOCK_SIZE,
            MAX_BLOCKS,
            LOCAL_MEMORY_ENABLED,
            LOCAL_MEMORY_MAX_BYTES,
            LOCAL_MEMORY_MAX_FILE_BYTES,
            READ_PREFETCH
        );
    }
}
