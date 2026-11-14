/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.settings.SettingsProvider;

import java.util.List;

/**
 * Settings of the cache copy a joining search node takes from its peers.
 *
 * <p>Each setting is defined here once, with its type and default, and found through {@link SettingsProvider}.
 */
public final class WarmCopySettings implements SettingsProvider {

    /** The most bytes a second a joiner copies, across all its peers. */
    public static final Setting<Long> MAX_BYTES_PER_SECOND = Setting.longSetting(
        "serverless.search.warm_copy.max_bytes_per_second",
        CacheWarmCopy.DEFAULT_MAX_BYTES_PER_SECOND,
        Property.NodeScope
    );

    /** How many blocks one transfer asks a peer for. */
    public static final Setting<Integer> BATCH_BLOCKS = Setting.intSetting(
        "serverless.search.warm_copy.batch_blocks",
        CacheWarmCopy.DEFAULT_BATCH_BLOCKS,
        Property.NodeScope
    );

    /** Bytes a joining node copies from its peers' caches; by default a quarter of its own block cache. */
    public static final Setting<String> MAX_BYTES = Setting.simpleString("serverless.search.warm_copy.max_bytes", Property.NodeScope);

    /** How recently a block must have been read on a peer to be copied. */
    public static final Setting<TimeValue> RECENT_WINDOW = Setting.timeSetting(
        "serverless.search.warm_copy.recent_window",
        CacheWarmCopy.DEFAULT_RECENT_WINDOW,
        Property.NodeScope
    );

    /** How many peers a joiner copies from at once. */
    public static final Setting<Integer> PARALLEL_PEERS = Setting.intSetting(
        "serverless.search.warm_copy.parallel_peers",
        CacheWarmCopy.DEFAULT_PARALLEL_PEERS,
        Property.NodeScope
    );

    /**
     * Whether a joiner copies blocks from peers in other zones; false by default. A block from a peer in another zone
     * is billed by the byte both ways, about four hundred times what reading the same block from the object store in
     * 8 MB ranges costs, so by default such a peer is asked only what it holds and the blocks are read from the store.
     */
    public static final Setting<Boolean> CROSS_ZONE = Setting.boolSetting(
        "serverless.search.warm_copy.cross_zone",
        false,
        Property.NodeScope
    );

    /** Used by {@link java.util.ServiceLoader}. */
    public WarmCopySettings() {}

    @Override
    public List<Setting<?>> settings() {
        return List.of(MAX_BYTES_PER_SECOND, BATCH_BLOCKS, MAX_BYTES, RECENT_WINDOW, PARALLEL_PEERS, CROSS_ZONE);
    }
}
