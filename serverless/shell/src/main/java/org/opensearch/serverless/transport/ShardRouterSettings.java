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
import org.opensearch.serverless.settings.SettingsProvider;

import java.util.List;

/**
 * Settings of the shard router's traffic between nodes.
 *
 * <p>Each setting is defined here once, with its type and default, and found through {@link SettingsProvider}.
 */
public final class ShardRouterSettings implements SettingsProvider {

    /**
     * Whether forwards to a peer in another zone are compressed, true by default. Forwarded documents and search hits
     * are mostly JSON source, which compresses several times over, and traffic between zones is billed by the byte.
     */
    public static final Setting<Boolean> COMPRESS_CROSS_ZONE = Setting.boolSetting(
        "serverless.network.compress_cross_zone",
        true,
        Property.NodeScope
    );

    /** Entries the router's replay cache holds. */
    public static final Setting<Integer> REPLAY_CACHE_CAPACITY = Setting.intSetting(
        "serverless.transport.replay_cache.capacity",
        65_536,
        Property.NodeScope
    );

    /** Used by {@link java.util.ServiceLoader}. */
    public ShardRouterSettings() {}

    @Override
    public List<Setting<?>> settings() {
        return List.of(COMPRESS_CROSS_ZONE, REPLAY_CACHE_CAPACITY);
    }
}
