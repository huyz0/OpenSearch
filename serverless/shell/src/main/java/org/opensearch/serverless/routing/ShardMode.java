/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.routing;

import org.opensearch.common.settings.Settings;

import java.util.Locale;

/**
 * How an index's units start and whether they change by themselves.
 *
 * <p>Set once, at creation, with {@code index.serverless.shard_mode}. An index created without it has no unit layout at
 * all and routes as it always has, so nothing about an existing index, or a new one that does not ask,
 * changes.
 */
public enum ShardMode {

    /**
     * One unit per declared shard from creation, and never cut automatically. For indices known to be
     * big from the first day, or that need predictable per-shard behaviour.
     */
    FIXED,

    /**
     * One unit covering every declared shard at creation, cut in the background as it grows.
     */
    ADAPTIVE;

    /**
    * Returns the mode's name as it is written in settings and in the descriptor.
    *
    * @return the lowercase name
    */
    public String settingValue() {
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * Parses a mode from its setting value.
     *
     * @param value {@code fixed} or {@code adaptive}
     * @return the mode
     * @throws IllegalArgumentException naming the accepted values when it is neither
     */
    public static ShardMode parse(String value) {
        if (value != null) {
            for (ShardMode mode : values()) {
                if (mode.settingValue().equals(value.trim().toLowerCase(Locale.ROOT))) {
                    return mode;
                }
            }
        }
        throw new IllegalArgumentException(
            "[" + RoutingSettings.SHARD_MODE.getKey() + "] must be [fixed] or [adaptive], got [" + value + "]"
        );
    }

    /**
     * Returns the mode index settings ask for, or null when they ask for none.
     *
     * @param settings normalised ({@code index.}-prefixed) settings
     * @return the mode, or null
     * @throws IllegalArgumentException when the value is not a mode
     */
    public static ShardMode requested(Settings settings) {
        final String value = settings == null ? null : settings.get(RoutingSettings.SHARD_MODE.getKey());
        return value == null ? null : parse(value);
    }

    /**
     * Returns the settings without the mode, which the descriptor holds as a field of its own.
     *
     * <p>Core validates index settings against its own registry and has never heard of this one, and a
     * shard has no use for it: routing happens before a document reaches a shard.
     *
     * @param settings the settings, or null
     * @return the same settings without {@code index.serverless.shard_mode}
     */
    public static Settings without(Settings settings) {
        if (settings == null || settings.get(RoutingSettings.SHARD_MODE.getKey()) == null) {
            return settings;
        }
        final Settings.Builder builder = Settings.builder().put(settings);
        builder.remove(RoutingSettings.SHARD_MODE.getKey());
        return builder.build();
    }
}
