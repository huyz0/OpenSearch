/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.plugins;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.settings.SettingsProvider;

import java.util.List;

/**
 * Settings that switch the plugin host's pieces on and off.
 *
 * <p>Each setting is defined here once, with its type and default, and found through {@link SettingsProvider}.
 */
public final class PluginSettings implements SettingsProvider {

    /** How often changes made by other nodes are looked for; 0 to look only at start. Default 30 seconds. */
    public static final Setting<TimeValue> INDICES_VIEW_REFRESH_INTERVAL = Setting.timeSetting(
        "serverless.plugin_indices_view.refresh_interval",
        TimeValue.timeValueSeconds(30),
        Property.NodeScope
    );

    /** How often a node reads the register for changes made on other nodes. */
    public static final Setting<TimeValue> CLUSTER_SETTINGS_REFRESH_INTERVAL = Setting.timeSetting(
        "serverless.plugin_cluster_settings.refresh_interval",
        TimeValue.timeValueSeconds(30),
        Property.NodeScope
    );

    /** Used by {@link java.util.ServiceLoader}. */
    public PluginSettings() {}

    @Override
    public List<Setting<?>> settings() {
        return List.of(INDICES_VIEW_REFRESH_INTERVAL, CLUSTER_SETTINGS_REFRESH_INTERVAL);
    }
}
