/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

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

    /**
     * Whether plugin objects are built by core's injector: on by default.
     *
     * <p>Off falls back to constructor resolution only, which cannot run a plugin that binds Guice modules,
     * takes a {@code Provider}, or names lifecycle services. With no plugin installed the two are identical.
     */
    public static final Setting<Boolean> INJECTOR = Setting.boolSetting("serverless.plugin_injector.enabled", true, Property.NodeScope);

    /** Whether the view holds the plugins' indices: on by default, and only on a node whose plugins declare any. */
    public static final Setting<Boolean> INDICES_VIEW = Setting.boolSetting(
        "serverless.plugin_indices_view.enabled",
        true,
        Property.NodeScope
    );

    /** How often changes made by other nodes are looked for; 0 to look only at start. Default 30 seconds. */
    public static final Setting<TimeValue> INDICES_VIEW_REFRESH_INTERVAL = Setting.timeSetting(
        "serverless.plugin_indices_view.refresh_interval",
        TimeValue.timeValueSeconds(30),
        Property.NodeScope
    );

    /** Whether plugins' dynamic settings in the register are applied to the running plugins. On by default. */
    public static final Setting<Boolean> CLUSTER_SETTINGS = Setting.boolSetting(
        "serverless.plugin_cluster_settings.enabled",
        true,
        Property.NodeScope
    );

    /** How often a node reads the register for changes made on other nodes. */
    public static final Setting<TimeValue> CLUSTER_SETTINGS_REFRESH_INTERVAL = Setting.timeSetting(
        "serverless.plugin_cluster_settings.refresh_interval",
        TimeValue.timeValueSeconds(30),
        Property.NodeScope
    );

    /**
     * Whether the settings and thread pools plugins declare are registered: on by default.
     *
     * <p>Off is the old behaviour, kept as a switch for one release: a plugin that declares a setting it
     * listens to, or a pool it submits to, fails to start with it off -- Job Scheduler on its sweeper
     * period, k-NN and ML Commons on their pools. With no plugin installed the two are identical.
     */
    public static final Setting<Boolean> EXTENSIONS = Setting.boolSetting("serverless.plugin_extensions.enabled", true, Property.NodeScope);

    /**
     * Whether plugins see a cluster they can work with: on by default, and only on a node with plugins.
     *
     * <p>On, the node's view names a cluster manager and the plugin client answers cluster health and cluster
     * state. Off, a plugin that waits for health (the security plugin does, before loading its configuration)
     * or that reads the cluster state through its client never gets an answer.
     */
    public static final Setting<Boolean> CLUSTER_VIEW = Setting.boolSetting(
        "serverless.plugin_cluster_view.enabled",
        true,
        Property.NodeScope
    );

    /**
     * Whether the plugin client runs multi-get, bulk and update: on by default.
     *
     * <p>Off, a plugin that loads its state with a multi-get or writes it with a bulk -- the security plugin does
     * both -- is refused by name, as before.
     */
    public static final Setting<Boolean> DOCUMENT_BATCHES = Setting.boolSetting(
        "serverless.plugin_client.batches.enabled",
        true,
        Property.NodeScope
    );

    /**
     * Whether plugin components that are lifecycle components are started with the node and stopped with it: on by
     * default.
     *
     * <p>Core starts every component a plugin's {@code createComponents} returns that is a lifecycle component. The
     * alerting plugin's monitor runner builds its state in {@code doStart}; never started, every monitor run failed on
     * an uninitialised field. Off, they are left as they were built, as before.
     */
    public static final Setting<Boolean> COMPONENT_LIFECYCLE = Setting.boolSetting(
        "serverless.plugin_components.lifecycle.enabled",
        true,
        Property.NodeScope
    );

    /**
     * Whether the plugin client answers index administration: existence, mappings, settings and aliases, read and
     * changed, index deletion and rollover. On by default.
     *
     * <p>Off, a plugin that checks or upgrades its own indices -- alerting does before running a monitor -- is refused
     * by name, as before.
     */
    public static final Setting<Boolean> INDEX_ADMIN = Setting.boolSetting(
        "serverless.plugin_client.index_admin.enabled",
        true,
        Property.NodeScope
    );

    /**
     * Whether the plugin client answers node info, node stats, indices stats and refresh: on by default.
     *
     * <p>Off, a metrics exporter and a plugin that refreshes its own index are refused by name, as before.
     */
    public static final Setting<Boolean> NODE_ANSWERS = Setting.boolSetting(
        "serverless.plugin_client.node_answers.enabled",
        true,
        Property.NodeScope
    );

    /**
     * Whether the index-name resolver plugins are given answers for tenant names from the metadata plane: a concrete
     * name, an alias or a trailing wildcard, one read each. On by default.
     *
     * <p>Off, a plugin resolving a tenant index -- the anomaly-detection plugin checks a detector's source index before
     * each run -- is refused by name, as before.
     */
    public static final Setting<Boolean> NAME_RESOLUTION = Setting.boolSetting(
        "serverless.plugin_name_resolution.enabled",
        true,
        Property.NodeScope
    );

    /** Used by {@link java.util.ServiceLoader}. */
    public PluginSettings() {}

    @Override
    public List<Setting<?>> settings() {
        return List.of(
            INJECTOR,
            INDICES_VIEW,
            INDICES_VIEW_REFRESH_INTERVAL,
            CLUSTER_SETTINGS,
            CLUSTER_SETTINGS_REFRESH_INTERVAL,
            EXTENSIONS,
            CLUSTER_VIEW,
            DOCUMENT_BATCHES,
            COMPONENT_LIFECYCLE,
            INDEX_ADMIN,
            NODE_ANSWERS,
            NAME_RESOLUTION
        );
    }
}
