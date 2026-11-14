/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Plugins' dynamic settings, applied from the {@code cluster/config} register to the running plugins.
 *
 * <p>A plugin reads its dynamic settings through {@code ClusterSettings}: it registers a consumer, and core calls it when the
 * cluster state's persistent settings change. The shell keeps those settings in its register instead, so a value set through
 * {@code PUT /_cluster/settings} was stored and never reached the plugin. Here the register's values for settings a plugin
 * declares as dynamic are applied to the node's {@code ClusterSettings}, as core applies a new cluster state: on the node that
 * served the write when it returns, at start, and on every other node within its refresh interval (30 s by default; one register
 * read per interval per node, and only on nodes whose plugins declare dynamic settings). A key removed from the
 * register falls back to the node's own value, or the default. Only plugins' settings are applied: core's own dynamic settings
 * mean cluster-manager behaviour the shell does not have, and the shell's own settings are read where they are used.
 *
 * <p>A value a plugin's setting refuses is refused at the write, with the setting's own message, rather than stored and then
 * failing on every node.
 */
public final class PluginClusterSettings {

    private static final Logger logger = LogManager.getLogger(PluginClusterSettings.class);

    /** Whether plugins' dynamic settings in the register are applied to the running plugins. On by default. */
    static final String ENABLED = "serverless.plugin_cluster_settings.enabled";

    /** How often a node reads the register for changes made on other nodes. */
    static final String REFRESH_INTERVAL = "serverless.plugin_cluster_settings.refresh_interval";

    private final ServerlessNode node;
    private final List<Setting<?>> dynamic;
    private volatile Scheduler.Cancellable refresher;
    private Settings applied = null;

    PluginClusterSettings(ServerlessNode node, List<Setting<?>> pluginSettings) {
        this.node = node;
        final List<Setting<?>> found = new ArrayList<>();
        for (Setting<?> setting : pluginSettings) {
            if (setting.hasNodeScope() && setting.isDynamic()) {
                found.add(setting);
            }
        }
        this.dynamic = List.copyOf(found);
    }

    private boolean active() {
        return node.settings.getAsBoolean(ENABLED, true) && dynamic.isEmpty() == false;
    }

    /** The register's values for settings a plugin declares as dynamic. */
    Settings pluginPart(Settings cluster) {
        final Settings.Builder part = Settings.builder();
        for (String key : cluster.keySet()) {
            for (Setting<?> setting : dynamic) {
                if (setting.match(key)) {
                    part.copy(key, cluster);
                    break;
                }
            }
        }
        return part.build();
    }

    /**
     * Refuses a write whose value for a plugin's setting that setting does not accept.
     *
     * @param flat the keys being written, null for a removal
     * @throws IllegalArgumentException with the setting's own message
     */
    public void check(Map<String, Object> flat) {
        if (active() == false) {
            return;
        }
        final Settings.Builder written = Settings.builder();
        for (Map.Entry<String, Object> entry : flat.entrySet()) {
            if (entry.getValue() != null) {
                written.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        final Settings part = pluginPart(written.build());
        if (part.isEmpty() == false) {
            clusterSettings().validate(part, true);
        }
    }

    /**
     * Applies the register's values for plugins' settings, when they changed since the last apply.
     *
     * @param cluster the register's settings as just read or written
     */
    public void apply(Settings cluster) {
        if (active() == false) {
            return;
        }
        final Settings part = pluginPart(cluster);
        synchronized (this) {
            if (part.equals(applied)) {
                return;
            }
            try {
                clusterSettings().applySettings(part);
                applied = part;
            } catch (RuntimeException e) {
                logger.warn("could not apply the plugins' cluster settings " + part.keySet() + "; keeping the previous values", e);
            }
        }
    }

    private ClusterSettings clusterSettings() {
        return node.clusterService().getClusterSettings();
    }

    /** Applies what the register holds now, and starts following changes made on other nodes. */
    void start(MetadataPlane plane) {
        if (active() == false) {
            return;
        }
        refresh(plane);
        final TimeValue every = node.settings.getAsTime(REFRESH_INTERVAL, TimeValue.timeValueSeconds(30));
        if (every.millis() > 0) {
            try {
                refresher = node.threadPool().scheduleWithFixedDelay(() -> refresh(plane), every, ThreadPool.Names.GENERIC);
            } catch (RuntimeException e) {
                // Shutting down.
            }
        }
    }

    private void refresh(MetadataPlane plane) {
        try {
            apply(plane.clusterConfig().read().settings());
        } catch (Exception e) {
            logger.warn("could not read the cluster settings for the plugins; keeping the values they have", e);
        }
    }

    /** Stops following changes. */
    void stop() {
        final Scheduler.Cancellable running = refresher;
        if (running != null) {
            running.cancel();
        }
    }
}
