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
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.settings.SettingsProvider;

import java.util.List;

/**
 * Settings of how a node opens, idles and takes over shards.
 *
 * <p>Each setting is defined here once, with its type and default, and found through {@link SettingsProvider}.
 */
public final class ActivationSettings implements SettingsProvider {

    /** Whether to take unowned shards that somebody asks for. */
    public static final Setting<Boolean> ON_DEMAND = Setting.boolSetting("serverless.activation.on_demand", true, Property.NodeScope);

    /** How many shards this node will take on demand before refusing. */
    public static final Setting<Integer> MAX_SHARDS = Setting.intSetting(
        "serverless.activation.max_shards",
        BackgroundReconciler.DEFAULT_MAX_SHARDS_HELD,
        Property.NodeScope
    );

    /**
     * How long a shard may go unused before this node lets go of it. Zero never releases.
     *
     * <p>On here and off in the library, the same split {@link #ON_DEMAND} has. A node running unattended
     * that only ever acquires shards is not serverless in any sense its users would recognise — it is a
     * process that grows until it hits {@link #MAX_SHARDS} and then starts refusing.
     */
    public static final Setting<Long> IDLE_AFTER_MILLIS = Setting.longSetting(
        "serverless.activation.idle_after_millis",
        BackgroundReconciler.DEFAULT_IDLE_AFTER_MILLIS,
        Property.NodeScope
    );

    /** How long a shard must be unused before a node at its cap evicts it to take a new one. */
    public static final Setting<Long> EVICT_AFTER_MILLIS = Setting.longSetting(
        "serverless.activation.evict_after_millis",
        BackgroundReconciler.DEFAULT_EVICT_AFTER_MILLIS,
        Property.NodeScope
    );

    /**
     * How long a writer may go unused before it hibernates: it publishes, closes its engine and keeps its head, and the
     * next request wakes it at the same term with one head read rather than a takeover. Zero never hibernates. Below
     * {@link #IDLE_AFTER_MILLIS}, which still gives the head back once a writer has been unused that long.
     */
    public static final Setting<Long> HIBERNATE_AFTER_MILLIS = Setting.longSetting(
        "serverless.activation.hibernate_after_millis",
        ServerlessBootstrap.DEFAULT_HIBERNATE_AFTER_MILLIS,
        Property.NodeScope
    );

    /** Whether the activations at once follow the store, up to {@link #CONCURRENCY}; off by default. */
    public static final Setting<Boolean> ADAPTIVE = Setting.boolSetting("serverless.activation.adaptive", false, Property.NodeScope);

    /** Whether activations at once are cut while the write path's limiter is cutting writes; off until measured. */
    public static final Setting<Boolean> PACED_BY_WRITES = Setting.boolSetting(
        "serverless.activation.paced_by_writes",
        false,
        Property.NodeScope
    );

    /** How long an activation should take before the adaptive limit falls. */
    public static final Setting<Long> TARGET_MILLIS = Setting.longSetting(
        "serverless.activation.target_millis",
        BackgroundReconciler.DEFAULT_ACTIVATION_TARGET_MILLIS,
        Property.NodeScope
    );

    /**
     * How many shards a node activates at once; the activation pool is sized from it. An activation spends most of its time
     * waiting on the object store, so a node taking a dead node's shards, or a new node taking its share, is bounded by
     * this times the store's latency rather than by its own work.
     */
    public static final Setting<Integer> CONCURRENCY = Setting.intSetting(
        "serverless.activation.concurrency",
        BackgroundReconciler.DEFAULT_ACTIVATION_CONCURRENCY,
        Property.NodeScope
    );

    /** Whether requests for a dead member's shard go to the one survivor named for it; on by default. */
    public static final Setting<Boolean> TAKEOVER_ROUTE_TO_WINNER = Setting.boolSetting(
        "serverless.takeover.route_to_winner",
        true,
        Property.NodeScope
    );

    /** How far past its cap a node may take a dead member's shards, as a fraction of the cap; 0 turns it off. */
    public static final Setting<Double> TAKEOVER_OVERCOMMIT = Setting.doubleSetting(
        "serverless.takeover.overcommit",
        BackgroundReconciler.DEFAULT_TAKEOVER_OVERCOMMIT,
        -Double.MAX_VALUE,
        Property.NodeScope
    );

    /** Used by {@link java.util.ServiceLoader}. */
    public ActivationSettings() {}

    @Override
    public List<Setting<?>> settings() {
        return List.of(
            ON_DEMAND,
            MAX_SHARDS,
            IDLE_AFTER_MILLIS,
            EVICT_AFTER_MILLIS,
            HIBERNATE_AFTER_MILLIS,
            ADAPTIVE,
            PACED_BY_WRITES,
            TARGET_MILLIS,
            CONCURRENCY,
            TAKEOVER_ROUTE_TO_WINNER,
            TAKEOVER_OVERCOMMIT
        );
    }
}
