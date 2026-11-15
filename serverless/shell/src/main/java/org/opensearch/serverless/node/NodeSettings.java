/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.node;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.serverless.settings.SettingsProvider;

import java.util.List;
import java.util.function.Function;

/**
 * Settings of a node's identity, membership and housekeeping.
 *
 * <p>Each setting is defined here once, with its type and default, and found through {@link SettingsProvider}.
 */
public final class NodeSettings implements SettingsProvider {

    /** The default lease TTL: long enough to survive a slow object store, short enough to fail over. */
    public static final long DEFAULT_LEASE_TTL_MILLIS = 30_000L;

    /** The production descriptor refresh interval; see {@link #DESCRIPTOR_REFRESH_INTERVAL_MILLIS}. */
    public static final long DEFAULT_DESCRIPTOR_REFRESH_INTERVAL_MILLIS = 300_000L;

    /** The production janitor interval; see {@link #JANITOR_INTERVAL_MILLIS}. */
    public static final long DEFAULT_JANITOR_INTERVAL_MILLIS = 60_000L;

    /** The production head-verification interval; see {@link #HEAD_VERIFY_INTERVAL_MILLIS}. */
    public static final long DEFAULT_HEAD_VERIFY_INTERVAL_MILLIS = 3_600_000L;

    /**
     * Where to write the readiness line once the node is serving. Optional. A JVM system property, not a setting: it is
     * read before the node's settings are.
     */
    public static final String READY_FILE_PROPERTY = "serverless.ready.file";

    /** The node's roles: any of {@link NodeContext#ROLE_INGEST} and {@link NodeContext#ROLE_SEARCH}, both when unset, neither when set empty. */
    public static final Setting<String> ROLES = Setting.simpleString("serverless.roles", Property.NodeScope);

    /**
     * Refuse to start on a plaintext transport.
     *
     * <p>Forwarded requests carry the deployment's transport secret; on a plaintext transport it crosses
     * the wire in the clear, and anyone who can read the wire can forward writes as a member. Off by
     * default because the filesystem-backed, loopback-only configuration every test runs is exactly the
     * case where a plaintext transport is fine; a deployment whose transport port is on a reachable
     * network sets it and gets a refusal instead of a warning.
     */
    public static final Setting<Boolean> REQUIRE_SECURE_TRANSPORT = Setting.boolSetting(
        "serverless.transport.require_secure",
        false,
        Property.NodeScope
    );

    /** How long a lease stays valid after a renewal. */
    public static final Setting<Long> LEASE_TTL_MILLIS = Setting.longSetting(
        "serverless.lease.ttl_millis",
        NodeSettings.DEFAULT_LEASE_TTL_MILLIS,
        Property.NodeScope
    );

    /**
     * How long a node goes between re-reading the head of every writer shard it holds. Zero reads them on
     * every pass, which is the library default.
     *
     * <p>On here and at an hour, because a head cannot move away from a node whose renewals succeed
     * without that node being told: a taker revokes the owner's lease before taking its shard, and the
     * owner's next renewal fails on it (see {@code BlobLeaseMembership#revoke}), which re-reads every head at
     * once. What the scan still catches is a head released by someone other than its owner, which nothing in
     * this shell does: deleting an index gives its heads back, and the descriptor refresh of open indices,
     * which runs on its own interval, closes those shards. An hour bounds how long such a head would go
     * unnoticed. It was five minutes, which made the scan an idle open writer's largest cost: twelve head reads
     * an hour, now one.
     */
    public static final Setting<Long> HEAD_VERIFY_INTERVAL_MILLIS = Setting.longSetting(
        "serverless.heads.verify_interval_millis",
        NodeSettings.DEFAULT_HEAD_VERIFY_INTERVAL_MILLIS,
        Property.NodeScope
    );

    /**
     * How long a node goes between re-reading the descriptors of the indices it holds open, where nothing
     * else has read them meanwhile. Zero reads them on every pass, which is the library default.
     *
     * <p>A backstop, not the way changes arrive. Every operation on a shard -- write, get, search -- first passes
     * the incarnation fence, which reads the descriptor unless one was read within the last second and applies
     * what it read, so a mapping or settings change made on another node is in force for the first operation
     * after it. A shard nobody uses answers nothing, so nothing can see it behind; this pass only bounds how long
     * it stays so. At thirty seconds it was most of an idle node's store requests: 120 reads an hour per open
     * index, for shards nobody was asking anything of.
     */
    public static final Setting<Long> DESCRIPTOR_REFRESH_INTERVAL_MILLIS = Setting.longSetting(
        "serverless.descriptors.refresh_interval_millis",
        NodeSettings.DEFAULT_DESCRIPTOR_REFRESH_INTERVAL_MILLIS,
        Property.NodeScope
    );

    /**
     * How often each node cleans up after crashed nodes: a dead owner's fully published shards given back, the rollup
     * marks and claims it left cleared, and shards it left behind its log taken to be replayed, a bounded number a
     * pass. Zero runs no janitor.
     */
    public static final Setting<Long> JANITOR_INTERVAL_MILLIS = Setting.longSetting(
        "serverless.janitor.interval_millis",
        NodeSettings.DEFAULT_JANITOR_INTERVAL_MILLIS,
        Property.NodeScope
    );

    /**
     * Plugin classes to run, by name.
     *
     * <p><b>Not the same thing as installing one.</b> An installed plugin lives in {@code plugins/} with a
     * descriptor and its own jars, and {@link NodeContext} loads those through core's own loader. This
     * setting is for a class that is already on the node's classpath — a plugin a distribution ships rather
     * than one an operator installed — and it exists so that turning such a plugin on is configuration
     * rather than a change to the shell.
     */
    public static final Setting<List<String>> PLUGINS = Setting.listSetting(
        "serverless.plugins",
        List.of(),
        Function.identity(),
        Property.NodeScope
    );

    /** The index setting. */
    public static final Setting<String> SEGMENT_PLACEMENT_INDEX = new Setting<>(
        "index.serverless.segment_placement",
        "",
        SegmentPlacement::parse,
        Property.IndexScope,
        Property.Dynamic
    );

    /** The cluster setting, in the {@code cluster/config} register. */
    public static final Setting<String> SEGMENT_PLACEMENT_CLUSTER = Setting.simpleString(
        "serverless.segment_placement",
        Property.NodeScope
    );

    /** Used by {@link java.util.ServiceLoader}. */
    public NodeSettings() {}

    @Override
    public List<Setting<?>> settings() {
        return List.of(
            ROLES,
            REQUIRE_SECURE_TRANSPORT,
            LEASE_TTL_MILLIS,
            HEAD_VERIFY_INTERVAL_MILLIS,
            DESCRIPTOR_REFRESH_INTERVAL_MILLIS,
            JANITOR_INTERVAL_MILLIS,
            PLUGINS,
            SEGMENT_PLACEMENT_INDEX,
            SEGMENT_PLACEMENT_CLUSTER
        );
    }
}
