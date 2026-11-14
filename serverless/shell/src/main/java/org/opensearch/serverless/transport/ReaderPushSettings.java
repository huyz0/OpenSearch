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
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.serverless.settings.SettingsProvider;

import java.util.List;

/**
 * Settings of what writers push to the search nodes serving their shards: commits, tail overlays and refresh visibility.
 *
 * <p>Each setting is defined here once, with its type and default, and found through {@link SettingsProvider}.
 */
public final class ReaderPushSettings implements SettingsProvider {

    /** Whether this node, as a writer, pushes the commits it publishes to the shard's search nodes. */
    public static final Setting<Boolean> COMMIT_PUSH_ENABLED = Setting.boolSetting(
        "serverless.search.commit_push.enabled",
        true,
        Property.NodeScope
    );

    /** The most bytes of a commit's new files one push carries to each search node; 0 sends the commit alone. */
    public static final Setting<ByteSizeValue> COMMIT_PUSH_INLINE_BYTES = Setting.byteSizeSetting(
        "serverless.search.commit_push.inline_bytes",
        new ByteSizeValue(CommitPush.DEFAULT_INLINE_BYTES),
        Property.NodeScope
    );

    /** Whether a reader moved onto a pushed commit reuses the descriptor it holds when the writer's is no newer. */
    public static final Setting<Boolean> COMMIT_PUSH_REUSE_DESCRIPTOR = Setting.boolSetting(
        "serverless.search.commit_push.reuse_descriptor",
        true,
        Property.NodeScope
    );

    /** Whether a search node skips the periodic register look for readers its live writer pushes. */
    public static final Setting<Boolean> COMMIT_PUSH_TRUST = Setting.boolSetting(
        "serverless.search.commit_push.trust",
        true,
        Property.NodeScope
    );

    /** The longest a reader goes without a register look while it is skipped, in milliseconds. */
    public static final Setting<Long> COMMIT_PUSH_TRUST_BACKSTOP_MILLIS = Setting.longSetting(
        "serverless.search.commit_push.trust_backstop_millis",
        ReaderRegisterLooks.BACKSTOP_MILLIS,
        Property.NodeScope
    );

    /**
     * The node's default for whether writes are searchable before they are published: writers push what they
     * acknowledge to the search nodes serving the shard, and those search it on top of the commit. On by default.
     */
    public static final Setting<Boolean> TAIL_OVERLAY_ENABLED = Setting.boolSetting(
        "serverless.search.tail_overlay.enabled",
        true,
        Property.NodeScope
    );

    /**
     * How long a shard whose writes its search nodes already see may wait to be published, when its index sets no
     * refresh interval: the node's publish age is raised to this. Publishing then decides only what a new reader opens
     * and how much a successor replays, not how soon a write is searchable, and each publish is PUTs.
     */
    public static final Setting<Long> TAIL_OVERLAY_PUBLISH_MAX_AGE_MILLIS = Setting.longSetting(
        "serverless.search.tail_overlay.publish_max_age_millis",
        TailOverlays.DEFAULT_PUBLISH_MAX_AGE_MILLIS,
        Property.NodeScope
    );

    /** The node setting for how long acknowledged records wait for others to share a message. */
    public static final Setting<Long> TAIL_OVERLAY_PUSH_LINGER_MILLIS = Setting.longSetting(
        "serverless.search.tail_overlay.push_linger_millis",
        TailPush.LINGER_MILLIS,
        Property.NodeScope
    );

    /** The index setting, which wins over the cluster's: {@code true} or {@code false}, dynamic. */
    public static final Setting<String> TAIL_OVERLAY_INDEX = new Setting<>(
        "index.serverless.tail_overlay",
        "",
        TailOverlays::parse,
        Property.IndexScope,
        Property.Dynamic
    );

    /** The cluster setting, in the {@code cluster/config} register, which wins over the node's. */
    public static final Setting<String> TAIL_OVERLAY_CLUSTER = Setting.simpleString("serverless.tail_overlay", Property.NodeScope);

    /** The node's default. */
    public static final Setting<String> REFRESH_VISIBILITY = Setting.simpleString(
        "serverless.search.refresh_visibility",
        Property.NodeScope
    );

    /** The cluster setting, which wins over the node's. */
    public static final Setting<String> REFRESH_VISIBILITY_CLUSTER = Setting.simpleString(
        "serverless.refresh_visibility",
        Property.NodeScope
    );

    /** Used by {@link java.util.ServiceLoader}. */
    public ReaderPushSettings() {}

    @Override
    public List<Setting<?>> settings() {
        return List.of(
            COMMIT_PUSH_ENABLED,
            COMMIT_PUSH_INLINE_BYTES,
            COMMIT_PUSH_REUSE_DESCRIPTOR,
            COMMIT_PUSH_TRUST,
            COMMIT_PUSH_TRUST_BACKSTOP_MILLIS,
            TAIL_OVERLAY_ENABLED,
            TAIL_OVERLAY_PUBLISH_MAX_AGE_MILLIS,
            TAIL_OVERLAY_PUSH_LINGER_MILLIS,
            TAIL_OVERLAY_INDEX,
            TAIL_OVERLAY_CLUSTER,
            REFRESH_VISIBILITY,
            REFRESH_VISIBILITY_CLUSTER
        );
    }
}
