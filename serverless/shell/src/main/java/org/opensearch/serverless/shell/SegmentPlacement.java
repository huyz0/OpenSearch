/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import java.util.Locale;

/**
 * Where a publish puts new segment files: {@value #SEGMENT_BLOB}, one blob per segment under the shard, or
 * {@value #NODE_PACK}, one blob per node per window shared by every shard that publishes in it.
 *
 * <p>Changeable at runtime, the most specific setting first: the index's {@value #INDEX_SETTING}, then the cluster's
 * {@value #CLUSTER_SETTING} in the {@code cluster/config} register, then the node's
 * {@code serverless.segments.node_pack.enabled}, then {@value #SEGMENT_BLOB}. A change applies to each shard's next
 * publish and moves nothing already written: a manifest records every file's location, so one commit can name files in
 * both placements, and every reader and collector already handles both.
 */
public final class SegmentPlacement {

    /** The index setting. */
    public static final String INDEX_SETTING = "index.serverless.segment_placement";
    /** The cluster setting, in the {@code cluster/config} register. */
    public static final String CLUSTER_SETTING = "serverless.segment_placement";
    /** One blob per segment, under the shard. */
    public static final String SEGMENT_BLOB = "segment_blob";
    /** One blob per node per window, shared by every shard that publishes in it. */
    public static final String NODE_PACK = "node_pack";

    /** The index setting's definition: dynamic, and only the two placements. */
    public static final Setting<String> INDEX = new Setting<>(
        INDEX_SETTING,
        "",
        SegmentPlacement::parse,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /** How long a node keeps the cluster setting it read before reading the register again. */
    static final long CLUSTER_REFRESH_NANOS = java.util.concurrent.TimeUnit.MINUTES.toNanos(5);

    private SegmentPlacement() {}

    /**
     * Checks a placement.
     *
     * @param value the value, or empty for unset
     * @return the value, lower-cased
     * @throws IllegalArgumentException if it is neither placement
     */
    static String parse(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        final String lower = value.toLowerCase(Locale.ROOT);
        if (SEGMENT_BLOB.equals(lower) == false && NODE_PACK.equals(lower) == false) {
            throw new IllegalArgumentException(
                "segment placement must be [" + SEGMENT_BLOB + "] or [" + NODE_PACK + "], got [" + value + "]"
            );
        }
        return lower;
    }

    /**
     * Checks a placement given for the cluster setting.
     *
     * @param value the value
     * @throws IllegalArgumentException if it is neither placement
     */
    public static void check(String value) {
        parse(value);
    }

    /**
     * Applies the cluster's settings as just written to this node's publishes.
     *
     * @param node the node, or null
     * @param written the register's settings
     */
    public static void applyCluster(ServerlessNode node, Settings written) {
        if (node == null) {
            return;
        }
        node.clusterSegmentPlacement = written.get(CLUSTER_SETTING);
        node.clusterSegmentPlacementReadAtNanos = System.nanoTime();
        node.clusterSegmentPlacementRead = true;
    }

    /**
     * Whether a publish of a shard of an index uses node packs.
     *
     * @param indexSettings the index's settings
     * @param cluster the cluster setting's value, or null when unset or unreadable
     * @param nodeDefault the node's own setting
     * @return true for node packs
     */
    static boolean nodePacks(Settings indexSettings, String cluster, boolean nodeDefault) {
        final String index = indexSettings == null ? null : indexSettings.get(INDEX_SETTING);
        for (String value : new String[] { index, cluster }) {
            if (value == null || value.isEmpty()) {
                continue;
            }
            try {
                return NODE_PACK.equals(parse(value));
            } catch (IllegalArgumentException e) {
                // A value the register holds that is neither: the next, less specific, setting decides.
            }
        }
        return nodeDefault;
    }
}
