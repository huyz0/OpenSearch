/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.node;

import org.opensearch.common.settings.Settings;

import java.util.Locale;

/**
 * Where a publish puts new segment files: {@value #SEGMENT_BLOB}, one blob per segment under the shard, or
 * {@value #NODE_PACK}, one blob per node per window shared by every shard that publishes in it.
 *
 * <p>Changeable at runtime, the most specific setting first: the index's {@code index.serverless.segment_placement}, then the cluster's
 * {@code serverless.segment_placement} in the {@code cluster/config} register, then the node's
 * {@code serverless.segments.node_pack.enabled}, on by default. A change applies to each shard's next
 * publish and moves nothing already written: a manifest records every file's location, so one commit can name files in
 * both placements, and every reader and collector already handles both.
 */
public final class SegmentPlacement {

    /** One blob per segment, under the shard. */
    public static final String SEGMENT_BLOB = "segment_blob";
    /** One blob per node per window, shared by every shard that publishes in it. */
    public static final String NODE_PACK = "node_pack";

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
    public static void applyCluster(NodeContext node, Settings written) {
        if (node == null) {
            return;
        }
        node.descriptorRefresh.clusterSegmentPlacement = written.get(NodeSettings.SEGMENT_PLACEMENT_CLUSTER.getKey());
        node.descriptorRefresh.clusterBucketPolicy = written.get(org.opensearch.serverless.bucket.BucketSettings.CLUSTER_POLICY.getKey());
        LogCommitCheckpoint.applyCluster(node, written);
        final org.opensearch.serverless.usage.Guardrails guardrails = node.part(org.opensearch.serverless.usage.Guardrails.class);
        if (guardrails != null) {
            // The tenant levels a collector set, from the same read.
            guardrails.applyCluster(written);
        }
        if (node.push() != null) {
            node.push().applyCluster(written);
        }
        node.descriptorRefresh.clusterSegmentPlacementReadAtNanos = System.nanoTime();
        node.descriptorRefresh.clusterSegmentPlacementRead = true;
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
        final String index = indexSettings == null ? null : indexSettings.get(NodeSettings.SEGMENT_PLACEMENT_INDEX.getKey());
        return org.opensearch.serverless.settings.LayeredSetting.resolve(
            index,
            cluster,
            value -> NODE_PACK.equals(parse(value)),
            nodeDefault
        );
    }
}
