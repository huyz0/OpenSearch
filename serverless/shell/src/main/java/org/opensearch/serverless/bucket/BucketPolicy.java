/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.bucket;

import org.opensearch.common.settings.Settings;

import java.util.Locale;

/**
 * Whether an index is grouped into a bucket: one setting, per index or for the cluster, the index's winning.
 *
 * <p>{@code auto} groups an index while it is small: only a one-shard index with no unit layout is placed, and nothing
 * forces a big index into a bucket. {@code off} keeps an index on its own. Unset everywhere, the node's
 * {@link org.opensearch.serverless.bucket.BucketSettings#ENABLED} decides. The setting only decides placement; which head owns an index follows its
 * descriptor on every node whatever this says, so changing it never splits ownership.
 */
public final class BucketPolicy {

    /** Group while small. */
    public static final String AUTO = "auto";
    /** Never group. */
    public static final String OFF = "off";

    private BucketPolicy() {}

    static String parse(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        final String lower = value.toLowerCase(Locale.ROOT);
        if (AUTO.equals(lower) == false && OFF.equals(lower) == false) {
            throw new IllegalArgumentException("bucket must be [" + AUTO + "] or [" + OFF + "], got [" + value + "]");
        }
        return lower;
    }

    /**
     * Checks a value given for either setting.
     *
     * @param value the value
     * @throws IllegalArgumentException if it is neither {@code auto} nor {@code off}
     */
    public static void check(String value) {
        parse(value);
    }

    /**
     * Returns whether an index with these settings is grouped into a bucket, when it is small enough to be.
     *
     * @param indexSettings the index's settings, or null
     * @param cluster the cluster setting's value, or null when unset or unreadable
     * @param nodeDefault the node's own setting
     * @return true to group it
     */
    public static boolean wanted(Settings indexSettings, String cluster, boolean nodeDefault) {
        final String index = indexSettings == null ? null : indexSettings.get(BucketSettings.INDEX_POLICY.getKey());
        return org.opensearch.serverless.settings.LayeredSetting.resolve(index, cluster, value -> AUTO.equals(parse(value)), nodeDefault);
    }

    /**
     * Returns the group an index is bucketed with.
     *
     * @param indexName the index
     * @param indexSettings its settings, or null
     * @return the group
     */
    public static String group(String indexName, Settings indexSettings) {
        final String group = indexSettings == null ? null : indexSettings.get(BucketSettings.GROUP.getKey());
        return group == null || group.isEmpty() ? indexName : group;
    }
}
