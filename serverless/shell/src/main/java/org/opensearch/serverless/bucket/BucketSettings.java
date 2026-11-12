/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.bucket;

import org.opensearch.common.settings.Settings;

/**
 * The node settings that turn buckets on and size them.
 */
public final class BucketSettings {

    /** Whether new small indices are placed in buckets. Off until the bucket meter shows the savings without a cost. */
    public static final String ENABLED = "serverless.buckets.enabled";

    /** The most indices one bucket holds. */
    public static final String MAX_MEMBERS = "serverless.buckets.max_members";

    /** The default for {@link #MAX_MEMBERS}. */
    public static final int DEFAULT_MAX_MEMBERS = 256;

    private BucketSettings() {}

    /**
     * Returns whether buckets are on.
     *
     * @param settings the node settings
     * @return true if new small indices go into buckets
     */
    public static boolean enabled(Settings settings) {
        return settings.getAsBoolean(ENABLED, false);
    }

    /**
     * Returns the most indices one bucket holds.
     *
     * @param settings the node settings
     * @return the cap, at least one
     */
    public static int maxMembers(Settings settings) {
        return Math.max(1, settings.getAsInt(MAX_MEMBERS, DEFAULT_MAX_MEMBERS));
    }
}
