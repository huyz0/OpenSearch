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

    /**
     * The most indices one bucket holds: a hard ceiling, not what fills a bucket. Buckets fill by data size
     * ({@link #TARGET_BYTES}); this only bounds a bucket of many tiny or empty indices, whose manifest every member
     * read and every checkpoint has to go through.
     */
    public static final String MAX_MEMBERS = "serverless.buckets.max_members";

    /** The data size at which a stripe stops adding indices to its open bucket and opens the next. */
    public static final String TARGET_BYTES = "serverless.buckets.target_bytes";

    /** The default for {@link #TARGET_BYTES}: 1 GB. */
    public static final long DEFAULT_TARGET_BYTES = 1L << 30;

    /** The stored manifest size past which a bucket takes no new members, whatever its data size and member count. */
    public static final String MAX_MANIFEST_BYTES = "serverless.buckets.max_manifest_bytes";

    /** The default for {@link #MAX_MANIFEST_BYTES}: 2 MB, about 6K members in the compact encoding. */
    public static final int DEFAULT_MAX_MANIFEST_BYTES = 2 << 20;

    /** The default for {@link #MAX_MEMBERS}. */
    public static final int DEFAULT_MAX_MEMBERS = 1024;

    /**
     * How bucket manifests are written: {@code auto} (the default) for the compact encoding, compressed with zstd only
     * when the manifest is large enough and zstd shrinks it enough to pay; a {@link ManifestCodec} name to always use
     * that codec; or {@code json} for the encoding nodes without the compact one read. Reading takes any, whatever this
     * says.
     */
    public static final String MANIFEST_CODEC = "serverless.buckets.manifest_codec";

    /** The default for {@link #MANIFEST_CODEC}. */
    public static final ManifestCodec DEFAULT_MANIFEST_CODEC = ManifestCodec.ZSTD;

    /**
     * Returns how bucket manifests are written.
     *
     * @param settings the node settings
     * @return the codec, or null for JSON
     * @throws IllegalArgumentException if the setting names no codec
     */
    public static ManifestCodec manifestCodec(Settings settings) {
        final String name = settings.get(MANIFEST_CODEC, "auto").trim();
        if (name.equalsIgnoreCase("auto")) {
            return DEFAULT_MANIFEST_CODEC;
        }
        return name.equalsIgnoreCase("json") ? null : ManifestCodec.of(name);
    }

    /**
     * Returns whether manifests are compressed only when that pays, which is what {@code auto} means.
     *
     * @param settings the node settings
     * @return true unless the setting names a codec or json
     */
    public static boolean compressOnlyIfWorthIt(Settings settings) {
        return settings.get(MANIFEST_CODEC, "auto").trim().equalsIgnoreCase("auto");
    }

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

    /**
     * Returns the data size at which a bucket takes no new members.
     *
     * @param settings the node settings
     * @return the target, at least one byte
     */
    public static long targetBytes(Settings settings) {
        return Math.max(1L, settings.getAsLong(TARGET_BYTES, DEFAULT_TARGET_BYTES));
    }

    /**
     * Returns the stored manifest size past which a bucket takes no new members.
     *
     * @param settings the node settings
     * @return the cap, at least one byte
     */
    public static int maxManifestBytes(Settings settings) {
        return Math.max(1, settings.getAsInt(MAX_MANIFEST_BYTES, DEFAULT_MAX_MANIFEST_BYTES));
    }

    /**
     * Returns the limits new members are placed by.
     *
     * @param settings the node settings
     * @return the limits
     */
    public static BucketAssigner.Limits limits(Settings settings) {
        return new BucketAssigner.Limits(targetBytes(settings), maxMembers(settings), maxManifestBytes(settings));
    }
}
