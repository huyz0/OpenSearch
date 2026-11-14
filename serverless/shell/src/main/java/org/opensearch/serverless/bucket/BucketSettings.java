/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.bucket;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.settings.SettingsProvider;

import java.util.List;

/**
 * The node settings that turn buckets on and size them.
 */
public final class BucketSettings implements SettingsProvider {

    /** Whether new small indices are placed in buckets. Off until the bucket meter shows the savings without a cost. */
    public static final Setting<Boolean> ENABLED = Setting.boolSetting("serverless.buckets.enabled", false, Property.NodeScope);

    /**
     * The most indices one bucket holds: a hard ceiling, not what fills a bucket. Buckets fill by data size
     * ({@link #TARGET_BYTES}); this only bounds a bucket of many tiny or empty indices, whose manifest every member
     * read and every checkpoint has to go through.
     */
    public static final Setting<Integer> MAX_MEMBERS = Setting.intSetting(
        "serverless.buckets.max_members",
        BucketSettings.DEFAULT_MAX_MEMBERS,
        Property.NodeScope
    );

    /** The data size at which a stripe stops adding indices to its open bucket and opens the next. */
    public static final Setting<Long> TARGET_BYTES = Setting.longSetting(
        "serverless.buckets.target_bytes",
        BucketSettings.DEFAULT_TARGET_BYTES,
        Property.NodeScope
    );

    /** The stored manifest size past which a bucket takes no new members, whatever its data size and member count. */
    public static final Setting<Integer> MAX_MANIFEST_BYTES = Setting.intSetting(
        "serverless.buckets.max_manifest_bytes",
        BucketSettings.DEFAULT_MAX_MANIFEST_BYTES,
        Property.NodeScope
    );

    /**
     * How bucket manifests are written: {@code auto} (the default) for the compact encoding, compressed with zstd only
     * when the manifest is large enough and zstd shrinks it enough to pay; a {@link ManifestCodec} name to always use
     * that codec; or {@code json} for the encoding nodes without the compact one read. Reading takes any, whatever this
     * says.
     */
    public static final Setting<String> MANIFEST_CODEC = Setting.simpleString(
        "serverless.buckets.manifest_codec",
        "auto",
        Property.NodeScope
    );

    /** The index setting. */
    public static final Setting<String> INDEX_POLICY = new Setting<>(
        "index.serverless.bucket",
        "",
        BucketPolicy::parse,
        Property.IndexScope,
        Property.Dynamic
    );

    /** The cluster setting, in the {@code cluster/config} register. */
    public static final Setting<String> CLUSTER_POLICY = Setting.simpleString("serverless.bucket", Property.NodeScope);

    /** The index setting naming the group an index is bucketed with, such as its tenant; the index name by default. */
    public static final Setting<String> GROUP = Setting.simpleString("index.serverless.bucket_group", Property.IndexScope);

    /** The default for {@link org.opensearch.serverless.bucket.BucketSettings#TARGET_BYTES}: 1 GB. */
    public static final long DEFAULT_TARGET_BYTES = 1L << 30;

    /** The default for {@link org.opensearch.serverless.bucket.BucketSettings#MAX_MANIFEST_BYTES}: 2 MB, about 6K members in the compact encoding. */
    public static final int DEFAULT_MAX_MANIFEST_BYTES = 2 << 20;

    /** The default for {@link org.opensearch.serverless.bucket.BucketSettings#MAX_MEMBERS}. */
    public static final int DEFAULT_MAX_MEMBERS = 1024;

    /** The default for {@link org.opensearch.serverless.bucket.BucketSettings#MANIFEST_CODEC}. */
    public static final ManifestCodec DEFAULT_MANIFEST_CODEC = ManifestCodec.ZSTD;

    /**
     * Returns how bucket manifests are written.
     *
     * @param settings the node settings
     * @return the codec, or null for JSON
     * @throws IllegalArgumentException if the setting names no codec
     */
    public static ManifestCodec manifestCodec(Settings settings) {
        final String name = MANIFEST_CODEC.get(settings).trim();
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
        return MANIFEST_CODEC.get(settings).trim().equalsIgnoreCase("auto");
    }

    /** Used by {@link java.util.ServiceLoader}. */
    public BucketSettings() {}

    /**
     * Returns whether buckets are on.
     *
     * @param settings the node settings
     * @return true if new small indices go into buckets
     */
    public static boolean enabled(Settings settings) {
        return ENABLED.get(settings);
    }

    /**
     * Returns the most indices one bucket holds.
     *
     * @param settings the node settings
     * @return the cap, at least one
     */
    public static int maxMembers(Settings settings) {
        return Math.max(1, MAX_MEMBERS.get(settings));
    }

    /**
     * Returns the data size at which a bucket takes no new members.
     *
     * @param settings the node settings
     * @return the target, at least one byte
     */
    public static long targetBytes(Settings settings) {
        return Math.max(1L, TARGET_BYTES.get(settings));
    }

    /**
     * Returns the stored manifest size past which a bucket takes no new members.
     *
     * @param settings the node settings
     * @return the cap, at least one byte
     */
    public static int maxManifestBytes(Settings settings) {
        return Math.max(1, MAX_MANIFEST_BYTES.get(settings));
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

    @Override
    public List<Setting<?>> settings() {
        return List.of(ENABLED, MAX_MEMBERS, TARGET_BYTES, MAX_MANIFEST_BYTES, MANIFEST_CODEC, INDEX_POLICY, CLUSTER_POLICY, GROUP);
    }
}
