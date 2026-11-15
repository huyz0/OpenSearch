/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.serverless.settings.SettingsProvider;

import java.util.List;

/**
 * Settings of the object store a node reads and writes.
 *
 * <p>Each setting is defined here once, with its type and default, and found through {@link SettingsProvider}.
 */
public final class ObjectStoreSettings implements SettingsProvider {

    /** Which implementation backs the store: {@code fs} or {@code s3}. */
    public static final Setting<String> TYPE = Setting.simpleString("serverless.store.type", "fs", Property.NodeScope);

    /** For {@code s3}: the bucket. */
    public static final Setting<String> BUCKET = Setting.simpleString("serverless.store.bucket", Property.NodeScope);

    /** For {@code s3}: an endpoint override, for S3-compatible services. Optional. */
    public static final Setting<String> ENDPOINT = Setting.simpleString("serverless.store.endpoint", Property.NodeScope);

    /** For {@code s3}: the region. */
    public static final Setting<String> REGION = Setting.simpleString("serverless.store.region", "us-east-1", Property.NodeScope);

    /**
     * For {@code s3}: address the bucket by path rather than by hostname. Required by most S3-compatible
     * services, which do not have a wildcard DNS entry per bucket.
     */
    public static final Setting<Boolean> PATH_STYLE_ACCESS = Setting.boolSetting(
        "serverless.store.path_style_access",
        false,
        Property.NodeScope
    );

    /** Where the stand-in object store lives. Required. */
    public static final Setting<String> PATH = Setting.simpleString("serverless.store.path", Property.NodeScope);

    /**
     * Set to {@code true} to record that the operator knows an {@code s3} store's conditional writes have
     * not been proven linearizable by the conformance suite against their provider. Silences the boot warning and
     * nothing else: the node behaves the same either way.
     */
    public static final Setting<Boolean> PROVIDER_ACK = Setting.boolSetting("serverless.store.provider_ack", false, Property.NodeScope);

    /**
     * Whether the store charges every request to the index it was made for, on top of the node totals. The counters
     * are a few hundred bytes for each index the node has touched, and nothing is uploaded.
     */
    public static final Setting<Boolean> METERING = Setting.boolSetting("serverless.cost.metering", true, Property.NodeScope);

    /** How many indices metering tracks one by one before counting the rest on one line. */
    public static final Setting<Integer> METERING_MAX_INDICES = Setting.intSetting(
        "serverless.cost.metering.max_indices",
        StoreUsage.DEFAULT_MAX_INDICES,
        0,
        Property.NodeScope
    );

    /** Used by {@link java.util.ServiceLoader}. */
    public ObjectStoreSettings() {}

    @Override
    public List<Setting<?>> settings() {
        return List.of(TYPE, BUCKET, ENDPOINT, REGION, PATH_STYLE_ACCESS, PATH, PROVIDER_ACK, METERING, METERING_MAX_INDICES);
    }
}
