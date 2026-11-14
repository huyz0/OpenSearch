/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.routing;

import org.opensearch.common.settings.Setting;
import org.opensearch.serverless.settings.SettingsProvider;

import java.util.List;

/**
 * Settings of how an index is split into units.
 *
 * <p>Each setting is defined here once, with its type and default, and found through {@link SettingsProvider}.
 */
public final class RoutingSettings implements SettingsProvider {

    /**
     * The index setting that selects the mode. Read from a create-index request and kept in the index's descriptor, never
     * in its index settings, so it has no index scope and is not validated with them.
     */
    public static final Setting<String> SHARD_MODE = Setting.simpleString("index.serverless.shard_mode");

    /** Used by {@link java.util.ServiceLoader}. */
    public RoutingSettings() {}

    @Override
    public List<Setting<?>> settings() {
        return List.of(SHARD_MODE);
    }
}
