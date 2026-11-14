/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.schema;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.serverless.settings.SettingsProvider;

import java.util.List;

/**
 * Settings of schema sharing.
 *
 * <p>Each setting is defined here once, with its type and default, and found through {@link SettingsProvider}.
 */
public final class SchemaSettings implements SettingsProvider {

    /** Node setting that turns sharing on. */
    public static final Setting<Boolean> SHARING_ENABLED = Setting.boolSetting(
        "serverless.schema.sharing.enabled",
        SharedAnalysis.ENABLED_DEFAULT,
        Property.NodeScope
    );

    /** Used by {@link java.util.ServiceLoader}. */
    public SchemaSettings() {}

    @Override
    public List<Setting<?>> settings() {
        return List.of(SHARING_ENABLED);
    }
}
