/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.serverless.settings.SettingsProvider;

import java.util.List;

/**
 * Settings of the writer engine.
 *
 * <p>Each setting is defined here once, with its type and default, and found through {@link SettingsProvider}.
 */
public final class WriterEngineSettings implements SettingsProvider {

    /**
     * Whether a writer keeps core's translog on local disk. Off, the writer keeps none ({@link DiscardingTranslogManager}): an
     * acknowledged write is durable in the node log, and nothing ever replays a local translog, which cost every open writer two
     * file descriptors and every acknowledged write a local fsync.
     */
    public static final Setting<Boolean> LOCAL_TRANSLOG = Setting.boolSetting(
        "serverless.translog.local",
        ServerlessWriterEngine.DEFAULT_LOCAL_TRANSLOG,
        Property.NodeScope
    );

    /** Used by {@link java.util.ServiceLoader}. */
    public WriterEngineSettings() {}

    @Override
    public List<Setting<?>> settings() {
        return List.of(LOCAL_TRANSLOG);
    }
}
