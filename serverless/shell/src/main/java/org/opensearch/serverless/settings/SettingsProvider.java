/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.settings;

import org.opensearch.common.settings.Setting;

import java.util.List;

/**
 * A feature package's settings: every key it reads, with its type, default and scope, defined once.
 *
 * <p>Implementations are listed in {@code META-INF/services/org.opensearch.serverless.settings.SettingsProvider} and
 * need a public no-argument constructor.
 */
public interface SettingsProvider {

    /**
     * The settings this package defines.
     *
     * @return the settings
     */
    List<Setting<?>> settings();
}
