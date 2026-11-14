/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * How the shell's settings are defined and found.
 *
 * <p>Each feature package defines its own settings, next to the code that reads them, in a {@code *Settings} class
 * that implements {@link org.opensearch.serverless.settings.SettingsProvider} -- the same shape as a plugin's
 * {@code getSettings()}. Providers are found with {@link java.util.ServiceLoader}, so adding a feature never edits a
 * shared list: it adds its class and one line to the services file. The build checks that every provider is listed.
 */
package org.opensearch.serverless.settings;
