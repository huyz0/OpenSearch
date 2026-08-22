/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Node-level native-allocator stats types shared between the OpenSearch server and the plugin that
 * owns the node's native allocator (today: {@code arrow-base}).
 *
 * <p>Key types:
 * <ul>
 *   <li>{@link org.opensearch.plugin.stats.NativeAllocatorPoolStats} — per-pool + jemalloc runtime
 *       snapshot, contributed through the generic {@link org.opensearch.plugins.PluginNodeStats}
 *       path</li>
 *   <li>{@link org.opensearch.plugin.stats.NativeAllocatorStatsRegistry} — component-published
 *       holder that lets the server look the supplier up without linking the owning plugin</li>
 * </ul>
 *
 * <p><b>Split package warning.</b> {@code org.opensearch.plugin.stats} also exists in
 * {@code :sandbox:libs:plugin-stats-spi}, which is bundled into plugin zips. Under plugin
 * classloader isolation, a package split across the core classpath and a plugin jar means the two
 * halves are never visible to each other as one package; anything package-private cannot be shared,
 * and a duplicated class name would resolve differently depending on which classloader asked. Keep
 * this half small: new plugin-specific stats types belong in the plugin that produces them, wired in
 * through {@link org.opensearch.plugins.Plugin#nodeStats()} and
 * {@link org.opensearch.plugins.Plugin#getNamedWriteables()}, not here.
 */
package org.opensearch.plugin.stats;
