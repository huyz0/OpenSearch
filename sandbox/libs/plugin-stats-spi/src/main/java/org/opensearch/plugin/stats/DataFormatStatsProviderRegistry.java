/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * JVM-wide registry of {@link DataFormatStatsProvider} instances.
 *
 * <p>Each data-format plugin registers its provider here on construction. The registry
 * acts as a thin shared lookup so engines can self-register their per-shard trackers
 * with the right provider via {@link #get(String)}.
 *
 * <p><b>Scope:</b> this registry is per-classloader, not JVM-global. Each data-format plugin
 * bundles its own copy of this SPI library so that no plugin has to extend another purely to
 * borrow the jar, which means a node running several format plugins has several independent
 * registry instances. That is sound only because every use is intra-plugin: a plugin registers
 * exactly one provider under its own format name and only that plugin's REST/transport actions
 * look that name up. Do not add a cross-plugin lookup (e.g. iterating {@link #all()} to report
 * on formats owned by other plugins) — it would silently see only the calling plugin's
 * providers. A genuinely shared registry would first need this library to move somewhere every
 * format plugin already sits below, such as the server's {@code org.opensearch.plugin.stats}
 * package where {@code NativeAllocatorStatsRegistry} already lives.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class DataFormatStatsProviderRegistry {

    public static final DataFormatStatsProviderRegistry INSTANCE = new DataFormatStatsProviderRegistry();

    private final ConcurrentMap<String, DataFormatStatsProvider<?>> providers = new ConcurrentHashMap<>();

    private DataFormatStatsProviderRegistry() {}

    /** Registers a provider. First writer wins for a given format name. */
    public void register(DataFormatStatsProvider<?> provider) {
        providers.putIfAbsent(provider.formatName(), provider);
    }

    /** Returns the provider for a format, or {@code null} if no plugin has registered it. */
    public DataFormatStatsProvider<?> get(String formatName) {
        return providers.get(formatName);
    }

    /** Returns an unmodifiable snapshot of all registered providers. */
    public Collection<DataFormatStatsProvider<?>> all() {
        return Collections.unmodifiableCollection(providers.values());
    }
}
