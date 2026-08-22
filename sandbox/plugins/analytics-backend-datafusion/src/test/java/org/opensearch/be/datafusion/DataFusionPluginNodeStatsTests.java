/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.stats.AnalyticsBackendTaskCancellationStats;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.nativebridge.spi.AnalyticsBackendNativeMemoryStats;
import org.opensearch.plugins.PluginNodeStats;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Covers this plugin's {@code _nodes/stats} contribution through the generic
 * {@link PluginNodeStats} path.
 *
 * <p>Both stats types used to be reached by core through product-named {@code SearchBackEndPlugin}
 * SPI methods ({@code getAnalyticsBackendTaskCancellationStats} /
 * {@code getAnalyticsBackendNativeMemoryStats}), which forced the types themselves onto the
 * {@code :server} classpath and let {@code Node} surface only the first backend plugin's values.
 * They now travel {@link org.opensearch.plugins.Plugin#nodeStats()} plus
 * {@link org.opensearch.plugins.Plugin#getNamedWriteables()}, which is what these tests pin.
 */
public class DataFusionPluginNodeStatsTests extends OpenSearchTestCase {

    /**
     * Both contributions must be registered as {@link PluginNodeStats} named writeables, because
     * {@code NodeStats} deserializes each per-node plugin payload via
     * {@code readNamedWriteable(PluginNodeStats.class)}. An unregistered name is silently skipped by
     * the coordinator, which would drop these stats without any error.
     */
    public void testNamedWriteablesCoverBothContributions() {
        List<NamedWriteableRegistry.Entry> entries = new DataFusionPlugin().getNamedWriteables();

        Set<String> names = entries.stream().map(e -> e.name).collect(Collectors.toSet());
        assertEquals(Set.of(AnalyticsBackendTaskCancellationStats.WRITEABLE_NAME, AnalyticsBackendNativeMemoryStats.WRITEABLE_NAME), names);
        for (NamedWriteableRegistry.Entry entry : entries) {
            assertEquals("entries must be registered under the PluginNodeStats category", PluginNodeStats.class, entry.categoryClass);
        }
    }

    /**
     * {@code nodeStats()} contributes exactly the two payloads, each under its own writeable name.
     * When the native runtime is unavailable each falls back to a zeroed / {@code -1} snapshot rather
     * than throwing, which is what the removed suppliers did and what keeps a failed native read from
     * failing the whole {@code _nodes/stats} call.
     */
    public void testNodeStatsContributesBothPayloads() {
        List<PluginNodeStats> contributions = new DataFusionPlugin().nodeStats();

        assertEquals(2, contributions.size());
        assertEquals(
            Set.of(AnalyticsBackendTaskCancellationStats.WRITEABLE_NAME, AnalyticsBackendNativeMemoryStats.WRITEABLE_NAME),
            contributions.stream().map(PluginNodeStats::getWriteableName).collect(Collectors.toSet())
        );
    }

    /**
     * End-to-end of the wire path a coordinator walks: write each contribution as a named writeable
     * and read it back through a registry built from {@code getNamedWriteables()}. This is the step
     * that the old SPI methods never had to satisfy, since core carried the types itself.
     */
    public void testContributionsRoundTripThroughTheirOwnRegistry() throws IOException {
        DataFusionPlugin plugin = new DataFusionPlugin();
        NamedWriteableRegistry registry = new NamedWriteableRegistry(plugin.getNamedWriteables());

        for (PluginNodeStats stats : plugin.nodeStats()) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.writeNamedWriteable(stats);
                try (StreamInput raw = out.bytes().streamInput(); StreamInput in = new NamedWriteableAwareStreamInput(raw, registry)) {
                    PluginNodeStats readBack = in.readNamedWriteable(PluginNodeStats.class);
                    assertEquals(stats.getWriteableName(), readBack.getWriteableName());
                    assertEquals(stats, readBack);
                }
            }
        }
    }
}
