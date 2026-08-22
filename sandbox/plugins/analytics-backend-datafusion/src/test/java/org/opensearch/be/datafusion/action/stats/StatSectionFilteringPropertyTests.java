/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.be.datafusion.stats.DataFusionStats;
import org.opensearch.be.datafusion.stats.NativeExecutorsStats;
import org.opensearch.be.datafusion.stats.PartitionGateStats;
import org.opensearch.be.datafusion.stats.RuntimeMetrics;
import org.opensearch.be.datafusion.stats.TaskMonitorStats;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Randomized tests for stat section filtering correctness.
 *
 * <p>Feature: datafusion-cluster-stats, Property 3: Stat section filtering correctness
 *
 * <p>For any non-empty subset of valid stat section names and any {@link DataFusionStats}
 * instance, when the subset is applied as a filter via
 * {@link TransportDataFusionStatsAction#filteredStats}, the rendered JSON for that node
 * contains exactly the sections in the subset and no others.
 *
 * <p><b>Validates: Requirements 3.1, 3.2, 3.4, 5.3</b>
 */
public class StatSectionFilteringPropertyTests extends OpenSearchTestCase {

    private static final int TRIES = 150;
    private static final int TRIES_EMPTY_FILTER = 100;

    /**
     * The 7 stat section names the generated {@link DataFusionStats} below can populate.
     * The REST layer additionally accepts {@code adaptive_budget} and {@code disk_spill};
     * those are out of scope here because this generator always leaves them null.
     */
    private static final List<String> ALL_SECTIONS = List.of(
        "io_runtime",
        "cpu_runtime",
        "coordinator_reduce",
        "query_execution",
        "stream_next",
        "plan_setup",
        "fragment_executor_gate"
    );

    // ---- Object generators ----

    private long nonNegLong() {
        return randomLongBetween(0, Long.MAX_VALUE / 2);
    }

    private RuntimeMetrics runtimeMetrics() {
        return new RuntimeMetrics(
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong()
        );
    }

    /** RuntimeMetrics with {@code workersCount > 0}, marking the CPU runtime as present. */
    private RuntimeMetrics runtimeMetricsWithPositiveWorkers() {
        return new RuntimeMetrics(
            randomLongBetween(1, Long.MAX_VALUE / 2),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong()
        );
    }

    private TaskMonitorStats taskMonitorStats() {
        return new TaskMonitorStats(nonNegLong(), nonNegLong(), nonNegLong(), nonNegLong(), nonNegLong());
    }

    private Map<String, TaskMonitorStats> taskMonitors() {
        Map<String, TaskMonitorStats> monitors = new LinkedHashMap<>();
        monitors.put("coordinator_reduce", taskMonitorStats());
        monitors.put("query_execution", taskMonitorStats());
        monitors.put("stream_next", taskMonitorStats());
        monitors.put("plan_setup", taskMonitorStats());
        return monitors;
    }

    /** DataFusionStats with all sections populated (CPU runtime present). */
    private DataFusionStats dataFusionStatsFullCpuPresent() {
        return new DataFusionStats(
            new NativeExecutorsStats(runtimeMetrics(), runtimeMetricsWithPositiveWorkers(), taskMonitors()),
            new PartitionGateStats("fragment_executor_gate", 12, 3, 100, 50, 0, 12, 0, 0),
            null,
            null
        );
    }

    /** DataFusionStats with CPU runtime absent. */
    private DataFusionStats dataFusionStatsFullCpuAbsent() {
        return new DataFusionStats(
            new NativeExecutorsStats(runtimeMetrics(), null, taskMonitors()),
            new PartitionGateStats("fragment_executor_gate", 12, 3, 100, 50, 0, 12, 0, 0),
            null,
            null
        );
    }

    /** Combined DataFusionStats generator (CPU present or absent). */
    private DataFusionStats dataFusionStats() {
        return randomBoolean() ? dataFusionStatsFullCpuPresent() : dataFusionStatsFullCpuAbsent();
    }

    /** Produces a non-empty subset of the 7 stat section names. */
    private Set<String> statSectionSubset() {
        return new HashSet<>(randomSubsetOf(randomIntBetween(1, ALL_SECTIONS.size()), ALL_SECTIONS));
    }

    // ---- Property 3: Stat section filtering correctness ----

    /**
     * Feature: datafusion-cluster-stats, Property 3: Stat section filtering correctness
     *
     * <p>For any non-empty subset of valid stat section names and any DataFusionStats
     * instance, filtered output contains exactly the requested sections and no others.
     *
     * <p><b>Validates: Requirements 3.1, 3.2, 3.4, 5.3</b>
     */
    public void testFilteredStatsContainsExactlyRequestedSections() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            DataFusionStats stats = dataFusionStats();
            Set<String> requestedSections = statSectionSubset();

            DataFusionStats filtered = TransportDataFusionStatsAction.filteredStats(stats, requestedSections);

            // Collect all top-level keys from the rendered JSON
            Set<String> actualSections = new HashSet<>(renderToMap(filtered).keySet());

            // Determine expected sections: only sections that were both requested
            // AND present in the original stats will appear in the output.
            Set<String> expectedSections = computeExpectedSections(stats, requestedSections);

            // Verify: actual sections == expected sections
            assertEquals(
                "Filtered JSON must contain exactly the requested (and available) sections. Requested: " + requestedSections,
                expectedSections,
                actualSections
            );
        }
    }

    /**
     * Feature: datafusion-cluster-stats, Property 3: Stat section filtering correctness
     * (complement check)
     *
     * <p>For any non-empty subset of valid stat section names and any DataFusionStats
     * instance, sections NOT in the filter are absent from the rendered JSON.
     *
     * <p><b>Validates: Requirements 3.1, 3.2, 3.4, 5.3</b>
     */
    public void testFilteredStatsExcludesUnrequestedSections() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            DataFusionStats stats = dataFusionStats();
            Set<String> requestedSections = statSectionSubset();

            DataFusionStats filtered = TransportDataFusionStatsAction.filteredStats(stats, requestedSections);
            Map<String, Object> root = renderToMap(filtered);

            // Sections NOT requested must be absent from the output
            Set<String> excludedSections = new HashSet<>(ALL_SECTIONS);
            excludedSections.removeAll(requestedSections);

            for (String excluded : excludedSections) {
                assertFalse(
                    "Section '" + excluded + "' should NOT be present in filtered output. Requested: " + requestedSections,
                    root.containsKey(excluded)
                );
            }
        }
    }

    /**
     * Feature: datafusion-cluster-stats, Property 3: Stat section filtering correctness
     * (empty/null filter returns all)
     *
     * <p>When the filter is null or empty, all sections from the original stats are preserved.
     *
     * <p><b>Validates: Requirements 3.4, 5.3</b>
     */
    public void testEmptyFilterReturnsAllSections() {
        for (int i = 0; i < TRIES_EMPTY_FILTER; i++) {
            DataFusionStats stats = dataFusionStats();

            // Null filter returns same object
            DataFusionStats filteredNull = TransportDataFusionStatsAction.filteredStats(stats, null);
            assertSame("Null filter must return the original stats object", stats, filteredNull);

            // Empty filter returns same object
            DataFusionStats filteredEmpty = TransportDataFusionStatsAction.filteredStats(stats, Set.of());
            assertSame("Empty filter must return the original stats object", stats, filteredEmpty);
        }
    }

    // ---- Helper methods ----

    /**
     * Computes the set of section names expected in the filtered JSON output.
     * A section appears only if it was requested AND the original stats had
     * non-null data for that section.
     */
    private Set<String> computeExpectedSections(DataFusionStats stats, Set<String> requested) {
        Set<String> expected = new HashSet<>();
        for (String section : requested) {
            switch (section) {
                case "cpu_runtime":
                    if (stats.getNativeExecutorsStats() != null && stats.getNativeExecutorsStats().getCpuRuntime() != null) {
                        expected.add(section);
                    }
                    break;
                case "io_runtime":
                    if (stats.getNativeExecutorsStats() != null && stats.getNativeExecutorsStats().getIoRuntime() != null) {
                        expected.add(section);
                    }
                    break;
                case "coordinator_reduce":
                case "query_execution":
                case "stream_next":
                case "plan_setup":
                    if (stats.getNativeExecutorsStats() != null && stats.getNativeExecutorsStats().getTaskMonitors().get(section) != null) {
                        expected.add(section);
                    }
                    break;
                case "fragment_executor_gate":
                    if (stats.getFragmentExecutorGateStats() != null) {
                        expected.add(section);
                    }
                    break;
                default:
                    break;
            }
        }
        return expected;
    }

    private Map<String, Object> renderToMap(DataFusionStats stats) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        if (stats != null) {
            stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.endObject();
        return XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), builder.toString(), false);
    }
}
