/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.Version;
import org.opensearch.be.datafusion.stats.DataFusionStats;
import org.opensearch.be.datafusion.stats.NativeExecutorsStats;
import org.opensearch.be.datafusion.stats.PartitionGateStats;
import org.opensearch.be.datafusion.stats.RuntimeMetrics;
import org.opensearch.be.datafusion.stats.TaskMonitorStats;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Randomized tests for per-node stats equivalence.
 *
 * <p>Feature: datafusion-cluster-stats, Property 2: Per-node stats equivalence
 *
 * <p>For any {@link DataFusionStats} instance, when wrapped in a
 * {@link DataFusionStatsNodeResponse} and rendered via
 * {@link DataFusionStatsNodesResponse#toXContent}, the stats portion of the per-node
 * JSON is equivalent to rendering the same {@link DataFusionStats} directly via
 * {@code DataFusionStats.toXContent}.
 *
 * <p><b>Validates: Requirements 2.4</b>
 */
public class PerNodeStatsEquivalencePropertyTests extends OpenSearchTestCase {

    private static final int TRIES = 150;

    /** Metadata fields that were previously added by DataFusionStatsNodesResponse (now removed). */
    private static final Set<String> NODE_METADATA_FIELDS = Set.of();

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

    // ---- Property 2: Per-node stats equivalence ----

    /**
     * Feature: datafusion-cluster-stats, Property 2: Per-node stats equivalence
     *
     * <p>For any DataFusionStats instance, the stats portion rendered via
     * DataFusionStatsNodesResponse is equivalent to rendering via
     * DataFusionStats.toXContent directly.
     *
     * <p><b>Validates: Requirements 2.4</b>
     */
    public void testPerNodeStatsMatchDirectRendering() throws Exception {
        for (int i = 0; i < TRIES; i++) {
            DataFusionStats stats = dataFusionStats();

            // Step 1: Render DataFusionStats directly
            Map<String, Object> directMap = renderStatsDirect(stats);

            // Step 2: Wrap in a DataFusionStatsNodeResponse + DataFusionStatsNodesResponse and render
            Map<String, Object> wrappedMap = renderStatsViaNodesResponse(stats);

            // Step 3: Compare — they should be identical
            assertEquals(
                "Stats rendered directly via DataFusionStats.toXContent must equal "
                    + "the stats portion extracted from DataFusionStatsNodesResponse per-node entry",
                directMap,
                wrappedMap
            );
        }
    }

    // ---- Helper methods ----

    /**
     * Renders DataFusionStats directly: {@code builder.startObject(); stats.toXContent(builder, params); builder.endObject();}
     * then parses to a Map.
     */
    private Map<String, Object> renderStatsDirect(DataFusionStats stats) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), builder.toString(), false);
    }

    /**
     * Wraps the stats in a DataFusionStatsNodeResponse with a DiscoveryNode,
     * puts it in a DataFusionStatsNodesResponse, renders to JSON, extracts the
     * per-node entry from the "nodes" object, and removes the metadata fields
     * (name, host, transport_address) to isolate just the stats portion.
     *
     * <p>{@link #NODE_METADATA_FIELDS} is intentionally empty today: the response no
     * longer emits any per-node metadata, so the whole entry is the stats portion.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> renderStatsViaNodesResponse(DataFusionStats stats) throws Exception {
        // Create a DiscoveryNode for wrapping
        DiscoveryNode node = new DiscoveryNode(
            "test-node",
            "test-node-id",
            new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );

        // Wrap in node response
        DataFusionStatsNodeResponse nodeResponse = new DataFusionStatsNodeResponse(node, stats);

        // Wrap in nodes response
        DataFusionStatsNodesResponse nodesResponse = new DataFusionStatsNodesResponse(
            new ClusterName("test-cluster"),
            List.of(nodeResponse),
            Collections.emptyList()
        );

        // Render the full response to JSON
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        nodesResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        Map<String, Object> fullMap = XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), builder.toString(), false);

        // Extract the per-node entry
        Map<String, Object> nodesObj = (Map<String, Object>) fullMap.get("nodes");
        Map<String, Object> nodeEntry = (Map<String, Object>) nodesObj.get("test-node-id");

        // Remove metadata fields to isolate just the stats
        LinkedHashMap<String, Object> statsOnly = new LinkedHashMap<>(nodeEntry);
        for (String metaField : NODE_METADATA_FIELDS) {
            statsOnly.remove(metaField);
        }

        return statsOnly;
    }
}
