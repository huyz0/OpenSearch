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
import org.opensearch.be.datafusion.stats.SpillStats;
import org.opensearch.be.datafusion.stats.TaskMonitorStats;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Randomized tests for Writeable round-trip serialization of transport objects.
 *
 * <p>Feature: datafusion-cluster-stats, Property 5: Writeable round-trip for transport objects
 *
 * <p>For any {@link DataFusionStatsNodesRequest}, {@link DataFusionStatsNodeRequest},
 * {@link DataFusionStatsNodeResponse}, or {@link DataFusionStatsNodesResponse},
 * serializing to a {@code StreamOutput} and deserializing from the resulting
 * {@code StreamInput} produces an object equal to the original.
 *
 * <p><b>Validates: Requirements 5.4, 5.5, 5.6</b>
 */
public class WriteableRoundTripPropertyTests extends OpenSearchTestCase {

    private static final int TRIES = 100;

    /** Valid stat section names for generating filter sets. */
    private static final List<String> ALL_SECTIONS = List.of(
        "io_runtime",
        "cpu_runtime",
        "coordinator_reduce",
        "query_execution",
        "stream_next",
        "plan_setup",
        "fragment_executor_gate"
    );

    // ═══════════════════════════════════════════════════════════════════
    // Generators
    // ═══════════════════════════════════════════════════════════════════

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

    private TaskMonitorStats taskMonitorStats() {
        return new TaskMonitorStats(nonNegLong(), nonNegLong(), nonNegLong(), nonNegLong(), nonNegLong());
    }

    private PartitionGateStats partitionGateStats() {
        return new PartitionGateStats(
            "fragment_executor_gate",
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

    /**
     * Produces a {@link SpillStats}. Threaded into the {@link #dataFusionStats()}
     * generator so the round-trip property exercises the present-spill-stats branch
     * of {@code writeOptionalWriteable}, not just the {@code null} case. Catches
     * regressions in the SpillStats wire format that the fixed-input unit test in
     * {@code SpillStatsTests} cannot.
     */
    private SpillStats spillStats() {
        return new SpillStats(
            "/spill/" + randomAlphaOfLengthBetween(3, 20),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            nonNegLong(),
            randomLongBetween(0L, 10_000L)
        );
    }

    private Map<String, TaskMonitorStats> taskMonitors() {
        Map<String, TaskMonitorStats> monitors = new LinkedHashMap<>();
        monitors.put("coordinator_reduce", taskMonitorStats());
        monitors.put("query_execution", taskMonitorStats());
        monitors.put("stream_next", taskMonitorStats());
        monitors.put("plan_setup", taskMonitorStats());
        return monitors;
    }

    private DataFusionStats dataFusionStats() {
        // Cover both wire-format branches: with-spill exercises the present-Writeable case,
        // without-spill exercises the writeOptionalWriteable false-byte case. Likewise for
        // the CPU runtime, which has its own explicit boolean marker on the wire.
        boolean withCpu = randomBoolean();
        boolean withSpill = randomBoolean();
        return new DataFusionStats(
            new NativeExecutorsStats(runtimeMetrics(), withCpu ? runtimeMetrics() : null, taskMonitors()),
            partitionGateStats(),
            null,
            withSpill ? spillStats() : null
        );
    }

    /** Produces a set of stat section names (possibly empty). */
    private Set<String> statsToRetrieve() {
        return new HashSet<>(randomSubsetOf(randomIntBetween(0, ALL_SECTIONS.size()), ALL_SECTIONS));
    }

    /** Produces an array of node IDs (possibly empty). */
    private String[] nodeIds() {
        int count = randomIntBetween(0, 5);
        String[] ids = new String[count];
        for (int i = 0; i < count; i++) {
            ids[i] = randomAlphaOfLengthBetween(3, 10);
        }
        return ids;
    }

    /** Produces a DiscoveryNode with a stable loopback address. */
    private DiscoveryNode discoveryNode() throws IOException {
        return new DiscoveryNode(
            randomAlphaOfLengthBetween(3, 10),
            randomAlphaOfLengthBetween(5, 12),
            new TransportAddress(InetAddress.getByName("127.0.0.1"), randomIntBetween(1024, 65535)),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );
    }

    // ═══════════════════════════════════════════════════════════════════
    // Property 5: Writeable round-trip for DataFusionStatsNodesRequest
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Feature: datafusion-cluster-stats, Property 5: Writeable round-trip for transport objects
     *
     * <p>For any DataFusionStatsNodesRequest, serialize → deserialize produces an
     * object with equal nodesIds and statsToRetrieve.
     *
     * <p><b>Validates: Requirements 5.4, 5.5, 5.6</b>
     */
    public void testNodesRequestRoundTrip() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            DataFusionStatsNodesRequest original = new DataFusionStatsNodesRequest(nodeIds(), statsToRetrieve());

            // Serialize
            BytesStreamOutput out = new BytesStreamOutput();
            original.writeTo(out);

            // Deserialize
            StreamInput in = out.bytes().streamInput();
            DataFusionStatsNodesRequest deserialized = new DataFusionStatsNodesRequest(in);

            // Verify nodesIds
            assertEquals(
                "nodesIds must survive round-trip",
                new HashSet<>(Arrays.asList(original.nodesIds())),
                new HashSet<>(Arrays.asList(deserialized.nodesIds()))
            );

            // Verify statsToRetrieve
            assertEquals("statsToRetrieve must survive round-trip", original.getStatsToRetrieve(), deserialized.getStatsToRetrieve());
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Property 5: Writeable round-trip for DataFusionStatsNodeRequest
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Feature: datafusion-cluster-stats, Property 5: Writeable round-trip for transport objects
     *
     * <p>For any DataFusionStatsNodeRequest, serialize → deserialize produces an
     * object with equal statsToRetrieve.
     *
     * <p><b>Validates: Requirements 5.4, 5.5, 5.6</b>
     */
    public void testNodeRequestRoundTrip() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            // Create a NodesRequest first, then derive the NodeRequest from it
            DataFusionStatsNodesRequest nodesRequest = new DataFusionStatsNodesRequest(new String[0], statsToRetrieve());
            DataFusionStatsNodeRequest original = new DataFusionStatsNodeRequest(nodesRequest);

            // Serialize
            BytesStreamOutput out = new BytesStreamOutput();
            original.writeTo(out);

            // Deserialize
            StreamInput in = out.bytes().streamInput();
            DataFusionStatsNodeRequest deserialized = new DataFusionStatsNodeRequest(in);

            // Verify statsToRetrieve
            assertEquals("statsToRetrieve must survive round-trip", original.getStatsToRetrieve(), deserialized.getStatsToRetrieve());
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Property 5: Writeable round-trip for DataFusionStatsNodeResponse
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Feature: datafusion-cluster-stats, Property 5: Writeable round-trip for transport objects
     *
     * <p>For any DataFusionStatsNodeResponse, serialize → deserialize produces an
     * equal object (node + stats).
     *
     * <p><b>Validates: Requirements 5.4, 5.5, 5.6</b>
     */
    public void testNodeResponseRoundTrip() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            DataFusionStatsNodeResponse original = new DataFusionStatsNodeResponse(discoveryNode(), dataFusionStats());

            // Serialize
            BytesStreamOutput out = new BytesStreamOutput();
            original.writeTo(out);

            // Deserialize
            StreamInput in = out.bytes().streamInput();
            DataFusionStatsNodeResponse deserialized = new DataFusionStatsNodeResponse(in);

            // Verify equality (DataFusionStatsNodeResponse has equals/hashCode)
            assertEquals("NodeResponse must survive round-trip", original, deserialized);
            assertEquals("hashCode must be consistent after round-trip", original.hashCode(), deserialized.hashCode());

            // Verify individual components
            assertEquals("DiscoveryNode must survive round-trip", original.getNode(), deserialized.getNode());
            assertEquals("DataFusionStats must survive round-trip", original.getStats(), deserialized.getStats());
        }
    }

    /**
     * Feature: datafusion-cluster-stats, Property 5: Writeable round-trip for transport objects
     *
     * <p>For a DataFusionStatsNodeResponse with null stats, serialize → deserialize
     * preserves the null stats.
     *
     * <p><b>Validates: Requirements 5.4, 5.5, 5.6</b>
     */
    public void testNodeResponseRoundTripWithNullStats() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            DataFusionStatsNodeResponse original = new DataFusionStatsNodeResponse(discoveryNode(), null);

            // Serialize
            BytesStreamOutput out = new BytesStreamOutput();
            original.writeTo(out);

            // Deserialize
            StreamInput in = out.bytes().streamInput();
            DataFusionStatsNodeResponse deserialized = new DataFusionStatsNodeResponse(in);

            // Verify equality
            assertEquals("NodeResponse with null stats must survive round-trip", original, deserialized);
            assertEquals("DiscoveryNode must survive round-trip", original.getNode(), deserialized.getNode());
            assertNull("Null stats must remain null after round-trip", deserialized.getStats());
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Property 5: Writeable round-trip for DataFusionStatsNodesResponse
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Feature: datafusion-cluster-stats, Property 5: Writeable round-trip for transport objects
     *
     * <p>For any DataFusionStatsNodesResponse, serialize → deserialize produces an
     * object with equal node responses.
     *
     * <p><b>Validates: Requirements 5.4, 5.5, 5.6</b>
     */
    public void testNodesResponseRoundTrip() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            List<DataFusionStatsNodeResponse> nodeResponses = new ArrayList<>();
            nodeResponses.add(new DataFusionStatsNodeResponse(discoveryNode(), dataFusionStats()));
            nodeResponses.add(new DataFusionStatsNodeResponse(discoveryNode(), dataFusionStats()));

            DataFusionStatsNodesResponse original = new DataFusionStatsNodesResponse(
                new ClusterName("test-cluster"),
                nodeResponses,
                Collections.emptyList()
            );

            // Serialize
            BytesStreamOutput out = new BytesStreamOutput();
            original.writeTo(out);

            // Deserialize
            StreamInput in = out.bytes().streamInput();
            DataFusionStatsNodesResponse deserialized = new DataFusionStatsNodesResponse(in);

            // Verify cluster name
            assertNotNull("ClusterName must not be null after round-trip", deserialized.getClusterName());
            assertEquals("ClusterName must survive round-trip", original.getClusterName(), deserialized.getClusterName());

            // Verify node responses count
            assertEquals("Number of node responses must survive round-trip", original.getNodes().size(), deserialized.getNodes().size());

            // Verify each node response
            for (int n = 0; n < original.getNodes().size(); n++) {
                assertEquals(
                    "Node response at index " + n + " must survive round-trip",
                    original.getNodes().get(n),
                    deserialized.getNodes().get(n)
                );
            }

            // Verify failures count (empty in this test)
            assertEquals("Failures list size must survive round-trip", original.failures().size(), deserialized.failures().size());
        }
    }

    /**
     * Feature: datafusion-cluster-stats, Property 5: Writeable round-trip for transport objects
     *
     * <p>For a DataFusionStatsNodesResponse with empty node list, serialize → deserialize
     * preserves the empty state.
     *
     * <p><b>Validates: Requirements 5.4, 5.5, 5.6</b>
     */
    public void testNodesResponseRoundTripEmpty() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            DataFusionStatsNodesResponse original = new DataFusionStatsNodesResponse(
                new ClusterName("empty-cluster"),
                Collections.emptyList(),
                Collections.emptyList()
            );

            // Serialize
            BytesStreamOutput out = new BytesStreamOutput();
            original.writeTo(out);

            // Deserialize
            StreamInput in = out.bytes().streamInput();
            DataFusionStatsNodesResponse deserialized = new DataFusionStatsNodesResponse(in);

            // Verify
            assertEquals("ClusterName must survive round-trip", original.getClusterName(), deserialized.getClusterName());
            assertEquals("Empty node list must survive round-trip", 0, deserialized.getNodes().size());
            assertEquals("Empty failures list must survive round-trip", 0, deserialized.failures().size());
        }
    }
}
