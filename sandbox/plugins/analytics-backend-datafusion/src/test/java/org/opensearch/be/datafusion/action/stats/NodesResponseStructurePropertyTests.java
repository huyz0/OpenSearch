/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Randomized tests for NodesResponse rendering correct structure.
 *
 * <p>Feature: datafusion-cluster-stats, Property 1: NodesResponse renders correct structure
 *
 * <p>For any set of successful {@link DataFusionStatsNodeResponse} instances and
 * {@link FailedNodeException} failures, the rendered {@link DataFusionStatsNodesResponse}
 * JSON SHALL contain: a {@code _nodes} object with {@code total} equal to successes + failures,
 * {@code successful} equal to the number of successful responses, and {@code failed} equal to
 * the number of failures; a {@code cluster_name} string; and a {@code nodes} object where each
 * key is a node ID. Per-node entries deliberately do NOT carry {@code name}, {@code host} or
 * {@code transport_address} (KNN pattern — no IP exposure).
 *
 * <p><b>Validates: Requirements 1.4, 1.5, 2.1, 2.2</b>
 */
public class NodesResponseStructurePropertyTests extends OpenSearchTestCase {

    private static final int TRIES = 150;

    // ---- Generators ----

    /** Produces a non-negative long suitable for metrics. */
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

    /** Produces a DataFusionStats instance with all sections populated. */
    private DataFusionStats dataFusionStats() {
        Map<String, TaskMonitorStats> monitors = new LinkedHashMap<>();
        monitors.put("coordinator_reduce", taskMonitorStats());
        monitors.put("query_execution", taskMonitorStats());
        monitors.put("stream_next", taskMonitorStats());
        monitors.put("plan_setup", taskMonitorStats());
        return new DataFusionStats(
            new NativeExecutorsStats(runtimeMetrics(), null, monitors),
            new PartitionGateStats("fragment_executor_gate", 12, 3, 100, 50, 0, 12, 0, 0),
            null,
            null
        );
    }

    /** Produces a node ID string (alphanumeric, 5-15 chars). */
    private String nodeId() {
        return randomAlphaOfLengthBetween(5, 15);
    }

    /** Produces a node name string. */
    private String nodeName() {
        return "node-" + randomAlphaOfLengthBetween(3, 20);
    }

    /** Produces a valid IPv4 address as a string (using the 10.x.x.x range). */
    private String hostAddress() {
        return "10." + randomIntBetween(1, 254) + "." + randomIntBetween(1, 254) + "." + randomIntBetween(1, 254);
    }

    /** Produces a port number. */
    private int port() {
        return randomIntBetween(9200, 9400);
    }

    /** Produces a cluster name string. */
    private String clusterName() {
        return "cluster-" + randomAlphaOfLengthBetween(3, 20);
    }

    /** Produces {@code count} distinct node IDs. */
    private List<String> uniqueNodeIds(int count) {
        Set<String> ids = new LinkedHashSet<>();
        while (ids.size() < count) {
            ids.add(nodeId());
        }
        return new ArrayList<>(ids);
    }

    /**
     * Produces a list of DataFusionStatsNodeResponse instances (0 to 10 nodes).
     * Each node has a unique ID, name, host, and transport address.
     */
    private List<DataFusionStatsNodeResponse> nodeResponses() throws IOException {
        int count = randomIntBetween(0, 10);
        if (count == 0) {
            return Collections.emptyList();
        }
        List<String> ids = uniqueNodeIds(count);
        List<DataFusionStatsNodeResponse> responses = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            DiscoveryNode node = new DiscoveryNode(
                nodeName(),
                ids.get(i),
                new TransportAddress(InetAddress.getByName(hostAddress()), port()),
                Collections.emptyMap(),
                Collections.emptySet(),
                Version.CURRENT
            );
            responses.add(new DataFusionStatsNodeResponse(node, dataFusionStats()));
        }
        return responses;
    }

    /** Produces a list of FailedNodeException instances (0 to 5 failures). */
    private List<FailedNodeException> failures() {
        int count = randomIntBetween(0, 5);
        if (count == 0) {
            return Collections.emptyList();
        }
        List<FailedNodeException> failureList = new ArrayList<>(count);
        for (String id : uniqueNodeIds(count)) {
            failureList.add(new FailedNodeException(id, "node failure", new RuntimeException("test error")));
        }
        return failureList;
    }

    // ---- Property 1: NodesResponse renders correct structure ----

    /**
     * Feature: datafusion-cluster-stats, Property 1: NodesResponse renders correct structure
     *
     * <p>Verifies that {@code _nodes.total} = successes + failures,
     * {@code _nodes.successful} = successes count, and
     * {@code _nodes.failed} = failures count.
     *
     * <p><b>Validates: Requirements 1.4, 1.5, 2.1, 2.2</b>
     */
    public void testNodesHeaderCountsAreCorrect() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            List<DataFusionStatsNodeResponse> successResponses = nodeResponses();
            List<FailedNodeException> failureList = failures();
            ClusterName cluster = new ClusterName(clusterName());
            DataFusionStatsNodesResponse response = new DataFusionStatsNodesResponse(cluster, successResponses, failureList);

            Map<String, Object> root = renderAndParse(response);

            // Verify _nodes object
            Map<String, Object> nodesHeader = asMap(root.get("_nodes"));
            assertNotNull("_nodes object must be present", nodesHeader);

            int expectedTotal = successResponses.size() + failureList.size();
            assertEquals("_nodes.total must equal successes + failures", expectedTotal, asInt(nodesHeader.get("total")));
            assertEquals(
                "_nodes.successful must equal number of successful responses",
                successResponses.size(),
                asInt(nodesHeader.get("successful"))
            );
            assertEquals("_nodes.failed must equal number of failures", failureList.size(), asInt(nodesHeader.get("failed")));
        }
    }

    /**
     * Feature: datafusion-cluster-stats, Property 1: NodesResponse renders correct structure
     *
     * <p>Verifies that {@code cluster_name} is present and matches the input.
     *
     * <p><b>Validates: Requirements 1.5</b>
     */
    public void testClusterNameIsPresentAndCorrect() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            String clusterNameStr = clusterName();
            DataFusionStatsNodesResponse response = new DataFusionStatsNodesResponse(
                new ClusterName(clusterNameStr),
                nodeResponses(),
                failures()
            );

            Map<String, Object> root = renderAndParse(response);

            assertTrue("cluster_name field must be present", root.containsKey("cluster_name"));
            assertEquals("cluster_name must match the input cluster name", clusterNameStr, root.get("cluster_name"));
        }
    }

    /**
     * Feature: datafusion-cluster-stats, Property 1: NodesResponse renders correct structure
     *
     * <p>Verifies that the {@code nodes} object has exactly as many entries as
     * successful responses, and each entry is keyed by the node's ID.
     *
     * <p><b>Validates: Requirements 2.1</b>
     */
    public void testNodesObjectHasCorrectEntries() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            List<DataFusionStatsNodeResponse> successResponses = nodeResponses();
            DataFusionStatsNodesResponse response = new DataFusionStatsNodesResponse(
                new ClusterName(clusterName()),
                successResponses,
                failures()
            );

            Map<String, Object> root = renderAndParse(response);

            Map<String, Object> nodesObj = asMap(root.get("nodes"));
            assertNotNull("nodes object must be present", nodesObj);
            assertEquals(
                "nodes object must have exactly as many entries as successful responses",
                successResponses.size(),
                nodesObj.size()
            );

            // Verify each node entry is keyed by node ID
            for (DataFusionStatsNodeResponse nodeResp : successResponses) {
                String expectedId = nodeResp.getNode().getId();
                assertTrue("nodes object must contain entry for node ID: " + expectedId, nodesObj.containsKey(expectedId));
            }
        }
    }

    /**
     * Feature: datafusion-cluster-stats, Property 1: NodesResponse renders correct structure
     *
     * <p>Verifies that node entries do NOT contain {@code name}, {@code host},
     * or {@code transport_address} fields (KNN pattern — no IP exposure).
     *
     * <p><b>Validates: Security — no private IP leakage</b>
     */
    public void testEachNodeEntryDoesNotContainMetadataFields() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            List<DataFusionStatsNodeResponse> successResponses = nodeResponses();
            DataFusionStatsNodesResponse response = new DataFusionStatsNodesResponse(
                new ClusterName(clusterName()),
                successResponses,
                failures()
            );

            Map<String, Object> root = renderAndParse(response);
            Map<String, Object> nodesObj = asMap(root.get("nodes"));

            for (DataFusionStatsNodeResponse nodeResp : successResponses) {
                String nodeId = nodeResp.getNode().getId();
                Map<String, Object> nodeEntry = asMap(nodesObj.get(nodeId));
                assertNotNull("Node entry must exist for ID: " + nodeId, nodeEntry);

                // Verify metadata fields are NOT present
                assertFalse("Node entry must NOT contain 'name' field for node: " + nodeId, nodeEntry.containsKey("name"));
                assertFalse("Node entry must NOT contain 'host' field for node: " + nodeId, nodeEntry.containsKey("host"));
                assertFalse(
                    "Node entry must NOT contain 'transport_address' field for node: " + nodeId,
                    nodeEntry.containsKey("transport_address")
                );
            }
        }
    }

    // ---- Helper methods ----

    /** Renders the response to JSON and parses it back into a map. */
    private Map<String, Object> renderAndParse(DataFusionStatsNodesResponse response) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), builder.toString(), false);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Object value) {
        return (Map<String, Object>) value;
    }

    private int asInt(Object value) {
        return ((Number) value).intValue();
    }
}
