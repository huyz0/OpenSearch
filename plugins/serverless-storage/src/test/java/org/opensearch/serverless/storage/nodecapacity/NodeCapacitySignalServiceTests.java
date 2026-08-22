/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.scaletozero.ScaleToZeroCandidatesSchedulerTask;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exercises {@link NodeCapacitySignalService#evaluate()} against a real {@link ClusterState}
 * (routing table + index metadata), rather than just the model classes' serialization -- covers
 * unassigned-shard counting, writer/reader split, per-node aggregation, and serverless-storage
 * index filtering. Idle/hot-affinity flagging is not exercised here: {@link
 * #scaleToZeroCandidatesSchedulerTask} never evaluates (a deliberately huge interval), so {@link
 * ScaleToZeroCandidatesSchedulerTask#latestCandidates()} stays empty and every assigned shard is
 * treated as "no idleness data" -- the same code path a freshly-started node capacity service is
 * always in until scale-to-zero's own first successful evaluation.
 */
public class NodeCapacitySignalServiceTests extends OpenSearchTestCase {

    private static final String SERVERLESS_INDEX = "serverless-idx";
    private static final String CLASSIC_INDEX = "classic-idx";

    private TestThreadPool threadPool;
    private ScaleToZeroCandidatesSchedulerTask scaleToZeroCandidatesSchedulerTask;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        // Huge interval -- never fires during the test, so latestCandidates() stays at its default
        // empty list; only NodeCapacitySignalService.evaluate() itself is under test here.
        scaleToZeroCandidatesSchedulerTask = new ScaleToZeroCandidatesSchedulerTask(
            threadPool,
            TimeValue.timeValueDays(1),
            mock(Client.class),
            mock(ClusterService.class)
        );
    }

    @Override
    public void tearDown() throws Exception {
        scaleToZeroCandidatesSchedulerTask.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private static DiscoveryNode node(String id) {
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), java.util.Map.of(), java.util.Set.of(), Version.CURRENT);
    }

    private NodeCapacitySignalService newService(ClusterState state) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        return newService(clusterService);
    }

    private NodeCapacitySignalService newService(ClusterService clusterService) {
        return new NodeCapacitySignalService(
            threadPool,
            TimeValue.timeValueDays(1),
            clusterService,
            scaleToZeroCandidatesSchedulerTask,
            -1L, // reader cache affinity disabled -- not under test here
            10
        );
    }

    public void testCountsUnassignedAndAssignedShardsSeparatelyByRole() {
        IndexMetadata serverlessIndex = IndexMetadata.builder(SERVERLESS_INDEX)
            .settings(settings(Version.CURRENT).put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true).build())
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();
        IndexMetadata classicIndex = IndexMetadata.builder(CLASSIC_INDEX)
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        DiscoveryNode writerNode = node("writer-node");
        DiscoveryNode readerNode = node("reader-node");

        Index serverlessIndexRef = new Index(SERVERLESS_INDEX, serverlessIndex.getIndexUUID());
        ShardId writerShardId = new ShardId(serverlessIndexRef, 0);
        ShardId readerShardId = new ShardId(serverlessIndexRef, 1);

        // Shard 0: writer copy, started on writer-node.
        ShardRouting writerShard = TestShardRouting.newShardRouting(
            writerShardId,
            "writer-node",
            null,
            true,
            false,
            ShardRoutingState.STARTED,
            null
        );
        // Shard 1: reader copy, started on reader-node.
        ShardRouting readerShard = TestShardRouting.newShardRouting(
            readerShardId,
            "reader-node",
            null,
            false,
            true,
            ShardRoutingState.STARTED,
            null
        );
        // A second reader copy of shard 0, unassigned -- e.g. a search replica core hasn't placed yet.
        ShardRouting unassignedReaderShard = TestShardRouting.newShardRouting(
            writerShardId,
            null,
            false,
            true,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );

        IndexRoutingTable serverlessRoutingTable = IndexRoutingTable.builder(serverlessIndexRef)
            .addIndexShard(new IndexShardRoutingTable.Builder(writerShardId).addShard(writerShard).addShard(unassignedReaderShard).build())
            .addIndexShard(new IndexShardRoutingTable.Builder(readerShardId).addShard(readerShard).build())
            .build();

        RoutingTable routingTable = RoutingTable.builder()
            .add(serverlessRoutingTable)
            .addAsNew(classicIndex) // classic index's own (unassigned) shard must never be counted
            .build();

        Metadata metadata = Metadata.builder().put(serverlessIndex, false).put(classicIndex, false).build();
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(writerNode)
            .add(readerNode)
            .localNodeId("writer-node")
            .clusterManagerNodeId("writer-node")
            .build();

        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(discoveryNodes)
            .build();

        NodeCapacitySignalService service = newService(state);
        service.evaluateForTesting();
        NodeCapacitySignal signal = service.latestSignal();

        // Writer: one assigned shard (writer-node), zero unassigned.
        assertEquals(1, signal.writer().nodeCount());
        assertEquals(0, signal.writer().unassignedShardCount());
        assertEquals("writer-node", signal.writer().nodes().get(0).nodeId());
        assertEquals(1, signal.writer().nodes().get(0).assignedShardCount());

        // Reader: one assigned shard (reader-node) + one unassigned (the second copy of shard 0).
        assertEquals(1, signal.reader().nodeCount());
        assertEquals(1, signal.reader().unassignedShardCount());
        assertEquals("reader-node", signal.reader().nodes().get(0).nodeId());
        assertEquals(1, signal.reader().nodes().get(0).assignedShardCount());
        assertEquals(1, (int) signal.reader().unassignedByIndex().get(SERVERLESS_INDEX));

        // No idleness data available (scale-to-zero task never evaluated) -- every assigned shard
        // reads as "not idle," so no node is flagged as fully idle.
        assertFalse(signal.writer().nodes().get(0).allShardsIdle());
        assertFalse(signal.reader().nodes().get(0).allShardsIdle());
    }

    public void testNonServerlessIndexIsIgnoredEntirely() {
        IndexMetadata classicIndex = IndexMetadata.builder(CLASSIC_INDEX)
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Metadata metadata = Metadata.builder().put(classicIndex, false).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(classicIndex).build();
        DiscoveryNode local = node("only-node");
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(local)
            .localNodeId("only-node")
            .clusterManagerNodeId("only-node")
            .build();

        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(discoveryNodes)
            .build();

        NodeCapacitySignalService service = newService(state);
        service.evaluateForTesting();
        NodeCapacitySignal signal = service.latestSignal();

        assertEquals(0, signal.writer().nodeCount());
        assertEquals(0, signal.writer().unassignedShardCount());
        assertEquals(0, signal.reader().nodeCount());
        assertEquals(0, signal.reader().unassignedShardCount());
    }

    public void testNonClusterManagerNodeNeverEvaluates() {
        IndexMetadata serverlessIndex = IndexMetadata.builder(SERVERLESS_INDEX)
            .settings(settings(Version.CURRENT).put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Metadata metadata = Metadata.builder().put(serverlessIndex, false).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(serverlessIndex).build();
        DiscoveryNode local = node("not-the-cluster-manager");
        DiscoveryNode other = node("the-real-cluster-manager");
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(local)
            .add(other)
            .localNodeId("not-the-cluster-manager")
            .clusterManagerNodeId("the-real-cluster-manager")
            .build();

        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(discoveryNodes)
            .build();

        NodeCapacitySignalService service = newService(state);
        service.evaluateForTesting();

        // Never elected -- evaluate() returns early, latestSignal() stays at its initial empty value.
        assertEquals(NodeCapacitySignal.empty(), service.latestSignal());
    }

    /**
     * Regression test: a warming mark left behind by a node that departed the cluster (crashed
     * before {@code NodeWarmupCoordinator.clearWarming}, or was replaced by a differently-named
     * node) must be swept every tick, exactly like a stale drain-exclude entry -- otherwise it
     * permanently blocks reader allocation to any future node that reuses the departed name.
     */
    public void testStaleWarmingNameForADepartedNodeIsSweptOnEvaluate() throws Exception {
        Settings.Builder transientSettings = Settings.builder()
            .put(NodeWarmupCoordinator.WARMING_NAMES_SETTING_KEY, "live-node,departed-node");
        Metadata metadata = Metadata.builder().transientSettings(transientSettings.build()).build();
        DiscoveryNode local = node("live-node");
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(local)
            .localNodeId("live-node")
            .clusterManagerNodeId("live-node")
            .build();

        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(metadata)
            .routingTable(RoutingTable.builder().build())
            .nodes(discoveryNodes)
            .build();

        // A real (test) ClusterService is required here, not a mock -- the sweep now runs through
        // DrainCoordinator/NodeWarmupCoordinator's own ClusterStateUpdateTask (see their javadoc for
        // why), which a plain mock can't execute.
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        clusterService.setRerouteService((reason, priority, listener) -> listener.onResponse(clusterService.state()));
        ClusterServiceUtils.setState(clusterService, state);

        NodeCapacitySignalService service = newService(clusterService);
        service.evaluateForTesting();

        // The sweep's cluster-state task runs asynchronously on the (real) cluster-manager task
        // queue -- assertBusy waits for it rather than racing it.
        assertBusy(() -> assertEquals(Set.of("live-node"), NodeWarmupCoordinator.currentlyWarmingNames(clusterService.state())));

        clusterService.close();
    }
}
