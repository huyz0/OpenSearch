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
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.serverless.storage.allocation.ReaderShardPlacementAllocationDecider;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeSelfWarmupSchedulerTaskTests extends OpenSearchTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private AtomicInteger submitCount;
    /** How many of the next {@code submitStateUpdateTask} calls should simulate failure. */
    private AtomicInteger failuresRemaining;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = mock(ClusterService.class);
        submitCount = new AtomicInteger();
        failuresRemaining = new AtomicInteger();

        RerouteService rerouteService = mock(RerouteService.class);
        doAnswer(invocation -> {
            ActionListener<ClusterState> listener = invocation.getArgument(2);
            listener.onResponse(clusterService.state());
            return null;
        }).when(rerouteService).reroute(anyString(), any(Priority.class), any());
        when(clusterService.getRerouteService()).thenReturn(rerouteService);

        // Runs the ClusterStateUpdateTask synchronously against whatever clusterService.state()
        // currently returns, exactly the way the real cluster-manager task queue would -- but
        // without needing a real (asynchronous) MasterService, so tests can assert immediately.
        doAnswer(invocation -> {
            submitCount.incrementAndGet();
            String source = invocation.getArgument(0);
            ClusterStateUpdateTask task = invocation.getArgument(1);
            if (failuresRemaining.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                task.onFailure(source, new RuntimeException("simulated failure"));
                return null;
            }
            ClusterState before = clusterService.state();
            ClusterState after = task.execute(before);
            task.clusterStateProcessed(source, before, after);
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private static DiscoveryNode node(String name, boolean reader) {
        Map<String, String> attributes = reader
            ? Map.of(ReaderShardPlacementAllocationDecider.READER_NODE_ATTRIBUTE, "true")
            : Map.of();
        return new DiscoveryNode(
            name,
            name,
            new TransportAddress(InetAddress.getLoopbackAddress(), 0),
            attributes,
            Set.of(),
            Version.CURRENT
        );
    }

    private static ClusterState stateWithLocalNode(DiscoveryNode localNode, String warmingValue) {
        Settings.Builder settings = Settings.builder();
        if (warmingValue != null) {
            settings.put(NodeWarmupCoordinator.WARMING_NAMES_SETTING_KEY, warmingValue);
        }
        Metadata metadata = Metadata.builder().transientSettings(settings.build()).build();
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build();
        return ClusterState.builder(new ClusterName("test")).metadata(metadata).nodes(nodes).build();
    }

    public void testSelfMarksAReaderNodeAsWarming() {
        DiscoveryNode local = node("reader-1", true);
        when(clusterService.state()).thenReturn(stateWithLocalNode(local, null));
        NodeWarmupCoordinator coordinator = new NodeWarmupCoordinator(clusterService);

        NodeSelfWarmupSchedulerTask task = new NodeSelfWarmupSchedulerTask(
            threadPool,
            TimeValue.timeValueDays(1),
            clusterService,
            coordinator,
            TimeValue.ZERO
        );
        try {
            task.evaluateForTesting();

            assertEquals(1, submitCount.get());
            assertTrue(task.isSelfMarkedForTesting());
        } finally {
            task.close();
        }
    }

    public void testDoesNotSelfMarkANonReaderNode() {
        DiscoveryNode local = node("writer-1", false);
        when(clusterService.state()).thenReturn(stateWithLocalNode(local, null));
        NodeWarmupCoordinator coordinator = new NodeWarmupCoordinator(clusterService);

        NodeSelfWarmupSchedulerTask task = new NodeSelfWarmupSchedulerTask(
            threadPool,
            TimeValue.timeValueDays(1),
            clusterService,
            coordinator,
            TimeValue.ZERO
        );
        try {
            task.evaluateForTesting();

            assertEquals(0, submitCount.get());
            assertTrue("a non-reader node has nothing to do, so it's considered done", task.isSelfMarkedForTesting());
        } finally {
            task.close();
        }
    }

    public void testDoesNotReMarkANodeAlreadyWarming() {
        DiscoveryNode local = node("reader-1", true);
        when(clusterService.state()).thenReturn(stateWithLocalNode(local, "reader-1"));
        NodeWarmupCoordinator coordinator = new NodeWarmupCoordinator(clusterService);

        NodeSelfWarmupSchedulerTask task = new NodeSelfWarmupSchedulerTask(
            threadPool,
            TimeValue.timeValueDays(1),
            clusterService,
            coordinator,
            TimeValue.ZERO
        );
        try {
            task.evaluateForTesting();

            assertEquals(0, submitCount.get());
            assertTrue(task.isSelfMarkedForTesting());
        } finally {
            task.close();
        }
    }

    public void testDoesNotAttemptAgainOnceSelfMarked() {
        DiscoveryNode local = node("reader-1", true);
        when(clusterService.state()).thenReturn(stateWithLocalNode(local, null));
        NodeWarmupCoordinator coordinator = new NodeWarmupCoordinator(clusterService);

        NodeSelfWarmupSchedulerTask task = new NodeSelfWarmupSchedulerTask(
            threadPool,
            TimeValue.timeValueDays(1),
            clusterService,
            coordinator,
            TimeValue.ZERO
        );
        try {
            task.evaluateForTesting();
            task.evaluateForTesting();
            task.evaluateForTesting();

            assertEquals(1, submitCount.get());
        } finally {
            task.close();
        }
    }

    public void testRetriesOnFailure() {
        DiscoveryNode local = node("reader-1", true);
        when(clusterService.state()).thenReturn(stateWithLocalNode(local, null));
        failuresRemaining.set(1);
        NodeWarmupCoordinator coordinator = new NodeWarmupCoordinator(clusterService);

        NodeSelfWarmupSchedulerTask task = new NodeSelfWarmupSchedulerTask(
            threadPool,
            TimeValue.timeValueDays(1),
            clusterService,
            coordinator,
            TimeValue.ZERO
        );
        try {
            task.evaluateForTesting(); // fails
            assertFalse(task.isSelfMarkedForTesting());
            task.evaluateForTesting(); // succeeds

            assertEquals(2, submitCount.get());
            assertTrue(task.isSelfMarkedForTesting());
        } finally {
            task.close();
        }
    }

    public void testAutoClearsAfterTheConfiguredDelay() throws InterruptedException {
        DiscoveryNode local = node("reader-1", true);
        when(clusterService.state()).thenReturn(stateWithLocalNode(local, null));
        CountDownLatch clearLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            submitCount.incrementAndGet();
            String source = invocation.getArgument(0);
            ClusterStateUpdateTask task = invocation.getArgument(1);
            ClusterState before = clusterService.state();
            ClusterState after = task.execute(before);
            task.clusterStateProcessed(source, before, after);
            if (NodeWarmupCoordinator.currentlyWarmingNames(after).isEmpty()) {
                clearLatch.countDown(); // the clear call empties the warming set back out.
            }
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        NodeWarmupCoordinator coordinator = new NodeWarmupCoordinator(clusterService);

        NodeSelfWarmupSchedulerTask task = new NodeSelfWarmupSchedulerTask(
            threadPool,
            TimeValue.timeValueDays(1),
            clusterService,
            coordinator,
            TimeValue.timeValueMillis(1)
        );
        try {
            task.evaluateForTesting();

            assertTrue("auto-clear should fire shortly after the configured delay", clearLatch.await(10, TimeUnit.SECONDS));
        } finally {
            task.close();
        }
    }
}
