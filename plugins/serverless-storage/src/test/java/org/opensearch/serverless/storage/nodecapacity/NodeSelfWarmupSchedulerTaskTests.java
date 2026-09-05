/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity;

import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.serverless.storage.allocation.ReaderShardPlacementAllocationDecider;
import org.opensearch.serverless.storage.nodecapacity.action.NodeWarmupAction;
import org.opensearch.serverless.storage.nodecapacity.action.NodeWarmupRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exercises this task through a fake {@link org.opensearch.transport.client.Client} rather than a
 * mocked {@link ClusterService}: {@link NodeSelfWarmupSchedulerTask} dispatches {@link
 * NodeWarmupAction} requests (see its own class javadoc for why -- a direct {@link
 * NodeWarmupCoordinator} call would throw {@code NotClusterManagerException} on the non-cluster-
 * manager nodes this task actually runs on), so the fake client intercepting {@code doExecute} for
 * exactly that action is what these tests need to control, not the cluster-state task machinery.
 */
public class NodeSelfWarmupSchedulerTaskTests extends OpenSearchTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private AtomicInteger requestCount;
    /** How many of the next {@code NodeWarmupAction} dispatches should simulate failure. */
    private AtomicInteger failuresRemaining;
    private NoOpClient client;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = mock(ClusterService.class);
        requestCount = new AtomicInteger();
        failuresRemaining = new AtomicInteger();
        client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                requestCount.incrementAndGet();
                assertSame("must dispatch through NodeWarmupAction, not any other action", NodeWarmupAction.INSTANCE, action);
                @SuppressWarnings("unchecked")
                ActionListener<AcknowledgedResponse> typed = (ActionListener<AcknowledgedResponse>) listener;
                if (failuresRemaining.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                    typed.onFailure(new RuntimeException("simulated failure"));
                } else {
                    typed.onResponse(new AcknowledgedResponse(true));
                }
            }
        };
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private static DiscoveryNode node(String name, boolean reader) {
        Map<String, String> attributes = reader ? Map.of(ReaderShardPlacementAllocationDecider.READER_NODE_ATTRIBUTE, "true") : Map.of();
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

        NodeSelfWarmupSchedulerTask task = new NodeSelfWarmupSchedulerTask(
            threadPool,
            TimeValue.timeValueDays(1),
            clusterService,
            client,
            TimeValue.ZERO
        );
        try {
            task.evaluateForTesting();

            assertEquals(1, requestCount.get());
            assertTrue(task.isSelfMarkedForTesting());
        } finally {
            task.close();
        }
    }

    /**
     * Finding N-2. The {@code client.execute} that marks the node warming used to sit outside any
     * try/catch, with {@code markAttemptInFlight} cleared only inside the listener callbacks. A
     * <em>synchronous</em> throw -- a {@code NodeClosedException} during shutdown, or an
     * {@code IllegalStateException} from the action registry on a still-starting node, which is
     * precisely the window this task's own {@code Throwable} catch exists for -- escaped to that
     * catch, was logged once, and left the flag set for the process lifetime. The node then never
     * marked itself warming, never logged why again, and silently received reader shards while
     * genuinely cold: the exact inverse of the failure this task exists to prevent.
     */
    public void testASynchronousDispatchFailureDoesNotPinTheInFlightFlagForever() {
        DiscoveryNode local = node("reader-1", true);
        when(clusterService.state()).thenReturn(stateWithLocalNode(local, null));

        java.util.concurrent.atomic.AtomicBoolean throwSynchronously = new java.util.concurrent.atomic.AtomicBoolean(true);
        NoOpClient throwingClient = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                requestCount.incrementAndGet();
                if (throwSynchronously.get()) {
                    throw new IllegalStateException("simulated: action not registered yet on a still-starting node");
                }
                @SuppressWarnings("unchecked")
                ActionListener<AcknowledgedResponse> typed = (ActionListener<AcknowledgedResponse>) listener;
                typed.onResponse(new AcknowledgedResponse(true));
            }
        };

        NodeSelfWarmupSchedulerTask task = new NodeSelfWarmupSchedulerTask(
            threadPool,
            TimeValue.timeValueDays(1),
            clusterService,
            throwingClient,
            TimeValue.ZERO
        );
        try {
            expectThrows(IllegalStateException.class, task::evaluateForTesting);
            assertFalse("the failed attempt must not count as having marked the node", task.isSelfMarkedForTesting());

            // The next tick must be able to try again. Before the fix, markAttemptInFlight was still
            // true here and evaluate() returned immediately -- forever.
            throwSynchronously.set(false);
            task.evaluateForTesting();
            assertTrue("a later tick must be able to complete the self-mark", task.isSelfMarkedForTesting());
            assertEquals("both attempts must actually have reached the client", 2, requestCount.get());
        } finally {
            task.close();
        }
    }

    public void testDoesNotSelfMarkANonReaderNode() {
        DiscoveryNode local = node("writer-1", false);
        when(clusterService.state()).thenReturn(stateWithLocalNode(local, null));

        NodeSelfWarmupSchedulerTask task = new NodeSelfWarmupSchedulerTask(
            threadPool,
            TimeValue.timeValueDays(1),
            clusterService,
            client,
            TimeValue.ZERO
        );
        try {
            task.evaluateForTesting();

            assertEquals(0, requestCount.get());
            assertTrue("a non-reader node has nothing to do, so it's considered done", task.isSelfMarkedForTesting());
        } finally {
            task.close();
        }
    }

    public void testDoesNotReMarkANodeAlreadyWarming() {
        DiscoveryNode local = node("reader-1", true);
        when(clusterService.state()).thenReturn(stateWithLocalNode(local, "reader-1"));

        NodeSelfWarmupSchedulerTask task = new NodeSelfWarmupSchedulerTask(
            threadPool,
            TimeValue.timeValueDays(1),
            clusterService,
            client,
            TimeValue.ZERO
        );
        try {
            task.evaluateForTesting();

            assertEquals(0, requestCount.get());
            assertTrue(task.isSelfMarkedForTesting());
        } finally {
            task.close();
        }
    }

    public void testDoesNotAttemptAgainOnceSelfMarked() {
        DiscoveryNode local = node("reader-1", true);
        when(clusterService.state()).thenReturn(stateWithLocalNode(local, null));

        NodeSelfWarmupSchedulerTask task = new NodeSelfWarmupSchedulerTask(
            threadPool,
            TimeValue.timeValueDays(1),
            clusterService,
            client,
            TimeValue.ZERO
        );
        try {
            task.evaluateForTesting();
            task.evaluateForTesting();
            task.evaluateForTesting();

            assertEquals(1, requestCount.get());
        } finally {
            task.close();
        }
    }

    public void testRetriesOnFailure() {
        DiscoveryNode local = node("reader-1", true);
        when(clusterService.state()).thenReturn(stateWithLocalNode(local, null));
        failuresRemaining.set(1);

        NodeSelfWarmupSchedulerTask task = new NodeSelfWarmupSchedulerTask(
            threadPool,
            TimeValue.timeValueDays(1),
            clusterService,
            client,
            TimeValue.ZERO
        );
        try {
            task.evaluateForTesting(); // fails
            assertFalse(task.isSelfMarkedForTesting());
            task.evaluateForTesting(); // succeeds

            assertEquals(2, requestCount.get());
            assertTrue(task.isSelfMarkedForTesting());
        } finally {
            task.close();
        }
    }

    public void testAutoClearsAfterTheConfiguredDelay() throws InterruptedException {
        DiscoveryNode local = node("reader-1", true);
        when(clusterService.state()).thenReturn(stateWithLocalNode(local, null));
        CountDownLatch clearLatch = new CountDownLatch(1);
        NoOpClient clearTrackingClient = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                requestCount.incrementAndGet();
                @SuppressWarnings("unchecked")
                ActionListener<AcknowledgedResponse> typed = (ActionListener<AcknowledgedResponse>) listener;
                typed.onResponse(new AcknowledgedResponse(true));
                NodeWarmupRequest warmupRequest = (NodeWarmupRequest) request;
                if (warmupRequest.warming() == false) {
                    clearLatch.countDown(); // the clear call carries warming=false.
                }
            }
        };

        NodeSelfWarmupSchedulerTask task = new NodeSelfWarmupSchedulerTask(
            threadPool,
            TimeValue.timeValueDays(1),
            clusterService,
            clearTrackingClient,
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
