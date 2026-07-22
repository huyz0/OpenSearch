/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.allocation.ReaderShardPlacementAllocationDecider;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.ClusterAdminClient;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NodeSelfWarmupSchedulerTaskTests extends OpenSearchTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private Client client;
    private ClusterAdminClient clusterAdminClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = mock(ClusterService.class);
        client = mock(Client.class);
        AdminClient adminClient = mock(AdminClient.class);
        clusterAdminClient = mock(ClusterAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
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

    private void stubUpdateSettingsSucceeds() {
        doAnswer(invocation -> {
            ActionListener<ClusterUpdateSettingsResponse> listener = invocation.getArgument(1);
            listener.onResponse(mock(ClusterUpdateSettingsResponse.class));
            return null;
        }).when(clusterAdminClient).updateSettings(any(ClusterUpdateSettingsRequest.class), any());
    }

    public void testSelfMarksAReaderNodeAsWarming() {
        DiscoveryNode local = node("reader-1", true);
        when(clusterService.state()).thenReturn(stateWithLocalNode(local, null));
        stubUpdateSettingsSucceeds();
        NodeWarmupCoordinator coordinator = new NodeWarmupCoordinator(client);

        NodeSelfWarmupSchedulerTask task = new NodeSelfWarmupSchedulerTask(
            threadPool,
            TimeValue.timeValueDays(1),
            clusterService,
            coordinator,
            TimeValue.ZERO
        );
        try {
            task.evaluateForTesting();

            org.mockito.ArgumentCaptor<ClusterUpdateSettingsRequest> captor = org.mockito.ArgumentCaptor.forClass(
                ClusterUpdateSettingsRequest.class
            );
            verify(clusterAdminClient, times(1)).updateSettings(captor.capture(), any());
            assertEquals("reader-1", captor.getValue().transientSettings().get(NodeWarmupCoordinator.WARMING_NAMES_SETTING_KEY));
            assertTrue(task.isSelfMarkedForTesting());
        } finally {
            task.close();
        }
    }

    public void testDoesNotSelfMarkANonReaderNode() {
        DiscoveryNode local = node("writer-1", false);
        when(clusterService.state()).thenReturn(stateWithLocalNode(local, null));
        NodeWarmupCoordinator coordinator = new NodeWarmupCoordinator(client);

        NodeSelfWarmupSchedulerTask task = new NodeSelfWarmupSchedulerTask(
            threadPool,
            TimeValue.timeValueDays(1),
            clusterService,
            coordinator,
            TimeValue.ZERO
        );
        try {
            task.evaluateForTesting();

            verify(clusterAdminClient, never()).updateSettings(any(ClusterUpdateSettingsRequest.class), any());
            assertTrue("a non-reader node has nothing to do, so it's considered done", task.isSelfMarkedForTesting());
        } finally {
            task.close();
        }
    }

    public void testDoesNotReMarkANodeAlreadyWarming() {
        DiscoveryNode local = node("reader-1", true);
        when(clusterService.state()).thenReturn(stateWithLocalNode(local, "reader-1"));
        NodeWarmupCoordinator coordinator = new NodeWarmupCoordinator(client);

        NodeSelfWarmupSchedulerTask task = new NodeSelfWarmupSchedulerTask(
            threadPool,
            TimeValue.timeValueDays(1),
            clusterService,
            coordinator,
            TimeValue.ZERO
        );
        try {
            task.evaluateForTesting();

            verify(clusterAdminClient, never()).updateSettings(any(ClusterUpdateSettingsRequest.class), any());
            assertTrue(task.isSelfMarkedForTesting());
        } finally {
            task.close();
        }
    }

    public void testDoesNotAttemptAgainOnceSelfMarked() {
        DiscoveryNode local = node("reader-1", true);
        when(clusterService.state()).thenReturn(stateWithLocalNode(local, null));
        stubUpdateSettingsSucceeds();
        NodeWarmupCoordinator coordinator = new NodeWarmupCoordinator(client);

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

            verify(clusterAdminClient, times(1)).updateSettings(any(ClusterUpdateSettingsRequest.class), any());
        } finally {
            task.close();
        }
    }

    public void testRetriesOnFailure() {
        DiscoveryNode local = node("reader-1", true);
        when(clusterService.state()).thenReturn(stateWithLocalNode(local, null));
        AtomicReference<Integer> callCount = new AtomicReference<>(0);
        doAnswer(invocation -> {
            ActionListener<ClusterUpdateSettingsResponse> listener = invocation.getArgument(1);
            int count = callCount.updateAndGet(c -> c + 1);
            if (count == 1) {
                listener.onFailure(new RuntimeException("simulated failure"));
            } else {
                listener.onResponse(mock(ClusterUpdateSettingsResponse.class));
            }
            return null;
        }).when(clusterAdminClient).updateSettings(any(ClusterUpdateSettingsRequest.class), any());
        NodeWarmupCoordinator coordinator = new NodeWarmupCoordinator(client);

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

            verify(clusterAdminClient, times(2)).updateSettings(any(ClusterUpdateSettingsRequest.class), any());
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
            ActionListener<ClusterUpdateSettingsResponse> listener = invocation.getArgument(1);
            ClusterUpdateSettingsRequest request = invocation.getArgument(0);
            listener.onResponse(mock(ClusterUpdateSettingsResponse.class));
            if (request.transientSettings().get(NodeWarmupCoordinator.WARMING_NAMES_SETTING_KEY) == null) {
                clearLatch.countDown(); // the clear call nulls the setting back out.
            }
            return null;
        }).when(clusterAdminClient).updateSettings(any(ClusterUpdateSettingsRequest.class), any());
        NodeWarmupCoordinator coordinator = new NodeWarmupCoordinator(client);

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
