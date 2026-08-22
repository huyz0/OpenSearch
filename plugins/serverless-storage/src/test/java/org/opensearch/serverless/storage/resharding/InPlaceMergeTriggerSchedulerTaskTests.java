/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.indices.split.InPlaceMergeShardAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.serverless.storage.resharding.action.NodeShardSplitCandidatesResponse;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidateEntry;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidatesAction;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidatesResponse;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class InPlaceMergeTriggerSchedulerTaskTests extends OpenSearchTestCase {

    private TestThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private static DiscoveryNode node(String id) {
        return new DiscoveryNode(id, buildNewFakeTransportAddress(), Version.CURRENT);
    }

    private ClusterService nonClusterManagerClusterService() {
        DiscoveryNode local = node("not-the-cluster-manager");
        DiscoveryNode other = node("the-real-cluster-manager");
        ClusterService clusterService = ClusterServiceUtils.createClusterService(
            threadPool,
            local,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        ClusterState notElected = ClusterState.builder(new org.opensearch.cluster.ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(local).add(other).localNodeId(local.getId()).clusterManagerNodeId(other.getId()))
            .build();
        ClusterServiceUtils.setState(clusterService, notElected);
        return clusterService;
    }

    private static ShardSplitCandidatesResponse emptyResponse() {
        return new ShardSplitCandidatesResponse(
            new org.opensearch.cluster.ClusterName("test"),
            List.<NodeShardSplitCandidatesResponse>of(),
            List.of(),
            10_000L,
            0L,
            org.opensearch.cluster.metadata.Metadata.EMPTY_METADATA
        );
    }

    public void testEvaluateOnTheElectedClusterManagerDispatchesTheCandidatesAction() {
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        AtomicInteger requestCount = new AtomicInteger();
        NoOpClient client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                requestCount.incrementAndGet();
                assertSame(
                    "the merge scheduler reuses the same cluster-wide split-candidates action for its per-shard signal",
                    ShardSplitCandidatesAction.INSTANCE,
                    action
                );
                @SuppressWarnings("unchecked")
                ActionListener<ShardSplitCandidatesResponse> typed = (ActionListener<ShardSplitCandidatesResponse>) listener;
                typed.onResponse(emptyResponse());
            }
        };

        InPlaceMergeTriggerSchedulerTask task = new InPlaceMergeTriggerSchedulerTask(
            threadPool,
            TimeValue.timeValueMinutes(10),
            client,
            clusterService
        );
        try {
            task.evaluateForTesting();
            assertEquals(1, requestCount.get());
        } finally {
            task.close();
            clusterService.close();
        }
    }

    public void testEvaluateOnANonClusterManagerNodeNeverCallsTheClient() {
        ClusterService clusterService = nonClusterManagerClusterService();
        AtomicInteger requestCount = new AtomicInteger();
        NoOpClient client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                requestCount.incrementAndGet();
            }
        };

        InPlaceMergeTriggerSchedulerTask task = new InPlaceMergeTriggerSchedulerTask(
            threadPool,
            TimeValue.timeValueMinutes(10),
            client,
            clusterService
        );
        try {
            task.evaluateForTesting();
            assertEquals("a non-cluster-manager node must never trigger the cluster-wide fan-out itself", 0, requestCount.get());
        } finally {
            task.close();
            clusterService.close();
        }
    }

    public void testAFailedEvaluationDoesNotThrow() {
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        NoOpClient client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                listener.onFailure(new RuntimeException("simulated transport failure"));
            }
        };

        InPlaceMergeTriggerSchedulerTask task = new InPlaceMergeTriggerSchedulerTask(
            threadPool,
            TimeValue.timeValueMinutes(10),
            client,
            clusterService
        );
        try {
            task.evaluateForTesting();
        } finally {
            task.close();
            clusterService.close();
        }
    }

    public void testEvaluateWithACoordinatorReachesItWithoutThrowing() {
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        // InPlaceMergeTriggerCoordinator is final; observe the scheduler reaching it by checking that
        // InPlaceMergeShardAction is never dispatched for a candidate whose index isn't in this
        // (empty) cluster state -- the coordinator finds no eligible pair and merges nothing.
        AtomicInteger mergeRequests = new AtomicInteger();
        NoOpClient client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action == ShardSplitCandidatesAction.INSTANCE) {
                    ShardSplitCandidateEntry childSignal = new ShardSplitCandidateEntry(
                        "idx-uuid",
                        1,
                        "my-index",
                        10L,
                        1024L,
                        false,
                        false
                    );
                    ShardSplitCandidatesResponse response = new ShardSplitCandidatesResponse(
                        new org.opensearch.cluster.ClusterName("test"),
                        List.<NodeShardSplitCandidatesResponse>of(),
                        List.of(),
                        10_000L,
                        0L,
                        org.opensearch.cluster.metadata.Metadata.EMPTY_METADATA
                    ) {
                        @Override
                        public List<ShardSplitCandidateEntry> candidates() {
                            return List.of(childSignal);
                        }
                    };
                    @SuppressWarnings("unchecked")
                    ActionListener<ShardSplitCandidatesResponse> typed = (ActionListener<ShardSplitCandidatesResponse>) listener;
                    typed.onResponse(response);
                } else if (action == InPlaceMergeShardAction.INSTANCE) {
                    mergeRequests.incrementAndGet();
                    listener.onFailure(new RuntimeException("irrelevant to this test"));
                }
            }
        };

        InPlaceMergeTriggerCoordinator realCoordinator = new InPlaceMergeTriggerCoordinator(
            client,
            1,
            0,
            2_000L,
            4L * 1024 * 1024 * 1024,
            0L
        );
        InPlaceMergeTriggerSchedulerTask task = new InPlaceMergeTriggerSchedulerTask(
            threadPool,
            TimeValue.timeValueMinutes(10),
            client,
            clusterService,
            realCoordinator
        );
        try {
            task.evaluateForTesting();
            assertEquals(0, mergeRequests.get());
        } finally {
            task.close();
            clusterService.close();
        }
    }
}
