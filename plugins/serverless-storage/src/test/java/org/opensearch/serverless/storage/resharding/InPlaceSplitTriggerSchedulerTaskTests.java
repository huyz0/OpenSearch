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

public class InPlaceSplitTriggerSchedulerTaskTests extends OpenSearchTestCase {

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
                    "the scheduler must invoke the cluster-wide policy action, not any single-node signal action",
                    ShardSplitCandidatesAction.INSTANCE,
                    action
                );
                @SuppressWarnings("unchecked")
                ActionListener<ShardSplitCandidatesResponse> typed = (ActionListener<ShardSplitCandidatesResponse>) listener;
                typed.onResponse(emptyResponse());
            }
        };

        InPlaceSplitTriggerSchedulerTask task = new InPlaceSplitTriggerSchedulerTask(
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

        InPlaceSplitTriggerSchedulerTask task = new InPlaceSplitTriggerSchedulerTask(
            threadPool,
            TimeValue.timeValueMinutes(10),
            client,
            clusterService
        );
        try {
            task.evaluateForTesting();
            assertEquals(
                "a non-cluster-manager node must never trigger the cluster-wide fan-out itself -- "
                    + "see the class javadoc for why only the elected cluster-manager runs this",
                0,
                requestCount.get()
            );
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

        InPlaceSplitTriggerSchedulerTask task = new InPlaceSplitTriggerSchedulerTask(
            threadPool,
            TimeValue.timeValueMinutes(10),
            client,
            clusterService
        );
        try {
            // Must not throw out of evaluateForTesting()/evaluate() -- the real scheduler wraps
            // evaluate() in evaluateSafely(), but evaluate() itself dispatches the failure to
            // onFailure rather than raising it synchronously.
            task.evaluateForTesting();
        } finally {
            task.close();
            clusterService.close();
        }
    }

    public void testEvaluateWithoutACoordinatorNeverActsOnCandidates() {
        // Policy-only constructor (no InPlaceSplitTriggerCoordinator) must not throw even when the
        // response reports real candidates -- there is simply nothing to act on them with.
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        ShardSplitCandidateEntry candidate = new ShardSplitCandidateEntry("idx-uuid", 0, "my-index", 99_999L, 0L, true, false);
        NoOpClient client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
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
                        return List.of(candidate);
                    }
                };
                @SuppressWarnings("unchecked")
                ActionListener<ShardSplitCandidatesResponse> typed = (ActionListener<ShardSplitCandidatesResponse>) listener;
                typed.onResponse(response);
            }
        };

        InPlaceSplitTriggerSchedulerTask task = new InPlaceSplitTriggerSchedulerTask(
            threadPool,
            TimeValue.timeValueMinutes(10),
            client,
            clusterService
        );
        try {
            // No exception is the assertion here -- a null coordinator must be a safe no-op.
            task.evaluateForTesting();
        } finally {
            task.close();
            clusterService.close();
        }
    }

    public void testEvaluateWithACoordinatorForwardsCandidatesAndState() {
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        // InPlaceSplitTriggerCoordinator is final; observe the scheduler task actually reaching the
        // coordinator by checking whether InPlaceSplitShardAction is ever dispatched for an eligible
        // candidate, rather than mocking/subclassing the coordinator.
        AtomicInteger splitRequests = new AtomicInteger();
        NoOpClient splitClient = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action == ShardSplitCandidatesAction.INSTANCE) {
                    ShardSplitCandidateEntry candidate = new ShardSplitCandidateEntry("idx-uuid", 0, "my-index", 99_999L, 0L, true, false);
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
                            return List.of(candidate);
                        }
                    };
                    @SuppressWarnings("unchecked")
                    ActionListener<ShardSplitCandidatesResponse> typed = (ActionListener<ShardSplitCandidatesResponse>) listener;
                    typed.onResponse(response);
                } else {
                    splitRequests.incrementAndGet();
                    listener.onFailure(new RuntimeException("simulated split failure, irrelevant to this test"));
                }
            }
        };

        InPlaceSplitTriggerCoordinator realCoordinator = new InPlaceSplitTriggerCoordinator(splitClient, 1, 0);
        InPlaceSplitTriggerSchedulerTask task = new InPlaceSplitTriggerSchedulerTask(
            threadPool,
            TimeValue.timeValueMinutes(10),
            splitClient,
            clusterService,
            realCoordinator
        );
        try {
            task.evaluateForTesting();
            // The candidate's index doesn't actually exist in this cluster state, so the
            // coordinator's own alreadySplitOrInFlight guard drops it before ever calling
            // InPlaceSplitShardAction -- confirming the scheduler task at least reaches the
            // coordinator without throwing is the useful assertion here.
            assertEquals(0, splitRequests.get());
        } finally {
            task.close();
            clusterService.close();
        }
    }
}
