/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaleup;

import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.serverless.storage.scaleup.action.NodeScaleUpCandidatesResponse;
import org.opensearch.serverless.storage.scaleup.action.ScaleUpCandidateEntry;
import org.opensearch.serverless.storage.scaleup.action.ScaleUpCandidatesAction;
import org.opensearch.serverless.storage.scaleup.action.ScaleUpCandidatesResponse;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ScaleUpCandidatesSchedulerTaskTests extends OpenSearchTestCase {

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
        ClusterState notElected = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(local).add(other).localNodeId(local.getId()).clusterManagerNodeId(other.getId()))
            .build();
        ClusterServiceUtils.setState(clusterService, notElected);
        return clusterService;
    }

    private static ScaleUpCandidatesResponse emptyResponse() {
        return new ScaleUpCandidatesResponse(
            new ClusterName("test"),
            List.<NodeScaleUpCandidatesResponse>of(),
            List.of(),
            10_000L,
            5,
            Metadata.EMPTY_METADATA
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
                    ScaleUpCandidatesAction.INSTANCE,
                    action
                );
                @SuppressWarnings("unchecked")
                ActionListener<ScaleUpCandidatesResponse> typed = (ActionListener<ScaleUpCandidatesResponse>) listener;
                typed.onResponse(emptyResponse());
            }
        };

        ScaleUpCandidatesSchedulerTask task = new ScaleUpCandidatesSchedulerTask(
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

        ScaleUpCandidatesSchedulerTask task = new ScaleUpCandidatesSchedulerTask(
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

        ScaleUpCandidatesSchedulerTask task = new ScaleUpCandidatesSchedulerTask(
            threadPool,
            TimeValue.timeValueMinutes(10),
            client,
            clusterService
        );
        try {
            // Must not throw out of evaluateForTesting()/evaluate() -- the failure is dispatched to
            // onFailure, not raised synchronously.
            task.evaluateForTesting();
        } finally {
            task.close();
            clusterService.close();
        }
    }

    public void testEvaluateWithoutACoordinatorNeverActsOnCandidates() {
        // Policy-only constructor (no ReaderReplicaExpansionCoordinator) must not throw even when
        // the response reports real candidates -- there is simply nothing to act on them with.
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("idx-uuid", 0, "my-index", 99_999L, 1, true);
        NoOpClient client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                ScaleUpCandidatesResponse response = new ScaleUpCandidatesResponse(
                    new ClusterName("test"),
                    List.<NodeScaleUpCandidatesResponse>of(),
                    List.of(),
                    10_000L,
                    5,
                    Metadata.EMPTY_METADATA
                ) {
                    @Override
                    public List<ScaleUpCandidateEntry> candidates() {
                        return List.of(candidate);
                    }
                };
                @SuppressWarnings("unchecked")
                ActionListener<ScaleUpCandidatesResponse> typed = (ActionListener<ScaleUpCandidatesResponse>) listener;
                typed.onResponse(response);
            }
        };

        ScaleUpCandidatesSchedulerTask task = new ScaleUpCandidatesSchedulerTask(
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

    public void testEvaluateWithACoordinatorReachesExpandCandidates() {
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        // ReaderReplicaExpansionCoordinator is final; observe the scheduler task actually reaching
        // the coordinator by checking whether an UpdateSettingsRequest is ever dispatched for a
        // reported candidate, rather than mocking/subclassing the coordinator.
        AtomicInteger updateSettingsRequests = new AtomicInteger();
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("idx-uuid", 0, "my-index", 99_999L, 1, true);
        NoOpClient client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action == ScaleUpCandidatesAction.INSTANCE) {
                    ScaleUpCandidatesResponse response = new ScaleUpCandidatesResponse(
                        new ClusterName("test"),
                        List.<NodeScaleUpCandidatesResponse>of(),
                        List.of(),
                        10_000L,
                        5,
                        Metadata.EMPTY_METADATA
                    ) {
                        @Override
                        public List<ScaleUpCandidateEntry> candidates() {
                            return List.of(candidate);
                        }
                    };
                    @SuppressWarnings("unchecked")
                    ActionListener<ScaleUpCandidatesResponse> typed = (ActionListener<ScaleUpCandidatesResponse>) listener;
                    typed.onResponse(response);
                } else {
                    updateSettingsRequests.incrementAndGet();
                    listener.onFailure(new RuntimeException("simulated settings-update failure, irrelevant to this test"));
                }
            }
        };

        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1, 0);
        ScaleUpCandidatesSchedulerTask task = new ScaleUpCandidatesSchedulerTask(
            threadPool,
            TimeValue.timeValueMinutes(10),
            client,
            clusterService,
            coordinator
        );
        try {
            task.evaluateForTesting();
            // requiredConsecutiveTicks is 1, so the very first tick's candidate is immediately
            // sustained and the coordinator issues an UpdateSettingsRequest for it.
            assertEquals(1, updateSettingsRequests.get());
        } finally {
            task.close();
            clusterService.close();
        }
    }
}
