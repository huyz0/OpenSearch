/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero;

import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidatesAction;
import org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidatesResponse;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ScaleToZeroCandidatesSchedulerTaskTests extends OpenSearchTestCase {

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

    public void testEvaluateOnTheElectedClusterManagerPopulatesLatestCandidates() {
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
                    ScaleToZeroCandidatesAction.INSTANCE,
                    action
                );
                ScaleToZeroCandidatesResponse response = new ScaleToZeroCandidatesResponse(
                    new ClusterName("test"),
                    List.of(),
                    List.of(),
                    10_000L,
                    0L
                );
                @SuppressWarnings("unchecked")
                ActionListener<ScaleToZeroCandidatesResponse> typed = (ActionListener<ScaleToZeroCandidatesResponse>) listener;
                typed.onResponse(response);
            }
        };

        ScaleToZeroCandidatesSchedulerTask task = new ScaleToZeroCandidatesSchedulerTask(
            threadPool,
            TimeValue.timeValueMinutes(10),
            client,
            clusterService
        );
        try {
            assertEquals(
                "before any evaluation runs, latestCandidates() must be empty rather than null",
                List.of(),
                task.latestCandidates()
            );
            task.evaluateForTesting();
            assertEquals(1, requestCount.get());
            assertEquals(List.of(), task.latestCandidates());
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

        ScaleToZeroCandidatesSchedulerTask task = new ScaleToZeroCandidatesSchedulerTask(
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

    public void testAFailedEvaluationLeavesTheStaleValueInPlaceRatherThanClearingIt() {
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

        ScaleToZeroCandidatesSchedulerTask task = new ScaleToZeroCandidatesSchedulerTask(
            threadPool,
            TimeValue.timeValueMinutes(10),
            client,
            clusterService
        );
        try {
            task.evaluateForTesting();
            assertEquals(
                "a failed evaluation must not throw out of evaluateSafely, and latestCandidates() "
                    + "must stay at its prior (here, still-initial) value rather than being cleared",
                List.of(),
                task.latestCandidates()
            );
        } finally {
            task.close();
            clusterService.close();
        }
    }
}
