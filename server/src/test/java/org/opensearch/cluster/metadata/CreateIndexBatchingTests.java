/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.indices.cluster.ClusterStateChanges;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.opensearch.action.support.ActiveShardCount.NONE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

/**
 * Index creation is submitted through a shared batching {@link org.opensearch.cluster.ClusterStateTaskExecutor},
 * so concurrent requests collapse into one cluster-state update instead of one cycle each.
 *
 * <p>Every other suite creates indices one at a time, which means a fold that silently dropped
 * every task after the first would still pass throughout. These tests exercise the multi-request
 * path specifically: many creations must all land, and a rejected request must fail only itself.
 */
public class CreateIndexBatchingTests extends OpenSearchTestCase {

    /**
     * The load-bearing test: drive the executor with a genuine multi-task batch and assert every
     * index lands. Going through {@code ClusterStateChanges} submits one task per call, so the batch
     * is always size one there and a fold that dropped everything after the first would still pass.
     */
    public void testExecutorAppliesEveryTaskInABatch() {
        final ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            final ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);
            // Reach the real service instance and its batching executor.
            final MetadataCreateIndexService service = cluster.getMetadataCreateIndexService();

            final List<MetadataCreateIndexService.CreateIndexTask> batch = new ArrayList<>();
            final int count = 5;
            for (int i = 0; i < count; i++) {
                batch.add(service.new CreateIndexTask(updateRequest("batch-index-" + i), noopListener()));
            }

            final ClusterStateTaskExecutor.ClusterTasksResult<MetadataCreateIndexService.CreateIndexTask> result;
            try {
                result = service.createIndexExecutor.execute(initialState(), batch);
            } catch (Exception e) {
                throw new AssertionError(e);
            }

            assertNotNull(result.resultingState);
            for (int i = 0; i < count; i++) {
                assertTrue(
                    "index [batch-index-" + i + "] must be created by the batch, not dropped",
                    result.resultingState.metadata().hasIndex("batch-index-" + i)
                );
            }
            assertEquals("every task must be recorded", count, result.executionResults.size());
            for (ClusterStateTaskExecutor.TaskResult taskResult : result.executionResults.values()) {
                assertTrue("no task should have failed", taskResult.isSuccess());
            }
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    /** A single bad request in a batch must fail alone; its neighbours still apply. */
    public void testOneBadRequestDoesNotPoisonTheBatch() {
        final ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            final ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);
            final MetadataCreateIndexService service = cluster.getMetadataCreateIndexService();

            // "dup" already exists, so the second request for it must be rejected on its own.
            ClusterState state = cluster.createIndex(initialState(), createRequest("dup"));

            final MetadataCreateIndexService.CreateIndexTask good = service.new CreateIndexTask(updateRequest("good"), noopListener());
            final MetadataCreateIndexService.CreateIndexTask bad = service.new CreateIndexTask(updateRequest("dup"), noopListener());

            final ClusterStateTaskExecutor.ClusterTasksResult<MetadataCreateIndexService.CreateIndexTask> result;
            try {
                result = service.createIndexExecutor.execute(state, List.of(bad, good));
            } catch (Exception e) {
                throw new AssertionError(e);
            }

            assertFalse("the duplicate must fail", result.executionResults.get(bad).isSuccess());
            assertTrue("its neighbour must still succeed", result.executionResults.get(good).isSuccess());
            assertTrue("the good index must exist", result.resultingState.metadata().hasIndex("good"));
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    /** Several creations in sequence must all land -- none silently dropped by the batch fold. */
    public void testEveryCreatedIndexSurvives() {
        final ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            final ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);
            ClusterState state = initialState();

            final List<String> names = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                names.add("batched-index-" + i);
            }
            for (String name : names) {
                state = cluster.createIndex(state, createRequest(name));
            }

            for (String name : names) {
                assertTrue("index [" + name + "] should exist after creation", state.metadata().hasIndex(name));
                assertNotNull("index [" + name + "] should have a routing table entry", state.routingTable().index(name));
            }
            assertEquals("no creation may be dropped", names.size(), state.metadata().indices().size());
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    /** A rejected request must fail on its own without blocking later creations. */
    public void testRejectedCreationDoesNotBlockLaterOnes() {
        final ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            final ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);
            ClusterState state = initialState();

            state = cluster.createIndex(state, createRequest("existing"));
            assertTrue(state.metadata().hasIndex("existing"));

            final ClusterState before = state;
            expectThrows(Exception.class, () -> cluster.createIndex(before, createRequest("existing")));

            state = cluster.createIndex(state, createRequest("after-failure"));
            assertTrue("a rejected request must not block later creations", state.metadata().hasIndex("after-failure"));
            assertTrue("the original index must be untouched", state.metadata().hasIndex("existing"));
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    private static CreateIndexClusterStateUpdateRequest updateRequest(String name) {
        return new CreateIndexClusterStateUpdateRequest("test", name, name).settings(
            Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        ).waitForActiveShards(NONE);
    }

    private static ActionListener<ClusterStateUpdateResponse> noopListener() {
        return ActionListener.wrap(r -> {}, e -> {});
    }

    private static CreateIndexRequest createRequest(String name) {
        return new CreateIndexRequest(name, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).build()).waitForActiveShards(NONE);
    }

    private static ClusterState initialState() {
        final DiscoveryNode clusterManager = node("node-cm", DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, 9300);
        final DiscoveryNode data = node("node-data", DiscoveryNodeRole.DATA_ROLE, 9301);
        return org.opensearch.action.support.replication.ClusterStateCreationUtils.state(
            clusterManager,
            clusterManager,
            new DiscoveryNode[] { clusterManager, data }
        );
    }

    private static DiscoveryNode node(String id, DiscoveryNodeRole role, int port) {
        return new DiscoveryNode(
            id,
            new TransportAddress(InetAddress.getLoopbackAddress(), port),
            Collections.emptyMap(),
            Set.of(role),
            org.opensearch.Version.CURRENT
        );
    }
}
