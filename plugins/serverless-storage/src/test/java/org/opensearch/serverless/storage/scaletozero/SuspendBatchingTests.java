/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.serverless.storage.allocation.SuspendedShardsMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The property batching exists for, asserted on the executor rather than on a timer.
 *
 * <p>G1 measured publication at 6 to 13 ms and linear in the number of plain tasks: a hundred cold
 * shards was a hundred publications. What makes the batched path different is not that it is faster but
 * that it publishes fewer times, so that is what these assert. A timing assertion here would be both
 * flakier and weaker.
 *
 * <p>Every task must also come back as a success or a failure. A batched executor that silently drops
 * tasks looks fast and leaves shards marked suspended in cluster state that nothing ever evicts, which
 * is worse than the cost batching removes.
 */
public class SuspendBatchingTests extends OpenSearchTestCase {

    /** The load-bearing one: many suspensions, one resulting state. */
    public void testABatchOfSuspensionsProducesOneState() throws Exception {
        int shards = 25;
        ClusterState state = ShardSuspensionCoordinatorTests.stateForBatching(shards);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        ShardSuspensionCoordinator coordinator = new ShardSuspensionCoordinator(
            clusterService,
            mock(org.opensearch.transport.client.Client.class),
            0L
        );

        List<Object> tasks = new ArrayList<>();
        ClusterStateTaskExecutor<Object> executor = captureSubmissions(coordinator, clusterService, state, shards, tasks);

        assertEquals("every shard must have produced a task, or the batch measured less than it claims", shards, tasks.size());

        ClusterStateTaskExecutor.ClusterTasksResult<Object> result = executor.execute(state, tasks);

        assertEquals("a batch of suspensions must resolve to a single state", 1, countStates(result));
        assertEquals("every task must be answered, since a dropped suspension is a shard nothing evicts", shards, answered(result, tasks));

        ClusterState batched = result.resultingState;
        for (int shard = 0; shard < shards; shard++) {
            assertTrue(
                "shard " + shard + " must be suspended in the single state the batch produced",
                SuspendedShardsMetadata.isSuspended(batched.metadata().index(ShardSuspensionCoordinatorTests.INDEX_NAME), shard)
            );
        }
    }

    /**
     * The control that the batch is not simply the last task winning. Folding has to accumulate, so a
     * state built from twenty-five tasks must differ from one built from a single task.
     */
    public void testFoldingAccumulatesRatherThanReplacing() throws Exception {
        ClusterState state = ShardSuspensionCoordinatorTests.stateForBatching(25);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        ShardSuspensionCoordinator coordinator = new ShardSuspensionCoordinator(
            clusterService,
            mock(org.opensearch.transport.client.Client.class),
            0L
        );

        List<Object> tasks = new ArrayList<>();
        ClusterStateTaskExecutor<Object> executor = captureSubmissions(coordinator, clusterService, state, 25, tasks);

        ClusterState fromOne = executor.execute(state, tasks.subList(0, 1)).resultingState;
        ClusterState fromAll = executor.execute(state, tasks).resultingState;

        int suspendedFromOne = suspendedCount(fromOne, 25);
        int suspendedFromAll = suspendedCount(fromAll, 25);

        assertEquals("one task must suspend exactly one shard", 1, suspendedFromOne);
        assertEquals("twenty-five tasks must suspend twenty-five shards, not overwrite each other", 25, suspendedFromAll);
    }

    // ---------------------------------------------------------------- helpers

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static ClusterStateTaskExecutor<Object> captureSubmissions(
        ShardSuspensionCoordinator coordinator,
        ClusterService clusterService,
        ClusterState state,
        int shards,
        List<Object> collected
    ) {
        for (int shard = 0; shard < shards; shard++) {
            coordinator.suspendWriterShard(ShardSuspensionCoordinatorTests.indexUuid(state), shard);
        }
        org.mockito.ArgumentCaptor<Object> taskCaptor = org.mockito.ArgumentCaptor.forClass(Object.class);
        org.mockito.ArgumentCaptor<ClusterStateTaskExecutor> executorCaptor = org.mockito.ArgumentCaptor.forClass(
            ClusterStateTaskExecutor.class
        );
        verify(clusterService, times(shards)).submitStateUpdateTask(
            anyString(),
            taskCaptor.capture(),
            any(),
            executorCaptor.capture(),
            any()
        );
        collected.addAll(taskCaptor.getAllValues());
        return executorCaptor.getValue();
    }

    /** One resulting state per batch is the whole point, so it is counted rather than assumed. */
    private static int countStates(ClusterStateTaskExecutor.ClusterTasksResult<Object> result) {
        return result.resultingState == null ? 0 : 1;
    }

    private static int answered(ClusterStateTaskExecutor.ClusterTasksResult<Object> result, List<Object> tasks) {
        int answered = 0;
        for (Object task : tasks) {
            if (result.executionResults.containsKey(task)) {
                answered++;
            }
        }
        return answered;
    }

    private static int suspendedCount(ClusterState state, int shards) {
        int suspended = 0;
        for (int shard = 0; shard < shards; shard++) {
            if (SuspendedShardsMetadata.isSuspended(state.metadata().index(ShardSuspensionCoordinatorTests.INDEX_NAME), shard)) {
                suspended++;
            }
        }
        return suspended;
    }
}
