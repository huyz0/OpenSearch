/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * H9a. Whether scale-to-zero can suspend an index that has no cluster state entry.
 *
 * <p>Two problems meet on one line. {@code ShardSuspensionCoordinator.findByUuid} scans
 * {@code metadata.indices()} to resolve a shard's index, which H1d recorded as O(total indices) on a path
 * that runs per candidate shard per tick. Area H adds the second: a gated index is not in metadata at all,
 * so the scan finds nothing.
 *
 * <p>The failure mode is the one this project has hit eight times. Suspension does not throw, it returns
 * having done nothing, so a gated index would simply never scale to zero. Since scaling to zero is the
 * point of the serverless design, an index that cannot do it is worse than one that fails loudly.
 *
 * <p>The mechanism is not the one predicted, and the prediction is recorded because it was wrong in an
 * instructive way. The guess was that the pre-check would find nothing and skip the submission. What
 * actually happens is that the pre-check falls through on a null, a cluster state task <em>is</em>
 * submitted, and the transform inside it then finds nothing and returns the state unchanged. So the cost
 * is paid and the work is not done, which is worse than skipping: every candidate shard of every gated
 * index queues a task per tick that can only be a no-op.
 *
 * <p>This asserts the gap rather than a hypothetical fix, in the shape that failed usefully for H4c and
 * H8a: it will fail the day someone closes it, and the message says what to do then.
 */
public class GatedIndexSuspensionGapTests extends OpenSearchTestCase {

    /**
     * The gap. A gated index's shard cannot be suspended, and nothing reports that it was not.
     */
    public void testAGatedIndexIsNeverSuspended() {
        // A cluster state with no index metadata at all, which is what a gated index looks like after H5.
        ClusterState gatedState = ClusterState.builder(ClusterName.DEFAULT).build();

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(gatedState);
        ShardSuspensionCoordinator coordinator = new ShardSuspensionCoordinator(clusterService, mock(Client.class), 0L);

        coordinator.suspendWriterShard("gated-uuid", 0);

        // A task is submitted, so the publication slot is spent, and running it changes nothing.
        // Asserted on the resulting state rather than on an absence of exceptions, since doing nothing
        // quietly is exactly the failure being pinned.
        ClusterState afterSuspension = runCapturedSuspension(clusterService, gatedState);

        assertSame(
            "a gated index's suspension is a no-op: the task runs, the state is unchanged, and the shard "
                + "never scales to zero. Fixing it means resolving the index through the descriptor seam "
                + "rather than by scanning metadata, which removes the O(total indices) cost at the same time",
            gatedState,
            afterSuspension
        );
    }

    /**
     * The same call for an index that <em>is</em> in metadata submits a task, so the silence above is
     * about the gate rather than about the coordinator being inert.
     */
    public void testAnOrdinaryIndexIsStillSuspended() {
        ClusterState state = ShardSuspensionCoordinatorTests.stateForBatching(1);

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        ShardSuspensionCoordinator coordinator = new ShardSuspensionCoordinator(clusterService, mock(Client.class), 0L);

        coordinator.suspendWriterShard(ShardSuspensionCoordinatorTests.indexUuid(state), 0);

        verify(clusterService).submitStateUpdateTask(anyString(), any(), any(), any(), any());
    }

    /**
     * Runs whatever suspension task was submitted, through its own executor.
     *
     * <p>The unchecked generics needed to capture a {@code ClusterStateTaskExecutor} from a mock live in
     * one suppressed method rather than in every test that needs them, which is the same containment
     * ShardSuspensionCoordinatorTests uses.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static ClusterState runCapturedSuspension(ClusterService clusterService, ClusterState state) {
        org.mockito.ArgumentCaptor<Object> taskCaptor = org.mockito.ArgumentCaptor.forClass(Object.class);
        org.mockito.ArgumentCaptor<org.opensearch.cluster.ClusterStateTaskExecutor> executorCaptor = org.mockito.ArgumentCaptor.forClass(
            org.opensearch.cluster.ClusterStateTaskExecutor.class
        );
        verify(clusterService).submitStateUpdateTask(anyString(), taskCaptor.capture(), any(), executorCaptor.capture(), any());
        org.opensearch.cluster.ClusterStateTaskExecutor executor = executorCaptor.getValue();
        try {
            org.opensearch.cluster.ClusterStateTaskExecutor.ClusterTasksResult result = executor.execute(
                state,
                java.util.List.of(taskCaptor.getValue())
            );
            return result.resultingState;
        } catch (Exception e) {
            throw new AssertionError("the captured executor threw", e);
        }
    }
}
