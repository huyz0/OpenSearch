/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.Priority;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * T14 (docs/rounds/STATE.md's "Open, not blocked" list): cluster-state publication latency against
 * cluster size, estimated at 50-200 ms and never measured -- the number that decides whether shard
 * wake/sleep need the same batching G1 already proved matters for publication count.
 *
 * <p>{@code PublicationLatencyMeasurementIT} (G1, already landed) answers a different question:
 * batched vs. unbatched, at whatever single cluster size the test framework's own randomised default
 * scope happens to pick for that run. It never varies the node count on purpose, so it cannot answer
 * "against cluster size" by itself. This class holds the node count fixed as the one controlled
 * variable, growing one real cluster between measurements rather than comparing across separate
 * randomly-sized runs -- the same shape of control G1's own "why an integration test, not a spike"
 * reasoning already established as necessary for a coordination cost, not a data-structure one.
 *
 * <p>Guards against measuring nothing, same discipline as G1: every measurement asserts the task
 * actually executed, the version advanced by exactly one publication, and elapsed time is non-zero.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PublicationLatencyVsClusterSizeIT extends OpenSearchIntegTestCase {

    /** Bounded so the suite stays runnable on a shared box; the trend matters more than a large N. */
    private static final int[] NODE_COUNTS = { 1, 3, 6 };

    public void testUnbatchedPublicationLatencyAsTheClusterGrows() throws Exception {
        internalCluster().startClusterManagerOnlyNode();

        StringBuilder table = new StringBuilder("\nT14 unbatched single-publication latency vs. cluster size (data nodes)\n");
        int currentDataNodes = 0;
        for (int targetDataNodes : NODE_COUNTS) {
            while (currentDataNodes < targetDataNodes) {
                internalCluster().startDataOnlyNode();
                currentDataNodes++;
            }
            ensureStableCluster(currentDataNodes + 1); // +1 for the cluster-manager-only node

            double millis = measureOnePublication();
            table.append(String.format(java.util.Locale.ROOT, "  dataNodes=%2d  latency=%7.2f ms%n", targetDataNodes, millis));
        }
        logger.warn(table.toString());
    }

    private double measureOnePublication() throws Exception {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getClusterManagerName());
        long startingVersion = clusterService.state().version();
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger executed = new AtomicInteger();
        String marker = "t14-marker-" + System.identityHashCode(done);

        long startedAt = System.nanoTime();
        clusterService.submitStateUpdateTask("t14-publication", new ClusterStateUpdateTask(Priority.NORMAL) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                executed.incrementAndGet();
                return ClusterState.builder(currentState)
                    .metadata(
                        Metadata.builder(currentState.metadata())
                            .persistentSettings(
                                org.opensearch.common.settings.Settings.builder()
                                    .put(currentState.metadata().persistentSettings())
                                    .put("archived.t14.marker", marker)
                                    .build()
                            )
                    )
                    .build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                done.countDown();
            }

            @Override
            public void onFailure(String source, Exception e) {
                done.countDown();
            }
        });
        assertTrue("the task must complete within the budget", done.await(60, TimeUnit.SECONDS));
        long elapsedNanos = System.nanoTime() - startedAt;

        assertEquals("the task must have executed, or the timing covers work that did not happen", 1, executed.get());
        assertTrue("time must have passed, or the harness measured nothing", elapsedNanos > 0);
        assertEquals("a single unbatched task must publish exactly once", startingVersion + 1, clusterService.state().version());

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(org.opensearch.common.settings.Settings.builder().putNull("archived.t14.marker"))
            .get();

        return elapsedNanos / 1_000_000.0;
    }
}
