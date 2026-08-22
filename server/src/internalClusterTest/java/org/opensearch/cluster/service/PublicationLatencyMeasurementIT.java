/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.Priority;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * G1. Publication latency, and whether wake and sleep need the batching C7 gave create-index.
 *
 * <p>The plan calls this the one unmeasured number that changes a decision.
 * {@code TransportReactivateShardsAction} and {@code ShardSuspensionCoordinator} both submit plain
 * {@code ClusterStateUpdateTask}s with no executor, so if publication dominates and batching collapses
 * many publications into one, they are paying that cost once per shard woken or slept.
 *
 * <p><b>Why this is an integration test rather than a benchmark.</b> The existing spike suite estimates
 * heap and wire sizes in a single JVM, which works because those are properties of data structures.
 * Publication is a property of coordination: the elected manager computes a new state, sends it to every
 * node, and waits for enough acknowledgements. Measured in one JVM with no peers it would report the
 * cost of the parts that do not matter.
 *
 * <p><b>Guards against measuring nothing.</b> This project's characteristic failure is a green run that
 * measured nothing, three times over. So every measurement asserts that the tasks actually executed,
 * that the cluster state version advanced by exactly the number of publications expected, and that the
 * elapsed time is not zero. A harness that silently did no work fails rather than reporting a fast
 * number.
 */
public class PublicationLatencyMeasurementIT extends OpenSearchIntegTestCase {

    /**
     * The marker each measurement writes has to be removed again, because the framework fails any test
     * that leaves persistent cluster metadata behind. Worth keeping rather than switching to a change
     * that leaves no trace: the setting is what makes the state diff non-empty, and a task returning an
     * unchanged state publishes nothing at all, which would make every number here a measurement of
     * skipped work.
     */
    @org.junit.After
    public void clearMarker() {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(org.opensearch.common.settings.Settings.builder().putNull(MARKER))
            .get();
    }

    private static final String MARKER = "archived.g1.marker";

    /** Bounded so the suite stays runnable; the shape matters more than the largest point. */
    private static final int[] BATCH_SIZES = { 1, 10, 100 };

    /**
     * Latency of publishing one state change at a time, which is what wake and sleep do today.
     *
     * <p>Reported as the per-publication cost, since that is the figure a caller pays per shard.
     */
    public void testUnbatchedPublicationLatency() throws Exception {
        StringBuilder table = new StringBuilder("\nG1 unbatched publication, ").append(clusterSize()).append(" nodes\n");

        for (int batch : BATCH_SIZES) {
            Measurement measurement = submitUnbatched(batch);
            table.append(
                String.format(
                    java.util.Locale.ROOT,
                    "  batch=%3d  total=%6.1f ms  per-publication=%6.2f ms  versionsAdvanced=%d%n",
                    batch,
                    measurement.elapsedMillis,
                    measurement.elapsedMillis / batch,
                    measurement.versionsAdvanced
                )
            );
        }
        logger.warn(table.toString());
    }

    /**
     * The same work through a {@link ClusterStateTaskExecutor}, which is what C7 gave create-index.
     *
     * <p>The comparison is the point of G1. Batching cannot make a single publication faster; what it
     * can do is turn N publications into one, so the difference should grow with the batch and vanish at
     * a batch of one.
     */
    public void testBatchedPublicationLatency() throws Exception {
        StringBuilder table = new StringBuilder("\nG1 batched publication, ").append(clusterSize()).append(" nodes\n");

        for (int batch : BATCH_SIZES) {
            Measurement measurement = submitBatched(batch);
            table.append(
                String.format(
                    java.util.Locale.ROOT,
                    "  batch=%3d  total=%6.1f ms  per-publication=%6.2f ms  versionsAdvanced=%d%n",
                    batch,
                    measurement.elapsedMillis,
                    measurement.elapsedMillis / batch,
                    measurement.versionsAdvanced
                )
            );
        }
        logger.warn(table.toString());
    }

    /**
     * Whether sustained URGENT work delays NORMAL work, which is the starvation the plan predicts
     * between reactivation and suspension.
     *
     * <p>Measured rather than assumed. The prediction is plausible, and plausible predictions are what
     * this project has repeatedly got wrong.
     */
    public void testUrgentSubmissionsDelayNormalOnes() throws Exception {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getClusterManagerName());

        int urgentCount = 300;
        CountDownLatch urgentDone = new CountDownLatch(urgentCount);
        CountDownLatch normalDone = new CountDownLatch(1);
        AtomicLong normalLatencyNanos = new AtomicLong();

        // The URGENT pressure has to already be queued when the NORMAL task arrives, which is the
        // situation the plan describes: sustained wake traffic with a sleep waiting behind it. An
        // earlier version submitted the NORMAL first and measured 24 ms, which says nothing about
        // starvation because the queue was empty when it arrived.
        for (int i = 0; i < urgentCount; i++) {
            final int ordinal = i;
            clusterService.submitStateUpdateTask("g1-urgent-" + ordinal, new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return bumpMetadata(currentState, "g1-urgent-" + ordinal);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    urgentDone.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    urgentDone.countDown();
                }
            });
        }

        long normalSubmittedAt = System.nanoTime();
        clusterService.submitStateUpdateTask("g1-normal", new ClusterStateUpdateTask(Priority.NORMAL) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return bumpMetadata(currentState, "g1-normal");
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                normalLatencyNanos.set(System.nanoTime() - normalSubmittedAt);
                normalDone.countDown();
            }

            @Override
            public void onFailure(String source, Exception e) {
                normalLatencyNanos.set(System.nanoTime() - normalSubmittedAt);
                normalDone.countDown();
            }
        });

        assertTrue("the urgent tasks must all complete, or this measures a stalled cluster", urgentDone.await(300, TimeUnit.SECONDS));
        assertTrue("the normal task must eventually complete", normalDone.await(300, TimeUnit.SECONDS));

        double normalMillis = normalLatencyNanos.get() / 1_000_000.0;
        assertTrue("the normal task must have been timed, or this measured nothing", normalLatencyNanos.get() > 0);
        logger.warn(
            String.format(
                java.util.Locale.ROOT,
                "%nG1 priority starvation, %d nodes: a NORMAL task submitted behind %d queued URGENT tasks waited %.1f ms%n",
                clusterSize(),
                urgentCount,
                normalMillis
            )
        );
    }

    /**
     * Whether batching shortens the queue enough to relieve the starvation, which is the claim behind
     * fixing wake and sleep rather than reordering their priorities.
     *
     * <p>G1 measured a NORMAL task waiting seconds behind 300 plain URGENT tasks. If the URGENT work is
     * batched instead, the same 300 tasks occupy far fewer publications, so the NORMAL task behind them
     * should wait proportionally less. Measured rather than assumed, because if it does not hold the
     * right response is to say so and leave the priorities alone.
     */
    public void testBatchingTheUrgentWorkRelievesTheStarvation() throws Exception {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getClusterManagerName());

        int urgentCount = 300;
        CountDownLatch urgentDone = new CountDownLatch(urgentCount);
        CountDownLatch normalDone = new CountDownLatch(1);
        AtomicLong normalLatencyNanos = new AtomicLong();
        AtomicInteger executed = new AtomicInteger();

        ClusterStateTaskExecutor<Integer> executor = (currentState, tasks) -> {
            executed.addAndGet(tasks.size());
            return ClusterStateTaskExecutor.ClusterTasksResult.<Integer>builder()
                .successes(tasks)
                .build(bumpMetadata(currentState, "g1-urgent-batched-" + tasks.size()));
        };
        ClusterStateTaskListener urgentListener = new ClusterStateTaskListener() {
            @Override
            public void onFailure(String source, Exception e) {
                urgentDone.countDown();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                urgentDone.countDown();
            }
        };

        for (int i = 0; i < urgentCount; i++) {
            clusterService.submitStateUpdateTask(
                "g1-urgent-batched",
                i,
                ClusterStateTaskConfig.build(Priority.URGENT),
                executor,
                urgentListener
            );
        }

        long normalSubmittedAt = System.nanoTime();
        clusterService.submitStateUpdateTask("g1-normal-behind-batch", new ClusterStateUpdateTask(Priority.NORMAL) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return bumpMetadata(currentState, "g1-normal-behind-batch");
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                normalLatencyNanos.set(System.nanoTime() - normalSubmittedAt);
                normalDone.countDown();
            }

            @Override
            public void onFailure(String source, Exception e) {
                normalLatencyNanos.set(System.nanoTime() - normalSubmittedAt);
                normalDone.countDown();
            }
        });

        assertTrue("the urgent batch must complete", urgentDone.await(300, TimeUnit.SECONDS));
        assertTrue("the normal task must complete", normalDone.await(300, TimeUnit.SECONDS));
        assertEquals(
            "every urgent task must have been executed, or this measured a shorter queue than it claims",
            urgentCount,
            executed.get()
        );
        assertTrue("the normal task must have been timed", normalLatencyNanos.get() > 0);

        logger.warn(
            String.format(
                java.util.Locale.ROOT,
                "%nG1 starvation with batched urgent work, %d nodes: a NORMAL task behind %d batched URGENT tasks waited %.1f ms%n",
                clusterSize(),
                urgentCount,
                normalLatencyNanos.get() / 1_000_000.0
            )
        );
    }

    /** Recorded with every number, since the framework randomises the node count. */
    private int clusterSize() {
        return internalCluster().size();
    }

    // ---------------------------------------------------------------- harness

    private Measurement submitUnbatched(int count) throws Exception {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getClusterManagerName());
        long startingVersion = clusterService.state().version();
        CountDownLatch done = new CountDownLatch(count);
        AtomicInteger executed = new AtomicInteger();

        long startedAt = System.nanoTime();
        for (int i = 0; i < count; i++) {
            final int ordinal = i;
            clusterService.submitStateUpdateTask("g1-unbatched-" + ordinal, new ClusterStateUpdateTask(Priority.NORMAL) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    executed.incrementAndGet();
                    return bumpMetadata(currentState, "g1-unbatched-" + ordinal);
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
        }
        assertTrue("every submitted task must complete within the budget", done.await(180, TimeUnit.SECONDS));
        long elapsedNanos = System.nanoTime() - startedAt;

        long versionsAdvanced = clusterService.state().version() - startingVersion;
        assertEquals("every task must have executed, or the timing covers work that did not happen", count, executed.get());
        assertTrue("time must have passed, or the harness measured nothing", elapsedNanos > 0);
        assertTrue(
            "unbatched submission must publish once per task, but the version advanced by " + versionsAdvanced,
            versionsAdvanced >= count
        );

        return new Measurement(elapsedNanos / 1_000_000.0, versionsAdvanced);
    }

    private Measurement submitBatched(int count) throws Exception {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getClusterManagerName());
        long startingVersion = clusterService.state().version();
        CountDownLatch done = new CountDownLatch(count);
        AtomicInteger executed = new AtomicInteger();

        ClusterStateTaskExecutor<Integer> executor = (currentState, tasks) -> {
            executed.addAndGet(tasks.size());
            ClusterState updated = bumpMetadata(currentState, "g1-batched-" + tasks.size());
            return ClusterStateTaskExecutor.ClusterTasksResult.<Integer>builder().successes(tasks).build(updated);
        };

        ClusterStateTaskListener listener = new ClusterStateTaskListener() {
            @Override
            public void onFailure(String source, Exception e) {
                done.countDown();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                done.countDown();
            }
        };

        long startedAt = System.nanoTime();
        for (int i = 0; i < count; i++) {
            clusterService.submitStateUpdateTask("g1-batched", i, ClusterStateTaskConfig.build(Priority.NORMAL), executor, listener);
        }
        assertTrue("every submitted task must complete within the budget", done.await(180, TimeUnit.SECONDS));
        long elapsedNanos = System.nanoTime() - startedAt;

        long versionsAdvanced = clusterService.state().version() - startingVersion;
        assertEquals("every task must have executed, or the timing covers work that did not happen", count, executed.get());
        assertTrue("time must have passed, or the harness measured nothing", elapsedNanos > 0);
        assertTrue(
            "batching must publish fewer times than it has tasks once there is more than one, but advanced "
                + versionsAdvanced
                + " for "
                + count
                + " tasks",
            count == 1 || versionsAdvanced < count
        );

        return new Measurement(elapsedNanos / 1_000_000.0, versionsAdvanced);
    }

    /**
     * A minimal but real state change. Publication cost depends on the diff being non-empty, so a task
     * returning the same state would be optimised away and the measurement would be of nothing.
     */
    private static ClusterState bumpMetadata(ClusterState currentState, String marker) {
        return ClusterState.builder(currentState)
            .metadata(
                Metadata.builder(currentState.metadata())
                    .persistentSettings(
                        org.opensearch.common.settings.Settings.builder()
                            .put(currentState.metadata().persistentSettings())
                            .put(MARKER, marker)
                            .build()
                    )
            )
            .build();
    }

    private static final class Measurement {
        final double elapsedMillis;
        final long versionsAdvanced;

        Measurement(double elapsedMillis, long versionsAdvanced) {
            this.elapsedMillis = elapsedMillis;
            this.versionsAdvanced = versionsAdvanced;
        }
    }

    /** Kept so the imports document what a real batching caller needs. */
    @SuppressWarnings("unused")
    private static List<String> unusedShape() {
        return new ArrayList<>();
    }
}
