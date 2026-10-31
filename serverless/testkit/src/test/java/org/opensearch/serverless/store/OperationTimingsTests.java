/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class OperationTimingsTests extends OpenSearchTestCase {

    public void testBoundarySamplesAndPercentileUpperBounds() {
        final var timings = new OperationTimings();
        final var stage = OperationTimings.Stage.WAL_FLUSH;
        assertEquals(0L, timings.snapshots().get(stage).samples());
        for (long value : new long[] { -1, 0, 1, 2, 3, 4, 8, Long.MAX_VALUE }) {
            timings.record(stage, value, value >= 0);
        }
        final var snapshot = timings.snapshots().get(stage);
        assertEquals(8L, snapshot.samples());
        assertEquals(1L, snapshot.failures());
        assertEquals(Long.MAX_VALUE, snapshot.maxNanos());
        assertEquals(Long.MAX_VALUE, snapshot.totalNanos());
        assertEquals(3L, snapshot.p50UpperBoundNanos());
        assertEquals(Long.MAX_VALUE, snapshot.p95UpperBoundNanos());
        assertEquals(Long.MAX_VALUE, snapshot.p99UpperBoundNanos());
        assertEquals(0L, timings.snapshots().get(OperationTimings.Stage.WAL_APPEND).samples());
        expectThrows(UnsupportedOperationException.class, () -> timings.snapshots().clear());
    }

    public void testConcurrentRecordingKeepsAllSamples() throws Exception {
        final var timings = new OperationTimings();
        final var start = new CountDownLatch(1);
        final var executor = Executors.newFixedThreadPool(4);
        try {
            final List<Future<?>> workers = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                workers.add(executor.submit(() -> {
                    start.await();
                    for (int j = 0; j < 1_000; j++) {
                        timings.record(OperationTimings.Stage.WAL_APPEND, 100, j % 2 == 0);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> worker : workers) {
                worker.get(30, TimeUnit.SECONDS);
            }
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
        final var snapshot = timings.snapshots().get(OperationTimings.Stage.WAL_APPEND);
        assertEquals(4_000L, snapshot.samples());
        assertEquals(2_000L, snapshot.failures());
        assertEquals(400_000L, snapshot.totalNanos());
        assertEquals(100L, snapshot.maxNanos());
        assertEquals(127L, snapshot.p99UpperBoundNanos());
    }
}
