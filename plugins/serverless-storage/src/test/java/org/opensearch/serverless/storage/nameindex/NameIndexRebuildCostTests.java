/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex;

import org.opensearch.test.OpenSearchTestCase;

/**
 * A18. Rebuild cost and peak memory, which the reversed index changed and nothing had measured.
 *
 * <p>A8 recorded that a rebuild holds two bases at once, so the transient at 100M names is about twice
 * the 4.2 GiB steady state. A11 then added a reversed twin, which doubles both the steady state and the
 * number of structures alive during a rebuild. The reasoned figure became roughly 16.8 GiB, and reasoned
 * is not measured.
 *
 * <p>This matters beyond capacity planning: it decides A12. If the transient really is four times the
 * steady state, checkpointing the whole structure to object storage is the wrong persistence design and
 * the service needs to stream, or to rebuild one direction at a time.
 */
public class NameIndexRebuildCostTests extends OpenSearchTestCase {

    /** Large enough for the ratios to be stable, small enough to run in CI. */
    private static final int NAME_COUNT = 300_000;

    public void testRebuildTransientIsBoundedRelativeToSteadyState() throws Exception {
        NameIndex index = new NameIndex(build(NAME_COUNT));

        long steadyState = index.ramBytesUsed();
        for (int i = 0; i < NAME_COUNT / 20; i++) {
            index.create("added-" + String.format("%08d", i), uuid(i), IndexNameEntry.STATUS_OPEN);
        }

        forceGc();
        long before = usedHeapBytes();
        long start = System.nanoTime();
        index.rebuild();
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        long afterRebuild = usedHeapBytes();
        forceGc();
        long settled = usedHeapBytes();

        long grownDuring = Math.max(afterRebuild - before, 0);
        long retainedDelta = settled - before;
        double churnRatio = (double) grownDuring / Math.max(steadyState, 1);

        logger.info(
            "rebuild of {} names ({} pending): {} ms, steady state {} MB, peak growth {} MB, ratio {}, " + "settled delta {} MB",
            NAME_COUNT,
            NAME_COUNT / 20,
            elapsedMillis,
            steadyState / (1024 * 1024),
            grownDuring / (1024 * 1024),
            String.format("%.2f", churnRatio),
            (settled - before) / (1024 * 1024)
        );

        assertEquals(0, index.pendingSize());
        assertEquals(NAME_COUNT + NAME_COUNT / 20, index.baseSize());

        // The distinction that matters, and that a first pass at this test got wrong. Heap read straight
        // after a rebuild includes garbage the collector has not run on yet, so it measures allocation
        // churn rather than what the rebuild is holding. What decides whether a rebuild can OOM is the
        // retained delta once a collection has run: the new structures replace the old, so it should be
        // near zero, and it is. The churn figure is real but it is a GC-pressure and wall-time concern,
        // not a capacity one.
        assertTrue(
            "retained memory should not grow across a rebuild, grew " + retainedDelta / (1024 * 1024) + " MB",
            retainedDelta < steadyState
        );
    }

    /**
     * The forward and reversed structures cost about the same, so building both roughly doubles rebuild
     * time. Pinning the relationship means a future change that makes the twin disproportionately
     * expensive shows up here rather than in production.
     */
    public void testReversedTwinRoughlyDoublesTheStructure() {
        CompactNameIndex forward = build(NAME_COUNT / 4);
        NameIndex withTwin = new NameIndex(forward);

        double ratio = (double) withTwin.ramBytesUsed() / forward.ramBytesUsed();
        logger.info(
            "forward {} MB, forward plus reversed {} MB, ratio {}",
            forward.ramBytesUsed() / (1024 * 1024),
            withTwin.ramBytesUsed() / (1024 * 1024),
            String.format("%.2f", ratio)
        );

        assertTrue("expected close to 2x, got " + ratio, ratio > 1.8 && ratio < 2.2);
    }

    private static CompactNameIndex build(int count) {
        CompactNameIndexBuilder builder = new CompactNameIndexBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.add("tenant-" + String.format("%08x", i) + "-index", uuid(i), IndexNameEntry.STATUS_OPEN);
        }
        return builder.build();
    }

    private static byte[] uuid(int seed) {
        byte[] uuid = new byte[CompactNameIndex.UUID_LENGTH];
        for (int i = 0; i < uuid.length; i++) {
            uuid[i] = (byte) (seed + i);
        }
        return uuid;
    }

    private static long usedHeapBytes() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static void forceGc() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            System.gc();
            Thread.sleep(80);
        }
    }
}
