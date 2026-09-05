/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction;

import org.opensearch.test.OpenSearchTestCase;

public class CompactionPolicyTests extends OpenSearchTestCase {

    private static final long GB = 1024L * 1024 * 1024;
    /**
     * A large-but-legal target bundle size. It used to be 5 GB here and in the shipped defaults,
     * which CompactionPolicy now rejects: anything above the 2 GiB whole-file read ceiling lets
     * compaction produce a segment no reader can ever materialize and no compaction can ever read
     * again -- see CompactionPolicy#MAX_TARGET_BUNDLE_SIZE_BYTES.
     */
    private static final long BIG = 2000L * 1024 * 1024;

    public void testSingleSegmentIsNeverACandidate() {
        CompactionPolicy policy = new CompactionPolicy(10, BIG, 0.2);
        assertFalse(policy.shouldCompact(1, 100, 100, 0.9));
        assertFalse(policy.shouldCompact(0, 0, 0, 0.0));
    }

    public void testHighDeleteRatioTriggersCompactionRegardlessOfSegmentCount() {
        CompactionPolicy policy = new CompactionPolicy(10, BIG, 0.2);
        // Only 2 segments (well below the count threshold of 10), but 25% deleted -- still a candidate.
        assertTrue(policy.shouldCompact(2, 1000, 500, 0.25));
    }

    public void testDeleteRatioBelowThresholdWithFewSegmentsIsNotACandidate() {
        CompactionPolicy policy = new CompactionPolicy(10, BIG, 0.2);
        assertFalse(policy.shouldCompact(3, 1000, 500, 0.1));
    }

    public void testManySmallSegmentsTriggersCompactionEvenWithNoDeletes() {
        CompactionPolicy policy = new CompactionPolicy(10, BIG, 0.2);
        assertTrue(policy.shouldCompact(15, 1000, 100, 0.0));
    }

    public void testManySegmentsButDominatedByOneHugeSegmentIsNotACandidate() {
        // 15 segments, but one already at 5GB -- merging the small remainder in again isn't
        // worth it (rfc-serverless-opensearch.md section 7.4's size-tiered rationale).
        CompactionPolicy policy = new CompactionPolicy(10, BIG, 0.2);
        assertFalse(policy.shouldCompact(15, 6 * GB, 5 * GB, 0.0));
    }

    public void testBoundaryAtExactSegmentCountThreshold() {
        CompactionPolicy policy = new CompactionPolicy(10, BIG, 0.2);
        assertTrue(policy.shouldCompact(10, 1000, 100, 0.0));
        assertFalse(policy.shouldCompact(9, 1000, 100, 0.0));
    }

    public void testBoundaryAtExactDeleteRatioThreshold() {
        CompactionPolicy policy = new CompactionPolicy(10, BIG, 0.2);
        assertTrue(policy.shouldCompact(2, 1000, 500, 0.2));
        assertFalse(policy.shouldCompact(2, 1000, 500, 0.19999));
    }

    public void testDefaultsAreUsable() {
        CompactionPolicy policy = CompactionPolicy.withDefaults();
        assertTrue(policy.shouldCompact(20, 1000, 100, 0.0));
        assertFalse(policy.shouldCompact(2, 1000, 500, 0.0));
    }

    public void testConstructorRejectsInvalidArguments() {
        expectThrows(IllegalArgumentException.class, () -> new CompactionPolicy(1, BIG, 0.2));
        expectThrows(IllegalArgumentException.class, () -> new CompactionPolicy(10, 0, 0.2));
        expectThrows(IllegalArgumentException.class, () -> new CompactionPolicy(10, BIG, -0.1));
        expectThrows(IllegalArgumentException.class, () -> new CompactionPolicy(10, BIG, 1.1));
    }

    /**
     * The regression this bound exists for: a 5 GB target bundle size -- which is exactly what
     * {@code withDefaults()} used to ship -- explicitly permits compaction to produce a single
     * merged segment above the 2 GiB whole-file read ceiling, after which no reader can materialize
     * the shard and no compaction can read it either, permanently, with no recovery path. Before
     * this bound existed the constructor accepted it without comment.
     */
    public void testConstructorRejectsATargetBundleSizeAboveTheWholeFileReadCeiling() {
        IllegalArgumentException tooBig = expectThrows(IllegalArgumentException.class, () -> new CompactionPolicy(10, 5 * GB, 0.2));
        assertTrue(tooBig.getMessage(), tooBig.getMessage().contains("whole-file read ceiling"));
        // Exactly at the ceiling is fine; one byte over is not.
        new CompactionPolicy(10, CompactionPolicy.MAX_TARGET_BUNDLE_SIZE_BYTES, 0.2);
        expectThrows(
            IllegalArgumentException.class,
            () -> new CompactionPolicy(10, CompactionPolicy.MAX_TARGET_BUNDLE_SIZE_BYTES + 1, 0.2)
        );
    }

    /** The shipped defaults must themselves satisfy the bound they are the reason for. */
    public void testDefaultsStayUnderTheWholeFileReadCeiling() {
        // withDefaults() would throw from its own constructor if it did not, but asserting it here
        // states the invariant where someone changing the default will actually read it.
        CompactionPolicy defaults = CompactionPolicy.withDefaults();
        assertEquals(1, defaults.targetSegmentCount(GB));
        assertEquals(2, defaults.targetSegmentCount(GB + 1));
    }

    public void testTargetSegmentCountIsOneWhenTotalSizeFitsInOneBundle() {
        CompactionPolicy policy = new CompactionPolicy(10, BIG, 0.2);
        assertEquals(1, policy.targetSegmentCount(0));
        assertEquals(1, policy.targetSegmentCount(GB));
        assertEquals(1, policy.targetSegmentCount(BIG));
    }

    public void testTargetSegmentCountScalesWithTotalSize() {
        CompactionPolicy policy = new CompactionPolicy(10, GB, 0.2);
        assertEquals(2, policy.targetSegmentCount(GB + 1));
        assertEquals(3, policy.targetSegmentCount(2 * GB + 1));
        assertEquals(10, policy.targetSegmentCount(10 * GB));
    }

    public void testTargetSegmentCountNeverGoesBelowOne() {
        CompactionPolicy policy = new CompactionPolicy(10, GB, 0.2);
        assertEquals(1, policy.targetSegmentCount(-1));
        assertEquals(1, policy.targetSegmentCount(0));
    }
}
