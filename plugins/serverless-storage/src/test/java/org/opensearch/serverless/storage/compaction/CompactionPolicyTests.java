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

    public void testSingleSegmentIsNeverACandidate() {
        CompactionPolicy policy = new CompactionPolicy(10, 5 * GB, 0.2);
        assertFalse(policy.shouldCompact(1, 100, 100, 0.9));
        assertFalse(policy.shouldCompact(0, 0, 0, 0.0));
    }

    public void testHighDeleteRatioTriggersCompactionRegardlessOfSegmentCount() {
        CompactionPolicy policy = new CompactionPolicy(10, 5 * GB, 0.2);
        // Only 2 segments (well below the count threshold of 10), but 25% deleted -- still a candidate.
        assertTrue(policy.shouldCompact(2, 1000, 500, 0.25));
    }

    public void testDeleteRatioBelowThresholdWithFewSegmentsIsNotACandidate() {
        CompactionPolicy policy = new CompactionPolicy(10, 5 * GB, 0.2);
        assertFalse(policy.shouldCompact(3, 1000, 500, 0.1));
    }

    public void testManySmallSegmentsTriggersCompactionEvenWithNoDeletes() {
        CompactionPolicy policy = new CompactionPolicy(10, 5 * GB, 0.2);
        assertTrue(policy.shouldCompact(15, 1000, 100, 0.0));
    }

    public void testManySegmentsButDominatedByOneHugeSegmentIsNotACandidate() {
        // 15 segments, but one already at 5GB -- merging the small remainder in again isn't
        // worth it (rfc-serverless-opensearch.md section 7.4's size-tiered rationale).
        CompactionPolicy policy = new CompactionPolicy(10, 5 * GB, 0.2);
        assertFalse(policy.shouldCompact(15, 6 * GB, 5 * GB, 0.0));
    }

    public void testBoundaryAtExactSegmentCountThreshold() {
        CompactionPolicy policy = new CompactionPolicy(10, 5 * GB, 0.2);
        assertTrue(policy.shouldCompact(10, 1000, 100, 0.0));
        assertFalse(policy.shouldCompact(9, 1000, 100, 0.0));
    }

    public void testBoundaryAtExactDeleteRatioThreshold() {
        CompactionPolicy policy = new CompactionPolicy(10, 5 * GB, 0.2);
        assertTrue(policy.shouldCompact(2, 1000, 500, 0.2));
        assertFalse(policy.shouldCompact(2, 1000, 500, 0.19999));
    }

    public void testDefaultsAreUsable() {
        CompactionPolicy policy = CompactionPolicy.withDefaults();
        assertTrue(policy.shouldCompact(20, 1000, 100, 0.0));
        assertFalse(policy.shouldCompact(2, 1000, 500, 0.0));
    }

    public void testConstructorRejectsInvalidArguments() {
        expectThrows(IllegalArgumentException.class, () -> new CompactionPolicy(1, 5 * GB, 0.2));
        expectThrows(IllegalArgumentException.class, () -> new CompactionPolicy(10, 0, 0.2));
        expectThrows(IllegalArgumentException.class, () -> new CompactionPolicy(10, 5 * GB, -0.1));
        expectThrows(IllegalArgumentException.class, () -> new CompactionPolicy(10, 5 * GB, 1.1));
    }
}
