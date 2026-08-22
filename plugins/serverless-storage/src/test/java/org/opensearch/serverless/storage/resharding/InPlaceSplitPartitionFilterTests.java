/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.opensearch.cluster.metadata.ShardRange;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.test.OpenSearchTestCase;

public class InPlaceSplitPartitionFilterTests extends OpenSearchTestCase {

    public void testDocMatchesItsOwnShardsHashRange() {
        String id = "doc-matches-own-range";
        int hash = Murmur3HashFunction.hash(id);
        ShardRange range = new ShardRange(0, hash, hash);
        assertTrue("a range containing exactly this id's hash must match it", InPlaceSplitPartitionFilter.matches(id, range));
    }

    public void testDocFromADifferentRangeIsExcluded() {
        String id = "doc-excluded";
        int hash = Murmur3HashFunction.hash(id);
        ShardRange range = new ShardRange(0, hash + 1, Integer.MAX_VALUE);
        assertFalse("a range that starts strictly after this id's hash must not match it", InPlaceSplitPartitionFilter.matches(id, range));
    }

    public void testBoundaryValueAtRangeStartMatches() {
        String id = "doc-boundary-start";
        int hash = Murmur3HashFunction.hash(id);
        ShardRange range = new ShardRange(0, hash, hash + 1000);
        assertTrue(
            "a hash exactly equal to the range's start must match (inclusive lower bound)",
            InPlaceSplitPartitionFilter.matches(id, range)
        );
    }

    public void testBoundaryValueAtRangeEndMatches() {
        String id = "doc-boundary-end";
        int hash = Murmur3HashFunction.hash(id);
        ShardRange range = new ShardRange(0, hash - 1000, hash);
        assertTrue(
            "a hash exactly equal to the range's end must match (inclusive upper bound)",
            InPlaceSplitPartitionFilter.matches(id, range)
        );
    }

    public void testJustOutsideRangeStartIsExcluded() {
        String id = "doc-just-below";
        int hash = Murmur3HashFunction.hash(id);
        // range starts one above this id's hash -- must not match.
        ShardRange range = new ShardRange(0, hash + 1, hash + 1000);
        assertFalse(InPlaceSplitPartitionFilter.matches(id, range));
    }

    public void testJustOutsideRangeEndIsExcluded() {
        String id = "doc-just-above";
        int hash = Murmur3HashFunction.hash(id);
        // range ends one below this id's hash -- must not match.
        ShardRange range = new ShardRange(0, hash - 1000, hash - 1);
        assertFalse(InPlaceSplitPartitionFilter.matches(id, range));
    }

    public void testAgreesWithCoresRealMurmur3HashAndShardRangeContainsDirectly() {
        for (int i = 0; i < 1000; i++) {
            String id = "agreement-doc-" + i;
            int hash = Murmur3HashFunction.hash(id);
            ShardRange containingRange = new ShardRange(0, hash - 1, hash + 1);
            ShardRange excludingRange = new ShardRange(1, hash + 1, Integer.MAX_VALUE);

            assertEquals(containingRange.contains(hash), InPlaceSplitPartitionFilter.matches(id, containingRange));
            assertEquals(excludingRange.contains(hash), InPlaceSplitPartitionFilter.matches(id, excludingRange));
            assertTrue(InPlaceSplitPartitionFilter.matches(id, containingRange));
            assertFalse(InPlaceSplitPartitionFilter.matches(id, excludingRange));
        }
    }

    public void testEveryIdMatchesExactlyOneOfTwoDisjointAdjacentRanges() {
        for (int i = 0; i < 500; i++) {
            String id = "split-doc-" + i;
            int hash = Murmur3HashFunction.hash(id);
            // Split the full int range into two disjoint, adjacent, inclusive-bounded ranges at the
            // midpoint, mirroring how a real in-place split's children partition the parent's range.
            long midpoint = ((long) Integer.MIN_VALUE + Integer.MAX_VALUE) / 2;
            ShardRange lower = new ShardRange(0, Integer.MIN_VALUE, (int) midpoint);
            ShardRange upper = new ShardRange(1, (int) midpoint + 1, Integer.MAX_VALUE);

            boolean matchesLower = InPlaceSplitPartitionFilter.matches(id, lower);
            boolean matchesUpper = InPlaceSplitPartitionFilter.matches(id, upper);
            assertTrue(
                "id [" + id + "] with hash " + hash + " must match exactly one of the two disjoint ranges",
                matchesLower ^ matchesUpper
            );
        }
    }
}
