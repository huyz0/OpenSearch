/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.manifest;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class PruningStatsTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        PruningStats original = new PruningStats(
            1234,
            1_700_000_000_000L,
            1_700_000_500_000L,
            Map.of("price", new PruningStats.FieldRange(1, 100), "qty", new PruningStats.FieldRange(-5, 5))
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        PruningStats deserialized = new PruningStats(out.bytes().streamInput());

        assertEquals(original, deserialized);
    }

    public void testEmptyStatsRoundTrip() throws Exception {
        PruningStats original = PruningStats.empty();
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        PruningStats deserialized = new PruningStats(out.bytes().streamInput());

        assertEquals(original, deserialized);
        assertNull(deserialized.minTimestampMillis());
        assertNull(deserialized.maxTimestampMillis());
    }

    // The pruning-safety invariant from rfc-serverless-metadata-plane.md section 5.1: a shard
    // with no timestamp stats must never be pruned away, only ever a candidate for activation.
    public void testMightOverlapTimeRangeDefaultsToTrueWhenNoStatsPresent() {
        assertTrue(PruningStats.empty().mightOverlapTimeRange(0, Long.MAX_VALUE));
        assertTrue(PruningStats.empty().mightOverlapTimeRange(500, 600));
    }

    public void testMightOverlapTimeRangeDetectsDisjointRanges() {
        PruningStats stats = new PruningStats(10, 1000L, 2000L, Map.of());

        assertTrue(stats.mightOverlapTimeRange(1500, 2500));
        assertTrue(stats.mightOverlapTimeRange(500, 1500));
        assertTrue(stats.mightOverlapTimeRange(1200, 1800)); // fully contained
        assertTrue(stats.mightOverlapTimeRange(0, 5000)); // fully containing

        assertFalse(stats.mightOverlapTimeRange(2001, 3000));
        assertFalse(stats.mightOverlapTimeRange(0, 999));
    }

    public void testFieldRangeOverlapDetection() {
        PruningStats.FieldRange range = new PruningStats.FieldRange(10, 20);
        assertTrue(range.mightOverlap(15, 25));
        assertTrue(range.mightOverlap(0, 10));
        assertTrue(range.mightOverlap(20, 30));
        assertFalse(range.mightOverlap(21, 30));
        assertFalse(range.mightOverlap(0, 9));
    }

    public void testConstructorRejectsInvertedTimestampRange() {
        expectThrows(IllegalArgumentException.class, () -> new PruningStats(0, 2000L, 1000L, Map.of()));
    }

    public void testConstructorRejectsInvertedFieldRange() {
        expectThrows(IllegalArgumentException.class, () -> new PruningStats.FieldRange(100, 1));
    }

    public void testConstructorRejectsNegativeDocumentCount() {
        expectThrows(IllegalArgumentException.class, () -> new PruningStats(-1, null, null, Map.of()));
    }
}
