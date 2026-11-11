/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.routing.RoutingHash;
import org.opensearch.serverless.routing.ShardMode;
import org.opensearch.serverless.routing.UnitLayout;
import org.opensearch.serverless.routing.UnitLayoutFormat;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * The range map from 64-bit routing hashes to units: declared shards as equal slices of the hash space,
 * lookup by cut point, the shape rule splits and merges keep, and the stored format.
 */
public class UnitLayoutTests extends OpenSearchTestCase {

    private static final int[] COUNTS = { 1, 2, 3, 5, 7, 10, 64, 100, 1000, 4096 };

    /** Declared shard i starts exactly where the hash-to-shard function first answers i, for awkward counts too. */
    public void testDeclaredShardBoundariesAgreeWithTheHashToShardFunction() {
        for (int n : COUNTS) {
            assertEquals(0L, RoutingHash.firstHashOf(0, n));
            assertEquals(-1L, RoutingHash.lastHashOf(n - 1, n));
            for (int i = 1; i < n; i++) {
                final long first = RoutingHash.firstHashOf(i, n);
                assertEquals("n=" + n + " i=" + i, i, RoutingHash.declaredShard(first, n));
                assertEquals("n=" + n + " i=" + i, i - 1, RoutingHash.declaredShard(first - 1L, n));
                assertEquals(first - 1L, RoutingHash.lastHashOf(i - 1, n));
            }
            assertEquals(n - 1, RoutingHash.declaredShard(-1L, n));
        }
    }

    /** Declared shards are equal slices: no two differ in width by more than one hash. */
    public void testDeclaredShardsAreEqualSlices() {
        for (int n : COUNTS) {
            final long width = Long.divideUnsigned(-1L, n);
            for (int i = 0; i < n; i++) {
                final long size = RoutingHash.lastHashOf(i, n) - RoutingHash.firstHashOf(i, n);
                assertTrue("n=" + n + " i=" + i, Math.abs(size - width) <= 1L);
            }
        }
    }

    /** The hash is stable: it is part of the stored format, and changing it would re-route every document. */
    public void testTheRoutingHashIsStable() {
        assertEquals(RoutingHash.of("doc-1"), RoutingHash.of("doc-1"));
        assertNotEquals(RoutingHash.of("doc-1"), RoutingHash.of("doc-2"));
        // A known value pins the function: the first half of Murmur3 x64 128 with seed 0.
        assertEquals("cbd8a7b341bd9b02", Long.toHexString(RoutingHash.of("hello")));
    }

    /** Fixed mode is one unit per declared shard, with unit id equal to the declared shard. */
    public void testFixedModeStartsWithOneUnitPerDeclaredShard() {
        for (int n : COUNTS) {
            final UnitLayout layout = UnitLayout.initial(ShardMode.FIXED, n);
            assertEquals(n, layout.unitCount());
            assertEquals(n, layout.nextUnitId());
            for (int i = 0; i < 200; i++) {
                final long hash = randomLong();
                assertEquals(RoutingHash.declaredShard(hash, n), layout.unitFor(hash).id());
            }
            for (int shard = 0; shard < n; shard++) {
                assertEquals(List.of(layout.unit(shard)), layout.unitsOf(shard));
                assertFalse(layout.needsFilter(layout.unit(shard), shard));
            }
        }
    }

    /** Adaptive mode is one unit holding every declared shard, which a routed search must filter. */
    public void testAdaptiveModeStartsWithOneUnit() {
        final UnitLayout layout = UnitLayout.initial(ShardMode.ADAPTIVE, 100);
        assertEquals(1, layout.unitCount());
        assertEquals(100, layout.declaredShards());
        assertEquals(0, layout.unitForRouting("anything").id());
        assertTrue(layout.needsFilter(layout.unit(0), 7));
        final UnitLayout single = UnitLayout.initial(ShardMode.ADAPTIVE, 1);
        assertFalse("one declared shard needs no filter", single.needsFilter(single.unit(0), 0));
    }

    /** Splits cut along declared-shard boundaries first, then halve inside one, and never move a document's declared shard. */
    public void testSplitsCutDeclaredShardBoundariesFirstThenHalve() {
        UnitLayout layout = UnitLayout.initial(ShardMode.ADAPTIVE, 5);
        UnitLayout.Split split = layout.planSplit(0);
        assertEquals(RoutingHash.firstHashOf(2, 5), split.right().first());
        layout = layout.apply(split);
        assertEquals(2, layout.unitCount());
        assertEquals(List.of(1, 2), layout.units().stream().map(UnitLayout.Unit::id).toList());

        // Keep splitting the largest unit until every unit is at most one declared shard, then once more.
        UnitLayout.Unit wide;
        while ((wide = firstHoldingSeveral(layout, 5)) != null) {
            layout = layout.apply(layout.planSplit(wide.id()));
        }
        assertEquals(5, layout.unitCount());
        for (int shard = 0; shard < 5; shard++) {
            assertEquals(1, layout.unitsOf(shard).size());
            assertFalse(layout.needsFilter(layout.unitsOf(shard).get(0), shard));
        }
        final UnitLayout.Unit whole = layout.unitsOf(3).get(0);
        layout = layout.apply(layout.planSplit(whole.id()));
        assertEquals(2, layout.unitsOf(3).size());
        for (UnitLayout.Unit piece : layout.unitsOf(3)) {
            assertFalse("a piece of one declared shard holds nothing else", layout.needsFilter(piece, 3));
        }
    }

    private static UnitLayout.Unit firstHoldingSeveral(UnitLayout layout, int n) {
        for (UnitLayout.Unit unit : layout.units()) {
            if (RoutingHash.declaredShard(unit.last(), n) > RoutingHash.declaredShard(unit.first(), n)) {
                return unit;
            }
        }
        return null;
    }

    /** Random splits and merges: lookup always agrees with a linear scan, and a hash's declared shard is among the units of it. */
    public void testLookupAgreesWithALinearScanAcrossRandomSplits() {
        final int n = COUNTS[randomIntBetween(0, COUNTS.length - 1)];
        UnitLayout layout = UnitLayout.initial(randomFrom(ShardMode.values()), n);
        for (int round = 0; round < 60; round++) {
            final List<UnitLayout.Unit> units = layout.units();
            if (randomBoolean() || units.size() == 1) {
                layout = layout.apply(layout.planSplit(randomFrom(units).id()));
            } else {
                final int at = randomIntBetween(0, units.size() - 2);
                try {
                    layout = layout.merge(units.get(at).id(), units.get(at + 1).id());
                } catch (IllegalArgumentException e) {
                    assertTrue(e.getMessage(), e.getMessage().contains("declared shard"));
                }
            }
            for (int i = 0; i < 50; i++) {
                final long hash = randomLong();
                UnitLayout.Unit expected = null;
                for (UnitLayout.Unit unit : layout.units()) {
                    if (unit.covers(hash)) {
                        assertNull("two units cover one hash", expected);
                        expected = unit;
                    }
                }
                assertEquals(expected, layout.unitFor(hash));
                assertTrue(layout.unitsOf(RoutingHash.declaredShard(hash, n)).contains(expected));
            }
        }
    }

    /** A unit holding part of one declared shard and any of another breaks the shape rule and is refused. */
    public void testALayoutBreakingTheShapeRuleIsRefused() {
        final long mid = RoutingHash.firstHashOf(1, 4) / 2;
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new UnitLayout(ShardMode.ADAPTIVE, 4, List.of(new UnitLayout.Unit(0, 0L, mid), new UnitLayout.Unit(1, mid + 1L, -1L)), 2)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("part of a declared shard"));
        expectThrows(IllegalArgumentException.class, () -> new UnitLayout(ShardMode.FIXED, 1, List.of(new UnitLayout.Unit(0, 1L, -1L)), 1));
        expectThrows(IllegalArgumentException.class, () -> new UnitLayout(ShardMode.FIXED, 1, List.of(new UnitLayout.Unit(0, 0L, -2L)), 1));
        expectThrows(IllegalArgumentException.class, () -> new UnitLayout(ShardMode.FIXED, 1, List.of(new UnitLayout.Unit(3, 0L, -1L)), 1));
    }

    /** Merging two pieces of one declared shard is allowed; a piece with a different declared shard is not. */
    public void testMergesKeepTheShapeRule() {
        UnitLayout layout = UnitLayout.initial(ShardMode.FIXED, 2);
        layout = layout.apply(layout.planSplit(0));
        final List<UnitLayout.Unit> pieces = layout.unitsOf(0);
        assertEquals(2, pieces.size());
        final UnitLayout split = layout;
        expectThrows(IllegalArgumentException.class, () -> split.merge(pieces.get(1).id(), 1));
        expectThrows(IllegalArgumentException.class, () -> split.merge(pieces.get(0).id(), 1));
        final UnitLayout merged = layout.merge(pieces.get(0).id(), pieces.get(1).id());
        assertEquals(2, merged.unitCount());
        assertEquals(1, merged.unitsOf(0).size());
        assertEquals(RoutingHash.lastHashOf(0, 2), merged.unitsOf(0).get(0).last());
    }

    /** The stored format round-trips, writes hashes as unsigned hex, and refuses a layout with a gap. */
    public void testTheStoredFormatRoundTrips() throws IOException {
        UnitLayout layout = UnitLayout.initial(ShardMode.ADAPTIVE, 10);
        layout = layout.apply(layout.planSplit(0));
        final String json = UnitLayoutFormat.toJson(layout);
        assertTrue(json, json.contains("\"last\":\"ffffffffffffffff\""));
        assertEquals(layout, UnitLayoutFormat.fromJson(json));
        final String gap = json.replaceFirst("\"first\":\"0\"", "\"first\":\"5\"");
        assertNotEquals(json, gap);
        final IOException e = expectThrows(IOException.class, () -> UnitLayoutFormat.fromJson(gap));
        assertTrue(e.getMessage(), e.getMessage().contains("malformed unit layout"));
    }

    /** A descriptor with a layout round-trips; one without writes exactly what it wrote before layouts existed. */
    public void testTheDescriptorCarriesTheLayoutOnlyWhenItHasOne() throws IOException {
        final IndexDescriptor plain = new IndexDescriptor("plain", "uuid-plain-000000000", 3, null, null);
        assertNull(plain.layout());
        assertEquals(3, plain.declaredShards());
        assertFalse(plain.toBytes().utf8ToString().contains("unit_layout"));
        assertEquals(plain, IndexDescriptor.fromStream(plain.toBytes().streamInput()));

        final IndexDescriptor adaptive = new IndexDescriptor("grow", "uuid-grow-0000000000", 8, null, null).withLayout(
            UnitLayout.initial(ShardMode.ADAPTIVE, 8)
        );
        assertEquals("one unit, the physical shard count", 1, adaptive.numberOfShards());
        assertEquals(8, adaptive.declaredShards());
        final IndexDescriptor read = IndexDescriptor.fromStream(adaptive.toBytes().streamInput());
        assertEquals(adaptive, read);
        assertEquals(adaptive.layout(), read.layout());
        assertEquals("a later edit keeps the layout", adaptive.layout(), read.withMapping("{}").layout());

        final IndexDescriptor fixed = new IndexDescriptor("fixed", "uuid-fixed-000000000", 4, null, null).withLayout(
            UnitLayout.initial(ShardMode.FIXED, 4)
        );
        assertEquals(4, fixed.numberOfShards());
        assertEquals(fixed.layout(), IndexDescriptor.fromStream(fixed.toBytes().streamInput()).layout());

        // Unit ids must be the physical shards 0..n-1 while callers enumerate shards by count.
        final UnitLayout split = UnitLayout.initial(ShardMode.ADAPTIVE, 8).apply(UnitLayout.initial(ShardMode.ADAPTIVE, 8).planSplit(0));
        expectThrows(IllegalArgumentException.class, () -> adaptive.withLayout(split));
    }
}
