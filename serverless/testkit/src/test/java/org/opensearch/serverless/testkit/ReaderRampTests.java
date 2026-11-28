/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.serverless.cluster.ReaderPlacement;
import org.opensearch.serverless.cluster.ReaderRamp;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** A joining reader's slow start: the ramp, the ranking, the controller and the lease fields, agreeing with the Rust build. */
public class ReaderRampTests extends OpenSearchTestCase {
    private static final ReaderRamp.Config CFG = new ReaderRamp.Config(30_000, 300_000);

    private static NodeLease lease(String id, long since, int ramp) {
        return new NodeLease(id, "e", "a", Set.of("search"), 0L).withSearchRamp(since, ramp);
    }

    public void testRampIsBoundedByTheFloorAndCeilingCurves() {
        final NodeLease silent = lease("n", 1_000, -1);
        assertEquals(0, ReaderRamp.permille(silent, 1_000, CFG));
        assertEquals(500, ReaderRamp.permille(silent, 151_000, CFG));
        assertEquals(1000, ReaderRamp.permille(silent, Long.MAX_VALUE, CFG));
        final NodeLease eager = lease("n", 0, 1000);
        assertEquals(500, ReaderRamp.permille(eager, 15_000, CFG));
        assertEquals(1000, ReaderRamp.permille(eager, 30_000, CFG));
        final NodeLease cold = lease("n", 0, 0);
        assertEquals(500, ReaderRamp.permille(cold, 150_000, CFG));
        assertEquals(200, ReaderRamp.permille(lease("n", 0, 200), 60_000, CFG));
        assertEquals(1000, ReaderRamp.permille(lease("n", -1, 0), 1, CFG));
        assertEquals(1000, ReaderRamp.permille(lease("n", 10, 0), 5, CFG));
        assertEquals(1000, ReaderRamp.permille(lease("n", 0, 0), 1, ReaderRamp.Config.OFF));
        assertEquals(1000, ReaderRamp.permille(lease("n", Long.MIN_VALUE + 1, 5), Long.MAX_VALUE, CFG));
    }

    public void testFixtureAgreesWithTheRustBuild() throws IOException {
        final List<String> rows;
        try (var in = getClass().getResourceAsStream("ramp-fixture.txt")) {
            rows = List.of(new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n"));
        }
        final List<NodeLease> fleet = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            fleet.add(new NodeLease("node-" + i, "e", "a", Set.of("search"), 0L));
        }
        fleet.add(lease("joiner", 1_000, 200));
        int ranked = 0;
        for (String row : rows) {
            final String[] f = row.split(" ");
            switch (f[0]) {
                case "R" -> {
                    final NodeLease l = lease("n", Long.parseLong(f[1]), "-".equals(f[2]) ? -1 : Integer.parseInt(f[2]));
                    assertEquals(row, Integer.parseInt(f[4]), ReaderRamp.permille(l, Long.parseLong(f[3]), CFG));
                }
                case "A" -> assertEquals(row, Integer.parseInt(f[4]), ReaderRamp.admission(f[1], f[2], Integer.parseInt(f[3])));
                case "K" -> {
                    ranked++;
                    final List<String> got = ReaderPlacement.candidatesFor(
                        f[1],
                        Integer.parseInt(f[2]),
                        fleet,
                        "search",
                        3,
                        CFG,
                        Long.parseLong(f[3])
                    );
                    assertEquals(row, f[4], String.join(",", got));
                }
                default -> fail("unknown row " + row);
            }
        }
        assertEquals(160, ranked);
    }

    public void testSlowStartOffRanksAsBefore() {
        final List<NodeLease> fleet = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            fleet.add(new NodeLease("node-" + i, "e", "a", Set.of("search"), 0L));
        }
        fleet.add(lease("joiner", 0, 0));
        for (int shard = 0; shard < 300; shard++) {
            assertEquals(
                ReaderPlacement.candidatesFor("idx", shard, fleet, "search", 3),
                ReaderPlacement.candidatesFor("idx", shard, fleet, "search", 3, ReaderRamp.Config.OFF, 1L)
            );
        }
    }

    public void testJoinerTakesItsShareInProportionAndNeverGivesAShardBack() {
        final ReaderRamp.Config cfg = new ReaderRamp.Config(0, 100_000);
        final List<NodeLease> fleet = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            fleet.add(new NodeLease("node-" + i, "e", "a", Set.of("search"), 0L));
        }
        fleet.add(lease("joiner", 0, -1));
        int full = 0;
        int half = 0;
        for (int shard = 0; shard < 2000; shard++) {
            final boolean atQuarter = first(fleet, shard, 25_000, cfg);
            final boolean atHalf = first(fleet, shard, 50_000, cfg);
            final boolean atFull = first(fleet, shard, 100_000, cfg);
            assertFalse(first(fleet, shard, 0, cfg));
            full += atFull ? 1 : 0;
            half += atHalf ? 1 : 0;
            if (atQuarter) {
                assertTrue("shard " + shard + " moved back", atHalf && atFull);
            }
        }
        assertTrue("share " + full, full > 100);
        assertTrue(half + " vs " + full / 2, Math.abs(half - full / 2) * 20 <= 2000);
    }

    private static boolean first(List<NodeLease> fleet, int shard, long now, ReaderRamp.Config cfg) {
        return "joiner".equals(ReaderPlacement.candidatesFor("idx", shard, fleet, "search", 1, cfg, now).get(0));
    }

    public void testControllerStepsUpOnWarmWindowsAndHoldsOtherwise() {
        final var c = ReaderRamp.Controller.withDefaults(new ReaderRamp.Totals(0, 0));
        assertEquals(100, c.ramp());
        assertEquals(100, c.observe(new ReaderRamp.Totals(19, 0)));
        assertEquals(200, c.observe(new ReaderRamp.Totals(20, 0)));
        assertEquals(200, c.observe(new ReaderRamp.Totals(50, 90_000)));
        assertEquals(300, c.observe(new ReaderRamp.Totals(70, 110_000)));
        for (int i = 0; i < 20; i++) {
            c.observe(new ReaderRamp.Totals(90 + i * 20L, 110_000));
        }
        assertEquals(1000, c.ramp());
    }

    public void testLeaseFieldsRoundTripAndAnAbsentPairIsWrittenAsBefore() throws IOException {
        final NodeLease plain = new NodeLease("n1", "e1", "a", Set.of("search"), 5L);
        final String plainText = plain.toBytes().utf8ToString();
        assertFalse(plainText, plainText.contains("search_since") || plainText.contains("ramp_permille"));
        final NodeLease ramped = plain.withSearchRamp(1_700_000_000_000L, 300);
        final String text = ramped.toBytes().utf8ToString();
        assertTrue(text, text.endsWith(",\"search_since\":1700000000000,\"ramp_permille\":300}"));
        final NodeLease back = NodeLease.fromStream(new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)));
        assertEquals(1_700_000_000_000L, back.searchSince());
        assertEquals(300, back.ramp());
        final String wild =
            "{\"node_id\":\"n\",\"ephemeral_id\":\"e\",\"address\":\"a\",\"roles\":[],\"expires_at_millis\":1,\"ramp_permille\":5000}";
        assertEquals(1000, NodeLease.fromStream(new ByteArrayInputStream(wild.getBytes(StandardCharsets.UTF_8))).ramp());
    }
}
