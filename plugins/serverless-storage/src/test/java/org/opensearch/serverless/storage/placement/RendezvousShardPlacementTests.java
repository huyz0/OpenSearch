/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * C1. The placement function that replaces the allocator for serverless indices.
 *
 * <p>S12 already measured that this algorithm distributes evenly and moves the theoretical minimum on a
 * membership change. These tests cover what a measurement cannot: that the function is <em>stable</em>,
 * because two coordinators disagreeing about where a shard lives is a split view of the cluster rather
 * than a performance problem.
 */
public class RendezvousShardPlacementTests extends OpenSearchTestCase {

    /**
     * The most important test in this class. Placement is computed independently on every node, so the
     * function's output is a wire contract in all but name. Pinning exact values means a well-meaning
     * refactor of the mixing constants fails here rather than silently repartitioning a live cluster --
     * every shard moving at once, with no error anywhere.
     *
     * <p>The expected values were produced by an independent reimplementation of the algorithm rather
     * than copied from a failing run, so they check the implementation rather than merely recording it.
     * A golden test whose values came from the code it tests only detects change, not correctness.
     */
    public void testPlacementIsStableAcrossBuilds() {
        List<String> nodes = List.of("node-a", "node-b", "node-c", "node-d", "node-e");

        assertEquals(List.of("node-d", "node-c", "node-a"), RendezvousShardPlacement.candidates(nodes, "index-uuid-1", 0));
        assertEquals(List.of("node-c", "node-a", "node-d"), RendezvousShardPlacement.candidates(nodes, "index-uuid-1", 1));
        assertEquals(List.of("node-d", "node-b", "node-e"), RendezvousShardPlacement.candidates(nodes, "index-uuid-2", 0));
    }

    /**
     * Node identity is the ID string, not the position in the list. Ordering-dependent placement would
     * make every membership event a full reshuffle, which is exactly what rendezvous exists to avoid, and
     * a caller that happens to hand over a differently-ordered list must not move any data.
     */
    public void testPlacementIgnoresNodeListOrder() {
        List<String> ordered = new ArrayList<>(List.of("node-a", "node-b", "node-c", "node-d", "node-e"));
        List<String> shuffled = new ArrayList<>(ordered);
        Collections.shuffle(shuffled, random());

        for (int shard = 0; shard < 20; shard++) {
            assertEquals(
                "shard " + shard,
                RendezvousShardPlacement.candidates(ordered, "idx", shard),
                RendezvousShardPlacement.candidates(shuffled, "idx", shard)
            );
        }
    }

    public void testCandidatesAreDistinctAndDrawnFromTheNodeList() {
        List<String> nodes = nodes(10);

        for (int shard = 0; shard < 50; shard++) {
            List<String> candidates = RendezvousShardPlacement.candidates(nodes, "idx", shard);
            assertEquals(RendezvousShardPlacement.DEFAULT_CANDIDATE_COUNT, candidates.size());
            assertEquals("candidates must be distinct", candidates.size(), new HashSet<>(candidates).size());
            assertTrue("candidates must come from the node list", nodes.containsAll(candidates));
        }
    }

    public void testPrimaryIsTheFirstCandidate() {
        List<String> nodes = nodes(8);

        for (int shard = 0; shard < 20; shard++) {
            assertEquals(
                RendezvousShardPlacement.candidates(nodes, "idx", shard).get(0),
                RendezvousShardPlacement.primaryCandidate(nodes, "idx", shard)
            );
        }
    }

    /** A cluster smaller than K should still place shards, on as many nodes as it has. */
    public void testSmallClusterReturnsWhatItCan() {
        assertEquals(1, RendezvousShardPlacement.candidates(nodes(1), "idx", 0).size());
        assertEquals(2, RendezvousShardPlacement.candidates(nodes(2), "idx", 0).size());
        assertEquals(3, RendezvousShardPlacement.candidates(nodes(3), "idx", 0).size());
        assertEquals(3, RendezvousShardPlacement.candidates(nodes(4), "idx", 0).size());
    }

    public void testEmptyClusterPlacesNothingRatherThanFailing() {
        assertEquals(List.of(), RendezvousShardPlacement.candidates(List.of(), "idx", 0));
        assertNull(RendezvousShardPlacement.primaryCandidate(List.of(), "idx", 0));
    }

    /** Different shards of one index must not collapse onto the same node, or the index has no spread. */
    public void testShardsOfOneIndexSpreadAcrossNodes() {
        List<String> nodes = nodes(10);
        Set<String> primaries = new HashSet<>();
        for (int shard = 0; shard < 30; shard++) {
            primaries.add(RendezvousShardPlacement.primaryCandidate(nodes, "one-index", shard));
        }

        assertTrue("30 shards should reach most of a 10-node cluster, reached " + primaries.size(), primaries.size() >= 7);
    }

    /** And two indices must not land identically, or every index shares one hot node. */
    public void testDifferentIndicesPlaceDifferently() {
        List<String> nodes = nodes(10);
        int identical = 0;
        for (int shard = 0; shard < 50; shard++) {
            if (RendezvousShardPlacement.primaryCandidate(nodes, "index-one", shard)
                .equals(RendezvousShardPlacement.primaryCandidate(nodes, "index-two", shard))) {
                identical++;
            }
        }
        // Roughly 1-in-10 collisions are expected with 10 nodes; a broken key would give 50.
        assertTrue("expected the two indices to diverge, got " + identical + "/50 identical", identical < 20);
    }

    /**
     * The property S12 measured, re-asserted here as a guard rather than as a measurement: adding a node
     * must move about 1/(N+1) of primaries. A hash that reshuffles everything on a membership change
     * turns each scale event into a fleet-wide cache wipe.
     */
    public void testAddingANodeMovesAboutTheTheoreticalMinimum() {
        List<String> before = nodes(20);
        List<String> after = nodes(21);

        int moved = 0;
        int total = 4_000;
        for (int shard = 0; shard < total; shard++) {
            if (RendezvousShardPlacement.primaryCandidate(before, "idx", shard)
                .equals(RendezvousShardPlacement.primaryCandidate(after, "idx", shard)) == false) {
                moved++;
            }
        }

        double fraction = (double) moved / total;
        double ideal = 1.0 / 21;
        assertTrue("moved " + fraction + ", expected near " + ideal, Math.abs(fraction - ideal) < 0.02);
    }

    /**
     * With K candidates, a single node joining can displace at most one of them, so no shard should lose
     * every warm holder. This is the structural property that makes incremental scaling free, and it is
     * worth pinning because it is the argument for scaling one node at a time.
     */
    public void testASingleJoinNeverLeavesAShardWithoutAWarmCandidate() {
        List<String> before = nodes(20);
        List<String> after = nodes(21);

        for (int shard = 0; shard < 2_000; shard++) {
            Set<String> old = new HashSet<>(RendezvousShardPlacement.candidates(before, "idx", shard));
            old.retainAll(RendezvousShardPlacement.candidates(after, "idx", shard));
            assertFalse("shard " + shard + " lost every previous candidate", old.isEmpty());
        }
    }

    public void testDistributionIsEven() {
        List<String> nodes = nodes(16);
        int[] counts = new int[16];
        int total = 32_000;
        for (int shard = 0; shard < total; shard++) {
            counts[nodes.indexOf(RendezvousShardPlacement.primaryCandidate(nodes, "idx", shard))]++;
        }

        double ideal = (double) total / nodes.size();
        for (int i = 0; i < counts.length; i++) {
            double ratio = counts[i] / ideal;
            assertTrue("node " + i + " holds " + ratio + "x its share", ratio > 0.85 && ratio < 1.15);
        }
    }

    public void testMalformedInputIsRejected() {
        expectThrows(IllegalArgumentException.class, () -> RendezvousShardPlacement.candidates(nodes(3), "idx", -1));
        expectThrows(IllegalArgumentException.class, () -> RendezvousShardPlacement.candidates(nodes(3), "idx", 0, 0));
        expectThrows(NullPointerException.class, () -> RendezvousShardPlacement.candidates(null, "idx", 0));
        expectThrows(NullPointerException.class, () -> RendezvousShardPlacement.candidates(nodes(3), null, 0));
    }

    private static List<String> nodes(int count) {
        List<String> nodes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            nodes.add("node-" + i);
        }
        return nodes;
    }
}
