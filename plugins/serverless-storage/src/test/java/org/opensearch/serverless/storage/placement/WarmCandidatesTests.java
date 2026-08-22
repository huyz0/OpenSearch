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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class WarmCandidatesTests extends OpenSearchTestCase {

    private static List<String> nodes(int count) {
        List<String> nodeIds = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            nodeIds.add("node-" + i);
        }
        return nodeIds;
    }

    public void testNoPreviousEpochMeansNoWarmClaim() {
        ComputedPlacementMembership first = ComputedPlacementMembership.of(nodes(5), 1L);
        assertEquals(
            "a cluster that never changed has its shards where placement says, not nowhere",
            List.of(),
            WarmCandidates.forShard(first, "uuid", 0, 3)
        );
    }

    /**
     * The property the whole approach rests on: where a shard used to be placed is where its cache is,
     * because deterministic placement is what put it there.
     */
    public void testWarmCandidatesAreWherePlacementPutTheShardLastEpoch() {
        List<String> before = nodes(5);
        List<String> after = new ArrayList<>(before);
        after.add("node-5");
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(after, before, 2L);

        List<String> expected = RendezvousShardPlacement.candidates(before, "uuid", 7, 3);
        assertEquals(expected, WarmCandidates.forShard(membership, "uuid", 7, 3));
    }

    /**
     * A departed node's cache is not reachable, so offering it would send traffic somewhere that cannot
     * answer. That is the failure a stale stored affinity record produces, and the reason the stored
     * version needed a freshness check that this does not.
     */
    public void testDepartedNodesAreNotOfferedAsWarm() {
        List<String> before = nodes(5);
        List<String> after = List.of("node-0", "node-1");
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(after, before, 2L);

        for (String nodeId : WarmCandidates.forShard(membership, "uuid", 3, 3)) {
            assertTrue("a warm candidate must still be a member, got " + nodeId, after.contains(nodeId));
        }
    }

    /**
     * Ordering rather than filtering. A warm-only list empties out exactly when the fleet has just
     * doubled, which S12 measured as the case where 12.4% of shards have no warm candidate, and that is
     * when a router most needs somewhere to send a request.
     */
    public void testPreferWarmNeverNarrowsTheChoice() {
        List<String> before = nodes(4);
        List<String> after = nodes(8);
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(after, before, 2L);

        for (int shardId = 0; shardId < 200; shardId++) {
            List<String> placement = RendezvousShardPlacement.candidates(after, "uuid", shardId, 3);
            List<String> preferred = WarmCandidates.preferWarm(membership, "uuid", shardId, 3);

            assertTrue("every placement candidate must survive ordering", preferred.containsAll(placement));
            assertEquals("and ordering must not invent or duplicate one", new HashSet<>(preferred), new HashSet<>(placement));
        }
    }

    /** A warm node that is still a placement candidate is offered first, which is the whole point. */
    public void testAWarmPlacementCandidateComesFirst() {
        List<String> before = nodes(6);
        List<String> after = new ArrayList<>(before);
        after.add("node-6");
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(after, before, 2L);

        int checked = 0;
        for (int shardId = 0; shardId < 200 && checked < 20; shardId++) {
            List<String> placement = RendezvousShardPlacement.candidates(after, "uuid", shardId, 3);
            Set<String> warm = new HashSet<>(WarmCandidates.forShard(membership, "uuid", shardId, 3));
            warm.retainAll(placement);
            if (warm.isEmpty()) {
                continue;
            }
            checked++;
            assertTrue(
                "a node that is both warm and placeable must lead",
                warm.contains(WarmCandidates.preferWarm(membership, "uuid", shardId, 3).get(0))
            );
        }
        assertTrue("the fixture must actually produce overlapping warm candidates", checked > 0);
    }

    /**
     * S12's structural result, restated as a property of this helper: one node joining leaves no shard
     * without a warm candidate, because a single new node can displace at most one of K.
     */
    public void testASingleJoinLeavesNoShardCold() {
        List<String> before = nodes(20);
        List<String> after = new ArrayList<>(before);
        after.add("node-20");
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(after, before, 2L);

        for (int shardId = 0; shardId < 2_000; shardId++) {
            List<String> placement = RendezvousShardPlacement.candidates(after, "uuid", shardId, 3);
            Set<String> warm = new HashSet<>(WarmCandidates.forShard(membership, "uuid", shardId, 3));
            warm.retainAll(placement);
            assertFalse("shard " + shardId + " lost every warm candidate to a single join", warm.isEmpty());
        }
    }
}
