/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * C2a. The node set placement is computed against, as a value rather than an observation.
 *
 * <p>What these assert is not obvious from the class alone. The property that matters is that a node
 * leaving does not change the membership, because a shard following a departing node lands somewhere
 * with none of its data and recovers blank. Everything else here exists to make that property safe to
 * depend on: identical membership from any input order, a version that moves only on real change, and
 * a round trip that survives publication.
 */
public class ComputedPlacementMembershipTests extends OpenSearchTestCase {

    /** Order of the input must not survive into the value, or two nodes disagree about placement. */
    public void testMembershipIsIndependentOfInputOrder() {
        ComputedPlacementMembership one = ComputedPlacementMembership.of(List.of("node-c", "node-a", "node-b"), 1L);
        ComputedPlacementMembership other = ComputedPlacementMembership.of(List.of("node-b", "node-c", "node-a"), 1L);

        assertEquals(one, other);
        assertEquals(List.of("node-a", "node-b", "node-c"), one.nodeIds());
    }

    public void testDuplicatesCollapse() {
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(List.of("node-a", "node-a", "node-b"), 1L);

        assertEquals(List.of("node-a", "node-b"), membership.nodeIds());
    }

    /**
     * The load-bearing one. Adding a node that is already a member must not advance the version, or
     * every cluster state update would look like a placement change and every node would re-derive.
     */
    public void testAddingAKnownNodeChangesNothing() {
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(List.of("node-a", "node-b"), 7L);

        ComputedPlacementMembership same = membership.withNodes(List.of("node-b", "node-a"));

        assertSame("a no-op must return the same instance so callers can detect it cheaply", membership, same);
        assertEquals(7L, same.version());
    }

    public void testAddingANewNodeAdvancesTheVersion() {
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(List.of("node-a"), 7L);

        ComputedPlacementMembership grown = membership.withNodes(List.of("node-b"));

        assertEquals(List.of("node-a", "node-b"), grown.nodeIds());
        assertEquals(8L, grown.version());
        assertEquals("the original must be untouched", List.of("node-a"), membership.nodeIds());
    }

    /**
     * The property C13 needs, stated directly: adding nodes cannot remove one, so a node going away
     * cannot move a shard by itself.
     *
     * <p>This javadoc used to say that a removal API should force the question of what happens to the
     * data first. T16 added one, so here is the answer. {@code withNodes} still cannot shrink the
     * membership, which is what protects a restarting node. Removal is a separate call a caller has to
     * mean, because only the caller can tell a restart from a departure, and under autoscaling with
     * scale-to-zero those stopped being the same condition.
     */
    public void testAddingNodesCanNeverShrinkTheMembership() {
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(List.of("node-a", "node-b"), 1L);

        ComputedPlacementMembership afterNodeBLeaves = membership.withNodes(List.of("node-a"));

        assertSame(membership, afterNodeBLeaves);
        assertTrue("a departed node must remain a member, or its shards move away from their data", afterNodeBLeaves.contains("node-b"));
    }

    /** A node genuinely gone can be removed, which is what autoscaling with scale-to-zero needs. */
    public void testRemovingADepartedNodeAdvancesTheVersion() {
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(List.of("node-a", "node-b"), 1L);

        ComputedPlacementMembership after = membership.withoutNodes(List.of("node-b"));

        assertEquals(List.of("node-a"), after.nodeIds());
        assertEquals(2L, after.version());
        assertFalse(after.contains("node-b"));
    }

    /** Removing a node that is not a member changes nothing, so placement does not re-derive for free. */
    public void testRemovingAStrangerIsANoOp() {
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(List.of("node-a"), 1L);

        assertSame(membership, membership.withoutNodes(List.of("node-z")));
    }

    /**
     * The point of an epoch over a version number: the list it replaced is retained, so warmth is
     * computable rather than stored. Without this, "which node held shard X warm" is O(shards) of written
     * down state, which is the structure this whole design exists to delete.
     */
    public void testAnEpochRemembersTheListItReplaced() {
        ComputedPlacementMembership first = ComputedPlacementMembership.of(List.of("node-a"), 1L);
        assertEquals("a first epoch has no predecessor, and that is an answer", List.of(), first.previousNodeIds());

        ComputedPlacementMembership second = first.withNodes(List.of("node-b"));
        assertEquals(List.of("node-a", "node-b"), second.nodeIds());
        assertEquals(List.of("node-a"), second.previousNodeIds());

        ComputedPlacementMembership third = second.withoutNodes(List.of("node-a"));
        assertEquals(List.of("node-b"), third.nodeIds());
        assertEquals(
            "removal retains its predecessor too, or a shrink loses the warm set",
            List.of("node-a", "node-b"),
            third.previousNodeIds()
        );
    }

    /**
     * Two memberships differing only in what they replaced are different, because warmth derives from it.
     * Equal would let a cluster state update carrying a corrected predecessor be dropped as a no-op.
     */
    public void testThePredecessorParticipatesInEquality() {
        ComputedPlacementMembership withoutHistory = ComputedPlacementMembership.of(List.of("node-a"), 2L);
        ComputedPlacementMembership withHistory = ComputedPlacementMembership.of(List.of("node-a"), List.of("node-b"), 2L);

        assertNotEquals(withoutHistory, withHistory);
    }

    public void testRoundTrip() throws IOException {
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(List.of("node-b", "node-a"), List.of("node-c"), 42L);

        ComputedPlacementMembership read;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            membership.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                read = new ComputedPlacementMembership(in);
            }
        }

        assertEquals(membership, read);
        assertEquals(42L, read.version());
        assertEquals(
            "the predecessor must survive the wire, or every node recomputes warmth as cold",
            List.of("node-c"),
            read.previousNodeIds()
        );
    }

    public void testEmptyIsEmpty() {
        assertTrue(ComputedPlacementMembership.EMPTY.isEmpty());
        assertEquals(0L, ComputedPlacementMembership.EMPTY.version());
        assertEquals(List.of("node-a"), ComputedPlacementMembership.EMPTY.withNodes(List.of("node-a")).nodeIds());
    }
}
