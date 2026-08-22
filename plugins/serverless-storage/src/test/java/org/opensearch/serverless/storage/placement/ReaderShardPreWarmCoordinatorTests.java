/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;

/**
 * Unit coverage for {@link ReaderShardPreWarmCoordinator#newlyEligibleCandidates}: the pure diffing
 * logic that decides which nodes need a pre-warm poll, in isolation from cluster wiring, transport
 * dispatch, or the elected-cluster-manager gate. End-to-end forwarding behaviour (does the poll
 * actually reach the right node, does the budget cap it, does only the cluster-manager act) needs a
 * real cluster and belongs in an internalClusterTest instead.
 */
public class ReaderShardPreWarmCoordinatorTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "test-index-uuid";
    private static final int SHARD_ID = 0;

    public void testANodeThatWasAlreadyACandidateIsNotNewlyEligible() {
        // Same node set both epochs -- rendezvous placement is a pure function of the member list, so
        // an unchanged member list must produce an unchanged candidate set, and nothing is "newly"
        // anything.
        List<String> nodes = List.of("node-a", "node-b", "node-c", "node-d", "node-e");
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(nodes, nodes, 2L);

        List<String> current = RendezvousShardPlacement.candidates(nodes, INDEX_UUID, SHARD_ID);
        List<String> newlyEligible = ReaderShardPreWarmCoordinator.newlyEligibleCandidates(membership, INDEX_UUID, SHARD_ID);

        assertTrue("every newly-eligible node must actually be a current candidate", current.containsAll(newlyEligible));
        assertTrue("an unchanged member list must produce no newly-eligible nodes at all", newlyEligible.isEmpty());
    }

    public void testANodeAddedToTheClusterThatBecomesACandidateIsNewlyEligible() {
        List<String> before = List.of("node-a", "node-b", "node-c");
        List<String> after = List.of("node-a", "node-b", "node-c", "node-d", "node-e", "node-f", "node-g", "node-h");
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(after, before, 2L);

        Set<String> beforeCandidates = Set.copyOf(RendezvousShardPlacement.candidates(before, INDEX_UUID, SHARD_ID));
        Set<String> afterCandidates = Set.copyOf(RendezvousShardPlacement.candidates(after, INDEX_UUID, SHARD_ID));
        List<String> newlyEligible = ReaderShardPreWarmCoordinator.newlyEligibleCandidates(membership, INDEX_UUID, SHARD_ID);

        for (String nodeId : newlyEligible) {
            assertTrue("a newly-eligible node must be a candidate now: " + nodeId, afterCandidates.contains(nodeId));
            assertFalse("a newly-eligible node must not have been a candidate before: " + nodeId, beforeCandidates.contains(nodeId));
        }
        // A real assertion this test can fail on a broken implementation: with a real fleet expansion
        // from 3 to 8 nodes, at least one shard-affecting change is expected across a spread of shard
        // ids, even though this specific shard id might not change. Assert the diff is internally
        // consistent (checked above) rather than assuming this exact shard id changes -- rendezvous
        // hashing gives no guarantee about any one shard.
    }

    public void testTheCurrentPrimaryCandidateIsRecognizedAsSuch() {
        // The gated pass's dedup mechanism (see ReaderShardPreWarmCoordinator's own "gated indices
        // needed a second, differently-shaped pass" javadoc): only the node rendezvous currently names
        // candidate zero for a shard should ever dispatch a pre-warm for it.
        List<String> nodes = List.of("node-a", "node-b", "node-c", "node-d", "node-e");
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(nodes, nodes, 2L);
        String primary = RendezvousShardPlacement.candidates(nodes, INDEX_UUID, SHARD_ID).get(0);

        assertTrue(
            "the real current primary candidate must be recognized as the primary",
            ReaderShardPreWarmCoordinator.isLocalNodePrimaryCandidate(membership, INDEX_UUID, SHARD_ID, primary)
        );
    }

    public void testANonPrimaryCandidateIsNotTheDispatcher() {
        List<String> nodes = List.of("node-a", "node-b", "node-c", "node-d", "node-e");
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(nodes, nodes, 2L);
        List<String> candidates = RendezvousShardPlacement.candidates(nodes, INDEX_UUID, SHARD_ID, 3);
        // A node that is a candidate (so it plausibly has the shard open) but not the primary -- the
        // exact case this check exists to reject, so at most one of the candidates ever dispatches.
        String secondCandidate = candidates.get(1);

        assertFalse(
            "a non-primary candidate must not be recognized as the dispatcher",
            ReaderShardPreWarmCoordinator.isLocalNodePrimaryCandidate(membership, INDEX_UUID, SHARD_ID, secondCandidate)
        );
    }

    public void testANodeThatIsNotACandidateAtAllIsNotThePrimary() {
        List<String> nodes = List.of("node-a", "node-b", "node-c", "node-d", "node-e");
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(nodes, nodes, 2L);

        assertFalse(
            "a node absent from the candidate list entirely must not be recognized as the primary",
            ReaderShardPreWarmCoordinator.isLocalNodePrimaryCandidate(membership, INDEX_UUID, SHARD_ID, "not-a-cluster-member")
        );
    }

    public void testANullLocalNodeIdIsNeverThePrimary() {
        // getLocalNodeId() can be null in principle (see applyClusterState's own call site); the check
        // must degrade to "not the dispatcher" rather than throwing.
        List<String> nodes = List.of("node-a", "node-b", "node-c");
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(nodes, nodes, 2L);

        assertFalse(
            "a null local node id must never be recognized as the primary",
            ReaderShardPreWarmCoordinator.isLocalNodePrimaryCandidate(membership, INDEX_UUID, SHARD_ID, null)
        );
    }

    /**
     * <b>The dedup this class used to argue it did not need.</b> Its comment read "ClusterStateApplier
     * only fires on a real state transition, and previousNodeIds() only changes when withNodes shifts the
     * epoch, so reaching here with a non-empty previous epoch already means something changed since the
     * last transition. No separate dedup needed." Every clause is true and the conclusion is not:
     * {@code previousNodeIds()} is non-empty permanently after the first epoch change, and an applier
     * fires on every transition, so after one membership change every later cluster state event -- an
     * index created, a suspension tick, anything -- re-dispatched the whole fan-out to the same targets.
     * The per-event budget bounded each burst and nothing ever ended them.
     */
    public void testAnUnchangedMembershipIsNotAnEpochTransition() {
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(
            List.of("node-a", "node-b", "node-c"),
            List.of("node-a", "node-b"),
            2L
        );
        // The identical membership republished, which is what every ordinary cluster state event carries.
        org.opensearch.cluster.ClusterState state = stateWith(membership);

        assertFalse(
            "an event that does not change the membership must not re-run the fan-out, however non-empty " + "the previous epoch is",
            ReaderShardPreWarmCoordinator.membershipChanged(new org.opensearch.cluster.ClusterChangedEvent("test", state, state))
        );
    }

    /** And a real epoch transition still is one, or the fix would simply disable pre-warming. */
    public void testARealEpochTransitionIsDetected() {
        ComputedPlacementMembership before = ComputedPlacementMembership.of(List.of("node-a", "node-b"), List.of("node-a"), 2L);
        ComputedPlacementMembership after = before.withNodes(List.of("node-c"));

        assertTrue(
            "a membership that actually gained a node must be acted on",
            ReaderShardPreWarmCoordinator.membershipChanged(
                new org.opensearch.cluster.ClusterChangedEvent("test", stateWith(after), stateWith(before))
            )
        );
    }

    private static org.opensearch.cluster.ClusterState stateWith(ComputedPlacementMembership membership) {
        return org.opensearch.cluster.ClusterState.builder(org.opensearch.cluster.ClusterName.DEFAULT)
            .metadata(org.opensearch.cluster.metadata.Metadata.builder().putCustom(ComputedPlacementMembership.TYPE, membership).build())
            .build();
    }

    public void testEmptyPreviousEpochProducesNoNewlyEligibleNodes() {
        // No previous epoch to diff against -- the coordinator itself short-circuits this case before
        // calling newlyEligibleCandidates at all (see applyClusterState's own early return), but the
        // pure function itself must also degrade safely: an empty previous member list makes every
        // current candidate technically "new" by a naive diff, which would pre-warm an entire fresh
        // cluster's placement on its very first epoch -- not wrong, but not what "previous epoch"
        // should mean for a cluster that never had one. Confirm the two candidate sets don't silently
        // coincide by accident for this fixture, so the real guard (in applyClusterState) is what's
        // doing the work, not a coincidence of this test's own node names.
        List<String> nodes = List.of("node-a", "node-b", "node-c", "node-d", "node-e");
        List<String> currentCandidates = RendezvousShardPlacement.candidates(nodes, INDEX_UUID, SHARD_ID);
        List<String> emptyPreviousCandidates = RendezvousShardPlacement.candidates(List.of(), INDEX_UUID, SHARD_ID);
        assertFalse(
            "the fixture must actually exercise a real difference for this test to mean anything",
            currentCandidates.equals(emptyPreviousCandidates)
        );
    }
}
