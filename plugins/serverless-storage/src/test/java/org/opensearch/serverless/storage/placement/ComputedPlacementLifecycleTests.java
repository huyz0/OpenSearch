/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

/**
 * One node closing must not disarm the nodes still running beside it.
 *
 * <h2>The defect</h2>
 *
 * Every registry these gates touch is static. A JVM hosts one node in production, so that was invisible
 * there; {@code InternalTestCluster} hosts several, and the first one to close called {@code uninstall} and
 * unregistered computed placement and descriptor resolution for all of them.
 *
 * <p>It presented as a suite that failed nondeterministically -- a different test each run, green on a rerun
 * of identical code, and green again when the failing class was run alone. It was written off as test
 * flakiness more than once in this work, including by the residency and eviction changes that had to be
 * debugged around it.
 *
 * <p>It is not only test flakiness. {@code IndicesClusterStateService.heldOnDemand} asks whether a descriptor
 * supplier is registered to decide whether this node is holding an index on demand, and a gated index whose
 * computed placement has been unregistered has no routing table at all. An unbalanced uninstall can
 * therefore make a live node stop recognising indices it is currently serving.
 *
 * <h2>Why a counter rather than per-node instances</h2>
 *
 * Per-node instances would be the better design and a much larger change: the seams are static because core
 * consults them from places that have no node context. Counting claims fixes the lifecycle without touching
 * the shape, and it is exactly right for the production case of one node, which increments to one and
 * decrements to zero.
 */
public class ComputedPlacementLifecycleTests extends OpenSearchTestCase {

    @After
    public void clearRegistries() {
        ComputedPlacementGate.uninstall();
    }

    public void testTheLastNodeToCloseIsTheOneThatClears() {
        ComputedPlacementGate.install(true);
        ComputedPlacementGate.install(true);
        ComputedPlacementGate.install(true);

        ComputedPlacementGate.uninstallOneNode();
        assertTrue(
            "two of three nodes are still running, so placement must still answer; unregistering here is "
                + "what left a live node's gated indices with no routing table",
            AbsentIndexRoutingSuppliers.isRegistered()
        );

        ComputedPlacementGate.uninstallOneNode();
        assertTrue("one node is still running", AbsentIndexRoutingSuppliers.isRegistered());

        ComputedPlacementGate.uninstallOneNode();
        assertFalse("the last node has gone, so nothing should be left registered", AbsentIndexRoutingSuppliers.isRegistered());
    }

    /**
     * A disabled install claims nothing, so a disabled node closing cannot decrement someone else's claim.
     *
     * <p>{@code install(false)} returns before registering anything. If it still counted, a cluster mixing
     * enabled and disabled nodes would have the disabled ones propping the count up, and the registries
     * would outlive every node that actually wanted them.
     */
    public void testADisabledNodeHoldsNoClaim() {
        ComputedPlacementGate.install(true);
        ComputedPlacementGate.install(false);

        ComputedPlacementGate.uninstallOneNode();

        assertFalse(
            "only one node ever installed, so one release must clear it; a disabled install that counted "
                + "would leave the suppliers registered with no node behind them",
            AbsentIndexRoutingSuppliers.isRegistered()
        );
    }

    /**
     * The unconditional reset really is unconditional, and leaves no count behind.
     *
     * <p>This is what a test's teardown calls between methods. If it decremented instead, a suite that
     * installed three nodes and reset once would leave a count of two, and the next method's install would
     * look like a fourth node that nothing ever releases.
     */
    public void testTheUnconditionalResetClearsEveryClaim() {
        ComputedPlacementGate.install(true);
        ComputedPlacementGate.install(true);

        ComputedPlacementGate.uninstall();
        assertFalse("a reset must clear the registries whatever the count was", AbsentIndexRoutingSuppliers.isRegistered());

        ComputedPlacementGate.install(true);
        ComputedPlacementGate.uninstallOneNode();
        assertFalse(
            "and must leave no residue, so one install still takes one release to clear",
            AbsentIndexRoutingSuppliers.isRegistered()
        );
    }

    /** Releasing more often than installing must not go negative and strand a later install. */
    public void testReleasingMoreOftenThanInstallingIsHarmless() {
        ComputedPlacementGate.uninstallOneNode();
        ComputedPlacementGate.uninstallOneNode();

        ComputedPlacementGate.install(true);
        assertTrue("an install after surplus releases must still register", AbsentIndexRoutingSuppliers.isRegistered());

        ComputedPlacementGate.uninstallOneNode();
        assertFalse(
            "and must still take exactly one release to clear; a count driven negative would need three",
            AbsentIndexRoutingSuppliers.isRegistered()
        );
    }
}
