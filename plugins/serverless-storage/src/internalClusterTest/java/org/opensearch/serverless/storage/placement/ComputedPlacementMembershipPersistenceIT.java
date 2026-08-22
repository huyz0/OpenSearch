/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Whether the published membership survives a full-cluster restart, which is the one thing {@link
 * ComputedPlacementMembership} exists to guarantee and the one thing nothing checked.
 *
 * <p>{@link ComputedPlacementMembership#context()} declares {@code API_AND_GATEWAY} and its javadoc calls
 * gateway persistence "the requirement rather than a nicety". It was neither, because only half of it was
 * wired: the plugin registered the custom's {@code NamedWriteable}s, which carry it between live nodes,
 * and no {@code NamedXContent} parser, which is what gateway persistence reads it back with. {@code
 * Metadata.Builder.fromXContent} does not fail on a custom it cannot parse -- it logs "Skipping unknown
 * custom object with type" and continues, so that a node can still start when a plugin is gone. The
 * result was silent: every full-cluster restart began with an empty membership, {@code
 * ComputedRoutingTable.eligibleNodes} fell through to the live {@code DiscoveryNodes} for the whole join
 * window, and placement was computed against a node list that changed as nodes came back -- the exact
 * time-varying input C13 showed moves a shard onto a node holding none of its data. The rebuilt
 * membership then restarted at version 1 with no predecessor, so neither staleness detection nor warmth
 * had anything to work with either.
 *
 * <p><b>The extra node before the restart is what makes this test able to fail.</b> A membership that was
 * lost and rebuilt looks like version 1 with an empty predecessor, and a small cluster's first membership
 * can legitimately be exactly that -- so the assertions would pass against the broken code. Starting one
 * more data node first forces a real epoch transition: a version above 1 and a non-empty predecessor,
 * neither of which a rebuilt-from-nothing membership can have.
 *
 * <p>In its own class because a full restart leaves the cluster in a state that other tests in the same
 * suite cannot be assumed to recover from -- the same reasoning {@link ComputedPlacementRestartIT}
 * records for itself.
 */
public class ComputedPlacementMembershipPersistenceIT extends OpenSearchIntegTestCase {

    /**
     * Only the membership machinery. The real plugin's gate and resolver would collide with the supplier
     * this test registers; see {@link MembershipOnlyTestPlugin}'s own javadoc.
     */
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MembershipOnlyTestPlugin.class);
    }

    /**
     * A supplier that declines everything. Nothing here needs a placement answer -- the maintainer simply
     * stays inert until <em>some</em> supplier is installed, which is the condition being satisfied.
     */
    @Before
    public void registerComputedPlacement() {
        AbsentIndexRoutingSuppliers.register((state, metadata) -> null);
    }

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.register(null);
    }

    public void testMembershipSurvivesAFullClusterRestart() throws Exception {
        awaitPublishedMembership();

        // Force a real epoch: a new member takes the version past 1 and leaves a predecessor behind.
        String joinedNodeName = internalCluster().startDataOnlyNode();
        ensureStableCluster(internalCluster().size());
        assertBusy(() -> {
            ComputedPlacementMembership membership = membership();
            assertTrue("the joined node must become a member: " + membership, membership.nodeIds().size() >= 2);
            assertTrue("the join must advance the epoch, or this test cannot fail: " + membership, membership.version() > 1);
            assertFalse("and it must leave a predecessor behind: " + membership, membership.previousNodeIds().isEmpty());
        }, 30, TimeUnit.SECONDS);
        ComputedPlacementMembership before = membership();
        assertNotNull(joinedNodeName);

        internalCluster().fullRestart();
        ensureStableCluster(internalCluster().size());

        assertBusy(() -> {
            ComputedPlacementMembership after = membership();
            assertFalse(
                "a restart must not start from an empty membership: placement would compute against the "
                    + "live node list for the whole join window",
                after.isEmpty()
            );
            assertEquals("every member must come back, since node ids are persisted in the data path", before.nodeIds(), after.nodeIds());
            assertTrue(
                "the version must not restart at 1, or every staleness check that compares one is defeated: " + before + " became " + after,
                after.version() >= before.version()
            );
            assertFalse(
                "the previous epoch must survive too, or a restart makes every shard in the cluster look "
                    + "cold to warmth-aware placement: "
                    + after,
                after.previousNodeIds().isEmpty()
            );
        }, 60, TimeUnit.SECONDS);
    }

    private static ComputedPlacementMembership membership() {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        return ComputedPlacementMembershipService.get(state);
    }

    private void awaitPublishedMembership() throws Exception {
        assertBusy(
            () -> assertFalse("membership must be published before anything can persist it", membership().isEmpty()),
            30,
            TimeUnit.SECONDS
        );
    }
}
