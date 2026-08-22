/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * C25. The maintainer that keeps the published membership up to date.
 *
 * <p>The value type has had tests since C2a; the service that writes it has had none. Everything it does
 * was covered only indirectly, by one integration test that would still pass if most of these rules broke
 * in ways that only show up later: publishing on registration, staying inert until a supplier is
 * installed, only the elected manager writing, and the listener removing itself when its node is gone.
 *
 * <p>That last one is the reason this suite exists rather than being nice to have. The registry is
 * static and outlives any node, so a listener that never leaves keeps its {@code ClusterService} alive
 * and wakes up on every later registration. It fires only on shutdown paths, which is exactly where a
 * leak goes unnoticed, and no integration test looks for it.
 */
public class ComputedPlacementMembershipServiceTests extends OpenSearchTestCase {

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.register(null);
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
    }

    /**
     * The C2 gap, as a test. Registration alone has to publish, or a cluster that enables placement and
     * then goes idle computes against the live node list until something unrelated happens.
     */
    public void testRegisteringASupplierPublishesMembership() {
        AtomicInteger updates = new AtomicInteger();
        ClusterService clusterService = clusterService(stateWithNodes(true), Lifecycle.State.STARTED, updates);
        new ComputedPlacementMembershipService(clusterService);

        AbsentIndexRoutingSuppliers.register((state, metadata) -> null);

        assertEquals("installing a supplier must publish membership without waiting for other activity", 1, updates.get());
    }

    /** Only the elected manager writes cluster state, so everyone else must stay quiet. */
    public void testANodeThatIsNotElectedPublishesNothing() {
        AtomicInteger updates = new AtomicInteger();
        ClusterService clusterService = clusterService(stateWithNodes(false), Lifecycle.State.STARTED, updates);
        new ComputedPlacementMembershipService(clusterService);

        AbsentIndexRoutingSuppliers.register((state, metadata) -> null);

        assertEquals("a node that is not the elected cluster manager must not submit an update", 0, updates.get());
    }

    /**
     * The self-removal, which is the untested one and the one that leaks.
     *
     * <p><b>Asserted on whether the listener runs, not on what it does.</b> The first version of this
     * test counted cluster state updates and passed against a mutation that deleted the removal
     * entirely, because a stopped node's listener checks lifecycle first and returns without submitting
     * anything either way. Counting the work a listener declines to do cannot detect whether it is still
     * there. Counting the calls it makes can: after a second registration, a removed listener has not
     * asked for its lifecycle state again.
     */
    public void testAStoppedNodeRemovesItselfFromTheRegistry() {
        ClusterService stopped = clusterService(stateWithNodes(true), Lifecycle.State.CLOSED, new AtomicInteger());
        new ComputedPlacementMembershipService(stopped);

        AbsentIndexRoutingSuppliers.register((state, metadata) -> null);
        verify(stopped, times(1)).lifecycleState();

        AbsentIndexRoutingSuppliers.register((state, metadata) -> null);

        verify(stopped, times(1)).lifecycleState();
    }

    /** An unconfigured cluster must never acquire this metadata, so turning the feature off leaves no trace. */
    public void testWithoutASupplierClusterChangesPublishNothing() {
        AtomicInteger updates = new AtomicInteger();
        ClusterState state = stateWithNodes(true);
        ClusterService clusterService = clusterService(state, Lifecycle.State.STARTED, updates);
        ComputedPlacementMembershipService service = new ComputedPlacementMembershipService(clusterService);

        service.clusterChanged(new ClusterChangedEvent("test", state, state));

        assertEquals("an unconfigured cluster must not acquire membership metadata", 0, updates.get());
    }

    /** With a supplier installed, an ordinary cluster state change maintains the membership. */
    public void testAClusterChangePublishesWhenASupplierIsInstalled() {
        AtomicInteger updates = new AtomicInteger();
        ClusterState state = stateWithNodes(true);
        ClusterService clusterService = clusterService(state, Lifecycle.State.STARTED, updates);
        ComputedPlacementMembershipService service = new ComputedPlacementMembershipService(clusterService);
        AbsentIndexRoutingSuppliers.register((s, metadata) -> null);
        updates.set(0); // discard the publish that registration itself triggers

        service.clusterChanged(new ClusterChangedEvent("test", state, state));

        assertEquals("a cluster change with a supplier installed must maintain the membership", 1, updates.get());
    }

    /**
     * The no-op case. Once every live data node is a member there is nothing to add, and submitting
     * anyway would advance the version on every cluster state update and make every node re-derive
     * placement for no reason.
     */
    public void testAMembershipThatAlreadyCoversEveryNodeSubmitsNothing() {
        AtomicInteger updates = new AtomicInteger();
        ClusterState state = stateWithPublishedMembership();
        ClusterService clusterService = clusterService(state, Lifecycle.State.STARTED, updates);
        ComputedPlacementMembershipService service = new ComputedPlacementMembershipService(clusterService);
        AbsentIndexRoutingSuppliers.register((s, metadata) -> null);
        updates.set(0);

        service.clusterChanged(new ClusterChangedEvent("test", state, state));

        assertEquals("a membership that already contains every node must not be rewritten", 0, updates.get());
    }

    /**
     * <b>The incident this gate exists for.</b> Decommission used to be counted purely in applied cluster
     * states, and a node leaving is itself the thing that produces a burst of them: node-left, the shard
     * failures behind it, the reroutes those trigger, plus whatever this plugin publishes on top. Ten
     * observations therefore elapsed inside an ordinary restart, and the node came back to find its shards
     * re-placed onto nodes holding none of its data -- which recover empty while looking healthy. So the
     * observations may all be spent and the member must still survive until real time has passed.
     */
    public void testABurstOfEventsInsideARestartDoesNotDecommission() {
        AtomicInteger updates = new AtomicInteger();
        ClusterState state = stateWithMembershipFor(List.of("node-1", "node-2", "restarting"));
        ClusterService clusterService = clusterService(state, Lifecycle.State.STARTED, updates);
        java.util.concurrent.atomic.AtomicLong now = new java.util.concurrent.atomic.AtomicLong(0L);
        ComputedPlacementMembershipService service = new ComputedPlacementMembershipService(
            clusterService,
            java.util.concurrent.TimeUnit.MINUTES.toMillis(15),
            now::get
        );
        AbsentIndexRoutingSuppliers.register((s, metadata) -> null);

        // Thirty cluster state events, all inside the first twenty seconds -- an entirely ordinary restart.
        List<String> decommissioned = List.of();
        for (int event = 0; event < 30; event++) {
            now.addAndGet(700L);
            service.clusterChanged(new ClusterChangedEvent("test", state, state));
            decommissioned = service.decommissionableForTesting(state);
        }

        assertEquals(
            "an absent member must not be decommissioned by cluster-state churn alone: the departure "
                + "itself is what generates the churn",
            List.of(),
            decommissioned
        );
    }

    /** And once the node really has been gone for the configured time, it is decommissioned. */
    public void testASustainedAbsenceInRealTimeDecommissions() {
        AtomicInteger updates = new AtomicInteger();
        ClusterState state = stateWithMembershipFor(List.of("node-1", "node-2", "departed"));
        ClusterService clusterService = clusterService(state, Lifecycle.State.STARTED, updates);
        java.util.concurrent.atomic.AtomicLong now = new java.util.concurrent.atomic.AtomicLong(0L);
        ComputedPlacementMembershipService service = new ComputedPlacementMembershipService(
            clusterService,
            java.util.concurrent.TimeUnit.MINUTES.toMillis(15),
            now::get
        );
        AbsentIndexRoutingSuppliers.register((s, metadata) -> null);

        List<String> decommissioned = List.of();
        for (int event = 0; event < 30; event++) {
            now.addAndGet(java.util.concurrent.TimeUnit.MINUTES.toMillis(1));
            service.clusterChanged(new ClusterChangedEvent("test", state, state));
            decommissioned = service.decommissionableForTesting(state);
        }

        assertEquals(
            "a member absent across both a run of observations and a real stretch of time is gone",
            List.of("departed"),
            decommissioned
        );
    }

    /**
     * <b>The second half of the same incident.</b> The decommission list is captured when the task is
     * submitted and applied when the task runs, and a queued task can wait behind exactly the burst of work
     * a returning node produces. Applied unconditionally, it removed a node that was present again in the
     * very state being transformed and then re-added it from the live node list -- bouncing every shard it
     * owned through two epochs for nothing.
     */
    public void testANodeThatCameBackWhileTheTaskWasQueuedIsNotRemoved() throws Exception {
        AtomicInteger updates = new AtomicInteger();
        java.util.List<ClusterStateUpdateTask> submitted = new java.util.ArrayList<>();
        ClusterState absentState = stateWithMembershipFor(List.of("node-1", "node-2", "restarting"));
        ClusterService clusterService = clusterService(absentState, Lifecycle.State.STARTED, updates, submitted);
        java.util.concurrent.atomic.AtomicLong now = new java.util.concurrent.atomic.AtomicLong(0L);
        ComputedPlacementMembershipService service = new ComputedPlacementMembershipService(clusterService, 0L, now::get);
        AbsentIndexRoutingSuppliers.register((s, metadata) -> null);
        submitted.clear();

        for (int event = 0; event < ComputedPlacementMembershipService.ABSENT_OBSERVATIONS_BEFORE_DECOMMISSION; event++) {
            service.clusterChanged(new ClusterChangedEvent("test", absentState, absentState));
        }
        assertFalse("the premise: a decommission was submitted", submitted.isEmpty());

        // The node rejoins before the queued task actually executes, which is what a restart finishing
        // looks like from here.
        List<String> allThree = List.of("node-1", "node-2", "restarting");
        ClusterState rejoined = stateWithMembershipFor(allThree, allThree);
        ClusterState result = submitted.get(submitted.size() - 1).execute(rejoined);

        assertTrue(
            "a member present in the state being updated must keep its membership, not be removed and " + "immediately re-added",
            ComputedPlacementMembershipService.get(result).contains("restarting")
        );
        assertSame("and with nothing left to change, the state must be returned untouched", rejoined, result);
    }

    // ---------------------------------------------------------------- helpers

    /** A state whose live nodes are node-1 and node-2, with a membership naming whatever is passed. */
    private static ClusterState stateWithMembershipFor(List<String> members) {
        return stateWithMembershipFor(members, List.of("node-1", "node-2"));
    }

    private static ClusterState stateWithMembershipFor(List<String> members, List<String> liveNodeIds) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (String nodeId : liveNodeIds) {
            nodes.add(dataNode(nodeId));
        }
        nodes.localNodeId(liveNodeIds.get(0)).clusterManagerNodeId(liveNodeIds.get(0));
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).nodes(nodes.build()).build();
        return ClusterState.builder(state)
            .metadata(
                Metadata.builder(state.metadata()).putCustom(ComputedPlacementMembership.TYPE, ComputedPlacementMembership.of(members, 1L))
            )
            .build();
    }

    private static ClusterService clusterService(ClusterState state, Lifecycle.State lifecycle, AtomicInteger updates) {
        return clusterService(state, lifecycle, updates, null);
    }

    /**
     * @param submitted when non-null, receives each submitted update task so a test can run it against a
     *                  state of its own choosing -- which is how the "the node came back while the task was
     *                  queued" case is reachable at all.
     */
    private static ClusterService clusterService(
        ClusterState state,
        Lifecycle.State lifecycle,
        AtomicInteger updates,
        java.util.List<ClusterStateUpdateTask> submitted
    ) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        when(clusterService.lifecycleState()).thenReturn(lifecycle);
        // The service reads its decommission delay from node settings, so a mock has to have some.
        when(clusterService.getSettings()).thenReturn(org.opensearch.common.settings.Settings.EMPTY);
        doAnswer(invocation -> {
            updates.incrementAndGet();
            if (submitted != null) {
                submitted.add(invocation.getArgument(1));
            }
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        return clusterService;
    }

    private static ClusterState stateWithNodes(boolean localNodeIsElected) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder().add(dataNode("node-1")).add(dataNode("node-2")).localNodeId("node-1");
        if (localNodeIsElected) {
            nodes.clusterManagerNodeId("node-1");
        }
        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes.build()).build();
    }

    private static ClusterState stateWithPublishedMembership() {
        ClusterState state = stateWithNodes(true);
        return ClusterState.builder(state)
            .metadata(
                Metadata.builder(state.metadata())
                    .putCustom(ComputedPlacementMembership.TYPE, ComputedPlacementMembership.of(List.of("node-1", "node-2"), 1L))
            )
            .build();
    }

    private static DiscoveryNode dataNode(String id) {
        return new DiscoveryNode(
            id,
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300 + Math.abs(id.hashCode() % 1000)),
            Map.of(),
            Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
    }
}
