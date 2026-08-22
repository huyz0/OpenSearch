/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.Lifecycle;

import java.util.ArrayList;
import java.util.List;

/**
 * Keeps the published placement membership up to date, by adding nodes and never removing them.
 *
 * <p>The asymmetry is the design. Adding a node has to happen automatically, because a cluster that
 * grows should start using the new capacity without an operator ceremony. Removing one must not happen
 * automatically, because a node is absent for two very different reasons that look identical from here:
 * it is restarting and will be back with its data, or it is gone for good. Guessing wrong in the second
 * direction is harmless and self-correcting; guessing wrong in the first direction moves every shard
 * that node owned to a node with none of its data, which then recovers empty while looking healthy.
 * That is the failure C13 hit, and it is silent.
 *
 * <p>So a departed node stays a member until something deliberately decommissions it, and decommission
 * is not implemented. Requests to an absent member fail and retry, which is the correct behaviour while
 * a node is restarting and an acceptable one while a node is permanently gone, since the alternative is
 * data loss rather than unavailability.
 *
 * <p>Runs only on the elected cluster manager, because it writes cluster state. Every other node reads
 * the result.
 *
 * <p><b>Deliberately still built on {@link AbsentIndexRoutingSuppliers} directly, out of scope for Phase
 * C4b of {@code core-pluggability-refactor-plan.md}</b> (its own {@code addRegistrationListener}/{@code
 * removeRegistrationListener}/{@code isRegistered} calls, in the constructor and {@link #publishIfElected}/
 * {@link #clusterChanged}). This service's whole reason to exist is reacting to a supplier's registration
 * <em>changing</em> during a node's lifetime -- publishing immediately once placement is enabled rather than
 * waiting for the next unrelated cluster-state change, and self-unregistering on node shutdown. The new SPI
 * ({@code ClusterPlugin#getIndexRoutingResolver()}) has no equivalent concept: a resolver is attached once,
 * unconditionally, at node startup (confirmed against the real registration:
 * {@code ServerlessStoragePlugin#getIndexRoutingResolver()} always returns a resolver, regardless of
 * whether the underlying feature is on), and never re-attached or detached afterward. Migrating this
 * service would mean either inventing a registration-change-notification concept the SPI doesn't have (a
 * real design question of its own, not attempted here) or dropping the "publish immediately on enable"
 * optimization and relying solely on {@link #clusterChanged}'s own organic triggering -- a real, if narrow,
 * behavior change deliberately not made without explicit sign-off. See {@code IndexCreationStrategyRegistry}
 * /{@code IndexRoutingResolver}'s own {@code isRegistered()}-shaped call sites for the closely related
 * finding (Phase C4b's second slice) that "a resolver is attached" and "the underlying feature is
 * currently active" are not the same question -- this service needs a third thing neither answers: notice
 * of the moment the feature turns on.
 */
public class ComputedPlacementMembershipService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(ComputedPlacementMembershipService.class);

    private final ClusterService clusterService;

    /**
     * Held as a field rather than passed as a method reference twice, because adding and removing have to
     * refer to the same object for the removal to find anything.
     */
    private final Runnable onPlacementRegistered = this::publishIfElected;

    /**
     * How many consecutive elected-cluster-manager observations a member may be missing before it is
     * treated as gone.
     *
     * <p>Not a tuned number so much as a stated one. Every cluster state change this node applies is an
     * observation, so this is "absent across a sustained run of cluster activity" rather than a duration.
     * Too low resurrects the C13 failure, where a restarting node's shards move to nodes holding none of
     * its data and recover empty while looking healthy. Too high leaves rendezvous weight on nodes that
     * are never coming back, which under scale-to-zero is permanent rather than transient. Ten is chosen
     * to be clearly on the safe side of that first failure, and should be re-derived against a real
     * restart profile before anyone relies on the exact value.
     */
    static final long ABSENT_OBSERVATIONS_BEFORE_DECOMMISSION = 10;

    /**
     * Consecutive absences per member, held only on the elected cluster manager.
     *
     * <p>Deliberately not persisted. See {@link #decommissionable}: losing this delays a decommission and
     * can never cause one, which is the direction that fails safely.
     */
    private final java.util.Map<String, Long> absentObservations = new java.util.concurrent.ConcurrentHashMap<>();

    public ComputedPlacementMembershipService(ClusterService clusterService) {
        this.clusterService = clusterService;
        // Publish as soon as placement is enabled rather than waiting for the next unrelated cluster
        // state change. Without this, a cluster that installs a supplier and then goes idle keeps
        // computing placement against the live node list, which is the input this whole mechanism exists
        // to stop using, and it does so silently.
        AbsentIndexRoutingSuppliers.addRegistrationListener(onPlacementRegistered);
    }

    private void publishIfElected() {
        // The registry is static and outlives any single node, so a node that has shut down is still on
        // its listener list. Leaving would mean submitting cluster state updates against a closed service,
        // which in a test JVM means every node of every earlier suite waking up on the next registration.
        if (clusterService.lifecycleState() != Lifecycle.State.STARTED) {
            AbsentIndexRoutingSuppliers.removeRegistrationListener(onPlacementRegistered);
            return;
        }
        ClusterState state = clusterService.state();
        if (state.nodes().isLocalNodeElectedClusterManager() == false) {
            return;
        }
        List<String> dataNodes = dataNodeIds(state);
        if (dataNodes.isEmpty() == false && get(state).withNodes(dataNodes) != get(state)) {
            submitUpdate(dataNodes, List.of());
        }
    }

    /** The membership published in this state, or empty when nothing has been published yet. */
    public static ComputedPlacementMembership get(ClusterState state) {
        ComputedPlacementMembership membership = (ComputedPlacementMembership) state.metadata().custom(ComputedPlacementMembership.TYPE);
        return membership == null ? ComputedPlacementMembership.EMPTY : membership;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().nodes().isLocalNodeElectedClusterManager() == false) {
            return;
        }
        // Nothing to maintain until a placement supplier is installed. An unconfigured cluster must not
        // acquire this metadata at all, so that turning the feature off leaves no trace behind.
        if (AbsentIndexRoutingSuppliers.isRegistered() == false) {
            return;
        }
        List<String> dataNodes = dataNodeIds(event.state());
        if (dataNodes.isEmpty()) {
            return;
        }
        List<String> decommission = decommissionable(get(event.state()), dataNodes);
        if (get(event.state()).withNodes(dataNodes) == get(event.state()) && decommission.isEmpty()) {
            return; // already a superset with nothing to drop, and withNodes returning the same instance is how we know
        }
        submitUpdate(dataNodes, decommission);
    }

    /**
     * Members that have been absent long enough to be treated as gone rather than restarting.
     *
     * <p>Counted in consecutive observations rather than elapsed time. A wall clock says how long a node
     * has been away and not whether anyone was watching, so a cluster manager that was itself restarting
     * would see a long absence for a node that never left. Consecutive observations by an elected cluster
     * manager are absences somebody actually saw.
     *
     * <p><b>The counter is in memory, and losing it is the safe direction.</b> A cluster manager election
     * resets every count, which delays a decommission and never causes one. That asymmetry is deliberate:
     * decommissioning a node that was merely restarting moves its shards to nodes holding none of its data,
     * and those recover empty while looking healthy, which is what C13 hit and it is silent. Waiting longer
     * costs requests that fail and retry, which is loud and self-correcting.
     */
    private List<String> decommissionable(ComputedPlacementMembership membership, List<String> presentDataNodes) {
        for (String present : presentDataNodes) {
            absentObservations.remove(present);
        }
        List<String> gone = new ArrayList<>();
        for (String member : membership.nodeIds()) {
            if (presentDataNodes.contains(member)) {
                continue;
            }
            long absences = absentObservations.merge(member, 1L, Long::sum);
            if (absences >= ABSENT_OBSERVATIONS_BEFORE_DECOMMISSION) {
                gone.add(member);
            }
        }
        return gone;
    }

    private void submitUpdate(List<String> dataNodes, List<String> decommission) {
        clusterService.submitStateUpdateTask("computed-placement-membership", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                ComputedPlacementMembership current = get(currentState);
                ComputedPlacementMembership updated = current.withNodes(dataNodes).withoutNodes(decommission);
                if (updated == current) {
                    return currentState;
                }
                logger.info("computed placement membership now {}", updated);
                return ClusterState.builder(currentState)
                    .metadata(Metadata.builder(currentState.metadata()).putCustom(ComputedPlacementMembership.TYPE, updated))
                    .build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("failed to update computed placement membership", e);
            }
        });
    }

    private static List<String> dataNodeIds(ClusterState state) {
        List<String> nodeIds = new ArrayList<>();
        for (DiscoveryNode node : state.nodes()) {
            if (node.isDataNode()) {
                nodeIds.add(node.getId());
            }
        }
        return nodeIds;
    }
}
