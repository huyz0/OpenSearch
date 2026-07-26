/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
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
 */
public class ComputedPlacementMembershipService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(ComputedPlacementMembershipService.class);

    private final ClusterService clusterService;

    /**
     * Held as a field rather than passed as a method reference twice, because adding and removing have to
     * refer to the same object for the removal to find anything.
     */
    private final Runnable onPlacementRegistered = this::publishIfElected;

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
            submitUpdate(dataNodes);
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
        if (get(event.state()).withNodes(dataNodes) == get(event.state())) {
            return; // already a superset, and withNodes returning the same instance is how we know
        }
        submitUpdate(dataNodes);
    }

    private void submitUpdate(List<String> dataNodes) {
        clusterService.submitStateUpdateTask("computed-placement-membership", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                ComputedPlacementMembership current = get(currentState);
                ComputedPlacementMembership updated = current.withNodes(dataNodes);
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
