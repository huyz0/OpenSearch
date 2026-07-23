/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Marks/unmarks a node as warming -- node autoscaling design doc part 2 (pre-warm before rotation),
 * Phase 3. A warming node has joined the cluster but should not yet receive reader shard copies:
 * whoever boots the node (the external control plane, or a future in-repo warmup hook) calls {@link
 * #markWarming} before the node is useful, prefetches whatever it wants warm, then calls {@link
 * #clearWarming} once ready. {@link org.opensearch.serverless.storage.allocation.NodeWarmupAllocationDecider}
 * is what actually withholds allocation while a node is marked this way.
 *
 * <p>Deliberately structured like {@link DrainCoordinator}: a dedicated transient cluster setting
 * (not new cluster-state metadata) holding the comma-joined set of warming node names, so the state
 * survives cluster-manager failover for free and needs no separate {@code Writeable}/diff plumbing.
 * Both directions are idempotent for the same reason drain is -- whatever drives this will crash and
 * retry.
 *
 * <p>Mutates the warming set via a plain {@link ClusterStateUpdateTask}, exactly like {@link
 * DrainCoordinator} -- see that class's javadoc for why a {@code ClusterUpdateSettingsRequest}
 * computed from a caller-supplied {@link ClusterState} snapshot is unsafe under concurrent callers
 * (e.g. two nodes self-warming at once), and why this triggers a reroute explicitly afterward.
 */
public final class NodeWarmupCoordinator {

    private static final Logger logger = LogManager.getLogger(NodeWarmupCoordinator.class);

    /** The transient setting key this coordinator reads/writes. */
    public static final String WARMING_NAMES_SETTING_KEY = "cluster.routing.allocation.serverless_storage.warming_names";

    private final ClusterService clusterService;

    /**
     * Creates a coordinator.
     *
     * @param clusterService used to submit the cluster-state task that mutates the warming set, and
     *                       to trigger a reroute afterward.
     */
    public NodeWarmupCoordinator(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * Reads the current warming node names from cluster state.
     *
     * @param state the cluster state to read from.
     */
    public static Set<String> currentlyWarmingNames(ClusterState state) {
        return currentlyWarmingNames(state.metadata());
    }

    /**
     * Reads the current warming node names from cluster metadata directly -- the overload {@link
     * org.opensearch.serverless.storage.allocation.NodeWarmupAllocationDecider} uses, since {@code
     * RoutingAllocation} exposes {@link Metadata} but not the full {@link ClusterState}.
     *
     * @param metadata the cluster metadata to read from.
     */
    public static Set<String> currentlyWarmingNames(Metadata metadata) {
        String value = metadata.transientSettings().get(WARMING_NAMES_SETTING_KEY);
        if (value == null || value.isEmpty()) {
            return Set.of();
        }
        return Arrays.stream(value.split(",")).map(String::trim).filter(s -> s.isEmpty() == false).collect(Collectors.toSet());
    }

    /**
     * Adds {@code nodeName} to the warming set -- a no-op success if already present.
     *
     * @param nodeName the node name (not id) to mark warming -- matches how {@link DrainCoordinator}
     *                 keys on name, so the two mechanisms compose without an id/name mismatch.
     * @param listener completed once the mutation either applies or fails.
     */
    public void markWarming(String nodeName, ActionListener<Void> listener) {
        mutateWarmingNames(names -> names.add(nodeName), listener);
    }

    /**
     * Removes {@code nodeName} from the warming set -- a no-op success if it wasn't warming at all.
     *
     * @param nodeName the node name to clear.
     * @param listener completed once the mutation either applies or fails.
     */
    public void clearWarming(String nodeName, ActionListener<Void> listener) {
        mutateWarmingNames(names -> names.remove(nodeName), listener);
    }

    /**
     * Removes every name in {@code staleNames} from the warming set -- mirrors {@link
     * DrainCoordinator#removeStaleNames}: a warming mark left behind for a node that has since left
     * the cluster (crashed before calling {@link #clearWarming}, or was replaced by a differently-
     * named node) would otherwise block allocation to any future node that happens to reuse the name,
     * forever.
     *
     * @param staleNames names to remove; a no-op if none of them are currently warming.
     * @param listener completed once the mutation either applies or fails.
     */
    public void removeStaleNames(Set<String> staleNames, ActionListener<Void> listener) {
        mutateWarmingNames(names -> names.removeAll(staleNames), listener);
    }

    /**
     * Submits a {@link ClusterStateUpdateTask} that recomputes the warming set from whatever cluster
     * state is current when the task actually runs (not any caller-supplied snapshot), applies {@code
     * mutation}, and -- only if that actually changed the set -- writes it back and triggers a reroute.
     */
    private void mutateWarmingNames(java.util.function.Consumer<Set<String>> mutation, ActionListener<Void> listener) {
        clusterService.submitStateUpdateTask("serverless-storage-mutate-warming-names", new ClusterStateUpdateTask(Priority.NORMAL) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                Set<String> names = new LinkedHashSet<>(currentlyWarmingNames(currentState));
                mutation.accept(names);
                if (names.equals(currentlyWarmingNames(currentState))) {
                    return currentState;
                }
                Settings.Builder transientSettings = Settings.builder().put(currentState.metadata().transientSettings());
                if (names.isEmpty()) {
                    transientSettings.remove(WARMING_NAMES_SETTING_KEY);
                } else {
                    transientSettings.put(WARMING_NAMES_SETTING_KEY, String.join(",", names));
                }
                return ClusterState.builder(currentState)
                    .metadata(Metadata.builder(currentState.metadata()).transientSettings(transientSettings.build()))
                    .build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (oldState != newState) {
                    clusterService.getRerouteService()
                        .reroute(
                            "serverless-storage warming names changed",
                            Priority.NORMAL,
                            ActionListener.wrap(rerouted -> {}, e -> logger.warn("reroute after warming names change failed", e))
                        );
                }
                listener.onResponse(null);
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
