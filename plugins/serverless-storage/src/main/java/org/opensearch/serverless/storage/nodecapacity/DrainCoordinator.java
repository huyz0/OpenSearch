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
 * Marks/unmarks a node as draining by wrapping core's existing {@code
 * cluster.routing.allocation.exclude._name} setting -- node autoscaling design doc part 1, "Drain
 * mode": "wraps core's existing ... mechanism rather than inventing new allocator logic." Idempotent
 * both directions: draining an already-draining node, or cancelling a non-existent drain, are both
 * no-op successes, since an external control plane will crash and retry.
 *
 * <p>Deliberately holds no separate durable "is this node draining" record beyond the exclude
 * setting itself -- the setting is already part of cluster state (survives cluster-manager
 * failover), and a second source of truth would only risk drifting from it. {@code
 * NodeCapacitySignalService} reads this same setting to populate {@link
 * NodeCapacityEntry#draining()} and to sweep stale (departed-node) entries.
 *
 * <p><b>Mutates the exclude list via a plain {@link ClusterStateUpdateTask}, not a {@code
 * ClusterUpdateSettingsRequest}, deliberately.</b> The settings-update transport action only
 * guarantees atomic replacement of a key's whole value, not a read-modify-write of its contents --
 * an earlier version of this class read {@link #currentlyExcludedNames} from a caller-supplied
 * {@link ClusterState} snapshot, computed the new comma-joined value client-side, and submitted that
 * as a full replacement. Two concurrent calls (e.g. draining two different nodes back to back) could
 * both read the same starting snapshot and race: the second submission's value would silently
 * clobber the first's addition, since neither request carries any dependency on the other. Computing
 * the new value inside {@link ClusterStateUpdateTask#execute} instead -- against whatever state is
 * current at the moment this task actually runs, serialized by the cluster-manager's single-threaded
 * task queue -- closes that race. Mirrors {@code ShardSuspensionCoordinator#suspend}'s identical
 * reasoning for its own cluster-state mutation.
 *
 * <p>Bypassing {@code TransportClusterUpdateSettingsAction} also bypasses its automatic
 * reroute-after-settings-change behavior, so {@link #mutateExcludeNames} triggers one explicitly via
 * {@link ClusterService#getRerouteService()} whenever the exclude list actually changed.
 */
public final class DrainCoordinator {

    private static final Logger logger = LogManager.getLogger(DrainCoordinator.class);

    /** The core setting key this coordinator reads/writes -- see {@code FilterAllocationDecider}. */
    public static final String EXCLUDE_NAME_SETTING_KEY = "cluster.routing.allocation.exclude._name";

    private final ClusterService clusterService;

    /**
     * Creates a coordinator.
     *
     * @param clusterService used to submit the cluster-state task that mutates the exclude list, and
     *                       to trigger a reroute afterward.
     */
    public DrainCoordinator(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * Reads the current excluded node names from cluster state -- transient settings take
     * precedence over persistent, matching how core itself resolves this setting.
     *
     * @param state the cluster state to read from.
     */
    public static Set<String> currentlyExcludedNames(ClusterState state) {
        String transientValue = state.metadata().transientSettings().get(EXCLUDE_NAME_SETTING_KEY);
        String value = transientValue != null ? transientValue : state.metadata().persistentSettings().get(EXCLUDE_NAME_SETTING_KEY);
        if (value == null || value.isEmpty()) {
            return Set.of();
        }
        return Arrays.stream(value.split(",")).map(String::trim).filter(s -> s.isEmpty() == false).collect(Collectors.toSet());
    }

    /**
     * Adds {@code nodeName} to the exclude list, draining it -- a no-op success if already present.
     *
     * @param nodeName the node name (not id) to drain -- core's exclude filter matches on name.
     * @param listener completed once the mutation either applies or fails.
     */
    public void drain(String nodeName, ActionListener<Void> listener) {
        // Already draining is not special-cased: the task still runs and still acks, so the caller
        // gets a real ack either way rather than a distinct "already draining" response shape it
        // would have to handle separately -- it just ends up a no-op state update.
        mutateExcludeNames(names -> names.add(nodeName), listener);
    }

    /**
     * Removes {@code nodeName} from the exclude list, cancelling its drain -- a no-op success if it
     * wasn't draining at all.
     *
     * @param nodeName the node name to cancel the drain of.
     * @param listener completed once the mutation either applies or fails.
     */
    public void cancelDrain(String nodeName, ActionListener<Void> listener) {
        mutateExcludeNames(names -> names.remove(nodeName), listener);
    }

    /**
     * Removes every name in {@code staleNames} from the exclude list -- the hygiene sweep node
     * autoscaling design doc part 1 requires: "any excluded name with no matching live node ... gets
     * removed," so a leaked exclude entry can never silently poison a future node that reuses the name.
     *
     * @param staleNames names to remove; a no-op if none of them are currently excluded.
     * @param listener completed once the mutation either applies or fails.
     */
    public void removeStaleNames(Set<String> staleNames, ActionListener<Void> listener) {
        mutateExcludeNames(names -> names.removeAll(staleNames), listener);
    }

    /**
     * Submits a {@link ClusterStateUpdateTask} that recomputes the exclude list from whatever cluster
     * state is current when the task actually runs (not any caller-supplied snapshot), applies {@code
     * mutation}, and -- only if that actually changed the set -- writes it back and triggers a reroute.
     */
    private void mutateExcludeNames(java.util.function.Consumer<Set<String>> mutation, ActionListener<Void> listener) {
        clusterService.submitStateUpdateTask("serverless-storage-mutate-drain-exclude-names", new ClusterStateUpdateTask(Priority.NORMAL) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                Set<String> names = new LinkedHashSet<>(currentlyExcludedNames(currentState));
                mutation.accept(names);
                if (names.equals(currentlyExcludedNames(currentState))) {
                    return currentState;
                }
                Settings.Builder transientSettings = Settings.builder().put(currentState.metadata().transientSettings());
                if (names.isEmpty()) {
                    transientSettings.remove(EXCLUDE_NAME_SETTING_KEY);
                } else {
                    transientSettings.put(EXCLUDE_NAME_SETTING_KEY, String.join(",", names));
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
                            "serverless-storage drain exclude names changed",
                            Priority.NORMAL,
                            ActionListener.wrap(rerouted -> {}, e -> logger.warn("reroute after drain exclude change failed", e))
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
