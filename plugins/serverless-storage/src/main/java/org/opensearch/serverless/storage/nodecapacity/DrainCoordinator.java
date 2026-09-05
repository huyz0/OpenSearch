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
        return splitNames(value);
    }

    /**
     * The excluded names written by this coordinator, i.e. the transient setting only.
     *
     * <p>Deliberately separate from {@link #currentlyExcludedNames}: that method answers "what is
     * excluded right now", including an operator's persistent exclusions, which is what a reporting
     * or sweeping caller wants. This one answers "what did we write", which is the only correct
     * basis for a read-modify-write against a key we only ever write transiently -- see {@link
     * #mutateExcludeNames} (finding N-7).
     *
     * @param state the cluster state to read from.
     */
    static Set<String> transientExcludedNames(ClusterState state) {
        return splitNames(state.metadata().transientSettings().get(EXCLUDE_NAME_SETTING_KEY));
    }

    /**
     * Splits a comma-joined node-name list.
     *
     * <p>Node names containing a comma are rejected on the write path rather than mangled here (see
     * {@link #drain}, finding N-10): a name with a comma in it would otherwise split into two bogus
     * names, each of which the hygiene sweep would then find "stale" and try to remove, every tick.
     */
    private static Set<String> splitNames(String value) {
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
        // Finding N-10: the exclude list is a comma-joined string with no escaping, so a name
        // containing a comma would be stored and then read back as two different, bogus names --
        // neither of which matches a live node, so the hygiene sweep would try to remove them on
        // every tick, forever, while the node the operator asked to drain was never actually
        // excluded. Refusing the name up front is the only honest answer: there is no encoding that
        // makes it work without changing the format core itself reads.
        if (nodeName != null && nodeName.indexOf(',') >= 0) {
            listener.onFailure(
                new IllegalArgumentException(
                    "cannot drain node ["
                        + nodeName
                        + "]: node names containing a comma cannot be represented in "
                        + EXCLUDE_NAME_SETTING_KEY
                        + ", which is a comma-separated list with no escaping"
                )
            );
            return;
        }
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
                // Finding N-7. This used to read and compare against currentlyExcludedNames(), which
                // falls back to the *persistent* setting when no transient one exists -- while the
                // write below only ever touches the transient one. That asymmetry produced an
                // infinite publication loop on a perfectly healthy cluster: with an operator's
                // persistent exclude of a node that has since left, the hygiene sweep resolved
                // {old-node} through the persistent fallback, found it stale, and called in here;
                // the mutation produced {}, which did not equal {old-node}, so this proceeded;
                // removing a transient key that was never present changed nothing, but returned a
                // newly constructed ClusterState -- and MasterService publishes on reference
                // inequality. One full cluster-state publication and one full reroute, every eval
                // tick, forever. The next tick read the same unchanged persistent value.
                //
                // The same asymmetry had a second consequence: cancelling the last transient drain
                // removed the key outright, silently resurrecting any persistent exclusions, while
                // the API reported the drain cancelled.
                //
                // So the read-modify-write is now transient-only and self-consistent. The persistent
                // fallback stays in currentlyExcludedNames(), which is the read-only reporting path
                // where "what is actually excluded right now" is the correct answer.
                Set<String> current = transientExcludedNames(currentState);
                Set<String> names = new LinkedHashSet<>(current);
                mutation.accept(names);
                if (names.equals(current)) {
                    // The *same* reference, so MasterService does not publish a no-op state.
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
