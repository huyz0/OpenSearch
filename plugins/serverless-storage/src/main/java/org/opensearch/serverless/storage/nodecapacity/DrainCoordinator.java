/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.client.Client;

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
 */
public final class DrainCoordinator {

    /** The core setting key this coordinator reads/writes -- see {@code FilterAllocationDecider}. */
    public static final String EXCLUDE_NAME_SETTING_KEY = "cluster.routing.allocation.exclude._name";

    private final Client client;

    /**
     * Creates a coordinator.
     *
     * @param client used to submit the transient settings update that actually marks/unmarks a drain.
     */
    public DrainCoordinator(Client client) {
        this.client = client;
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
     * @param state the cluster state to compute the new exclude list from.
     * @param nodeName the node name (not id) to drain -- core's exclude filter matches on name.
     * @param listener completed once the settings update either applies or fails.
     */
    public void drain(ClusterState state, String nodeName, ActionListener<ClusterUpdateSettingsResponse> listener) {
        // Already draining is not special-cased: applyExcludeNames still runs (a same-value settings
        // update), so the caller gets a real ack either way rather than a distinct "already draining"
        // response shape it would have to handle separately.
        Set<String> names = new LinkedHashSet<>(currentlyExcludedNames(state));
        names.add(nodeName);
        applyExcludeNames(names, listener);
    }

    /**
     * Removes {@code nodeName} from the exclude list, cancelling its drain -- a no-op success if it
     * wasn't draining at all.
     *
     * @param state the cluster state to compute the new exclude list from.
     * @param nodeName the node name to cancel the drain of.
     * @param listener completed once the settings update either applies or fails.
     */
    public void cancelDrain(ClusterState state, String nodeName, ActionListener<ClusterUpdateSettingsResponse> listener) {
        Set<String> names = new LinkedHashSet<>(currentlyExcludedNames(state));
        names.remove(nodeName);
        applyExcludeNames(names, listener);
    }

    /**
     * Removes every name in {@code staleNames} from the exclude list -- the hygiene sweep node
     * autoscaling design doc part 1 requires: "any excluded name with no matching live node ... gets
     * removed," so a leaked exclude entry can never silently poison a future node that reuses the name.
     *
     * @param state the cluster state to compute the new exclude list from.
     * @param staleNames names to remove; a no-op if none of them are currently excluded.
     * @param listener completed once the settings update either applies or fails.
     */
    public void removeStaleNames(ClusterState state, Set<String> staleNames, ActionListener<ClusterUpdateSettingsResponse> listener) {
        Set<String> names = new LinkedHashSet<>(currentlyExcludedNames(state));
        if (names.removeAll(staleNames) == false) {
            listener.onResponse(null);
            return;
        }
        applyExcludeNames(names, listener);
    }

    private void applyExcludeNames(Set<String> names, ActionListener<ClusterUpdateSettingsResponse> listener) {
        Settings.Builder builder = Settings.builder();
        if (names.isEmpty()) {
            builder.putNull(EXCLUDE_NAME_SETTING_KEY);
        } else {
            builder.put(EXCLUDE_NAME_SETTING_KEY, String.join(",", names));
        }
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest().transientSettings(builder);
        client.admin().cluster().updateSettings(request, listener);
    }
}
