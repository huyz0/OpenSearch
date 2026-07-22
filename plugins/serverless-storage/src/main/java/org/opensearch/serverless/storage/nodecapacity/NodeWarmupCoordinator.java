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
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.client.Client;

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
 */
public final class NodeWarmupCoordinator {

    /** The transient setting key this coordinator reads/writes. */
    public static final String WARMING_NAMES_SETTING_KEY = "cluster.routing.allocation.serverless_storage.warming_names";

    private final Client client;

    /**
     * Creates a coordinator.
     *
     * @param client used to submit the transient settings update that actually marks/unmarks warming.
     */
    public NodeWarmupCoordinator(Client client) {
        this.client = client;
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
     * @param state the cluster state to compute the new warming set from.
     * @param nodeName the node name (not id) to mark warming -- matches how {@link DrainCoordinator}
     *                 keys on name, so the two mechanisms compose without an id/name mismatch.
     * @param listener completed once the settings update either applies or fails.
     */
    public void markWarming(ClusterState state, String nodeName, ActionListener<ClusterUpdateSettingsResponse> listener) {
        Set<String> names = new LinkedHashSet<>(currentlyWarmingNames(state));
        names.add(nodeName);
        applyWarmingNames(names, listener);
    }

    /**
     * Removes {@code nodeName} from the warming set -- a no-op success if it wasn't warming at all.
     *
     * @param state the cluster state to compute the new warming set from.
     * @param nodeName the node name to clear.
     * @param listener completed once the settings update either applies or fails.
     */
    public void clearWarming(ClusterState state, String nodeName, ActionListener<ClusterUpdateSettingsResponse> listener) {
        Set<String> names = new LinkedHashSet<>(currentlyWarmingNames(state));
        names.remove(nodeName);
        applyWarmingNames(names, listener);
    }

    private void applyWarmingNames(Set<String> names, ActionListener<ClusterUpdateSettingsResponse> listener) {
        Settings.Builder builder = Settings.builder();
        if (names.isEmpty()) {
            builder.putNull(WARMING_NAMES_SETTING_KEY);
        } else {
            builder.put(WARMING_NAMES_SETTING_KEY, String.join(",", names));
        }
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest().transientSettings(builder);
        client.admin().cluster().updateSettings(request, listener);
    }
}
