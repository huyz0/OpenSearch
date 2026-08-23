/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * An {@link IndexCatalog} that answers from whatever a plugin has already registered with the two static
 * registries -- {@link AbsentIndexDescriptorSuppliers} for the metadata half, {@link
 * AbsentIndexRoutingSuppliers} for the routing half -- rather than duplicating that plugin's
 * lookup/caching logic in a plugin-owned adapter. It replaces the pair {@code
 * SupplierBackedIndexMetadataResolver} + {@code SupplierBackedIndexRoutingResolver}, which were two classes
 * in two packages doing exactly this for one plugin.
 *
 * <p><b>Settled architecture: one authority per plane.</b> The two static registries remain the node-level
 * <em>authorities</em> -- they carry a descriptor vocabulary (uuid, the three-way live/tombstoned/unknown
 * distinction, prefix expansion, shard suspension, memoisation) that this generic SPI correctly does not
 * expose, and a handful of core call sites legitimately consult them directly for exactly those richer
 * questions. This class is the registration <em>front door</em>: the object a plugin returns from {@code
 * ClusterPlugin#getIndexCatalog()} to feed the state-scoped read paths ({@link
 * Metadata#indexOrResolved(String)}, {@link Metadata#existsOrResolved(String)}, {@code
 * ClusterState#getIndexRoutingTable}, {@code ClusterState#allShards}, {@code
 * RoutingTable#shouldPublishRouting}) from those authorities. It holds no state of its own: every method
 * forwards, so the answers a state-scoped read gets and the answers the registries' few deliberate direct
 * callers get are the same answers by construction.
 *
 * <p><b>Why this lives in {@code server/} instead of the plugin.</b> A plugin that calls {@code
 * AbsentIndexDescriptorSuppliers.register(...)}/{@code AbsentIndexRoutingSuppliers.register(...)} today
 * (the only real one, as of this writing, is the registered storage plugin) already has everything this
 * class needs registered: this is pure glue with zero plugin-specific logic, so it belongs beside the
 * registries it forwards to, fully unit-testable with {@code :server:test} alone, and reusable by any
 * future plugin that populates the same registries instead of each one writing its own copy.
 *
 * <p><b>Additive, not a replacement.</b> A plugin using this still registers with those registries exactly
 * as before -- that registration is what this class reads from. Returning {@code Optional.of(new
 * SupplierBackedIndexCatalog())} from {@link org.opensearch.plugins.ClusterPlugin#getIndexCatalog()} only
 * adds a second path to the same answer, reachable exclusively through {@link
 * Metadata#indexOrResolved(String)} and {@code ClusterState#getIndexRoutingTable(String)} -- a caller has
 * to opt in by name to reach this catalog at all. {@link Metadata#index(String)} itself, which every
 * pre-existing call site still uses, never changes behavior regardless of whether this is registered; see
 * that method's own javadoc for why an earlier version of this design got that backwards, and why it now
 * cannot.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class SupplierBackedIndexCatalog implements IndexCatalog {

    /**
     * Delegates to {@link AbsentIndexDescriptorSuppliers#synthesisedMetadata(String)} specifically (not
     * {@link AbsentIndexDescriptorSuppliers#supply}, its lower-level primitive), which is the exact call
     * {@code metadataOrDescriptor} itself makes on a miss -- so this class's answer for a given index name
     * is identical to what every existing call site already gets, cache and all.
     */
    @Override
    public IndexMetadata resolveMetadata(Metadata metadata, String indexName) {
        return AbsentIndexDescriptorSuppliers.synthesisedMetadata(indexName);
    }

    @Override
    public IndexRoutingTable resolveRouting(ClusterState state, IndexMetadata indexMetadata) {
        return AbsentIndexRoutingSuppliers.supply(state, indexMetadata);
    }

    @Override
    public boolean shouldPublishRouting(IndexMetadata indexMetadata) {
        return AbsentIndexRoutingSuppliers.shouldPublishRouting(indexMetadata);
    }

    /**
     * Tied to {@link AbsentIndexRoutingSuppliers#isRegistered()}, read live on every call rather than
     * captured once -- which is the whole point of {@link IndexCatalog#isActive()}: this object is
     * installed for the node's lifetime, while the registry underneath it fills and empties as the feature
     * is installed and uninstalled.
     *
     * <p><b>Why the routing registry specifically, and not "either registry".</b> All four core callers of
     * {@code isActive()} ask one question: can an open index legitimately have no published routing entry
     * on this node? That is exactly what a registered placement supplier makes possible, and it is exactly
     * what those call sites asked before this seam existed (they each consulted {@code
     * AbsentIndexRoutingSuppliers.isRegistered()} directly, each with a comment explaining why the resolver
     * SPI of the day could not answer it). Widening this to include the descriptor registry would make
     * every one of them fire on a descriptor-only node -- requesting metadata that is not needed,
     * suppressing an assertion that is still valid -- which is a behavior change none of them asked for.
     */
    @Override
    public boolean isActive() {
        return AbsentIndexRoutingSuppliers.isRegistered();
    }
}
