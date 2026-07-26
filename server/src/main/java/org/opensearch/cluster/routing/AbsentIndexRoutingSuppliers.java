/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * Node-level hook for supplying a routing entry for an index that has none published.
 *
 * <p>Phase A made an index legal in metadata and absent from the routing table, and taught the request
 * paths to degrade rather than throw. That degradation is deliberately pessimistic: no routing entry
 * means no shard available. This hook lets a plugin answer the same question differently, by
 * <em>computing</em> the entry instead of reporting its absence.
 *
 * <p><b>Why a hook rather than a lazy routing table.</b> The obvious way to make routing derived is to
 * change {@link RoutingTable}'s internal map to hold suppliers, mirroring what was done for
 * {@code Metadata}. That map has around forty internal references, and among them the diff, the
 * serializer and the verifiable serializer. {@code Metadata}'s holders stayed contained because they
 * resolve before serialization; {@link RoutingTable} is diffed and serialized on <em>every</em> cluster
 * state publication, so a lazy entry there has to define what a diff of an unresolved entry means, and
 * getting that wrong means either no saving or two nodes reading one state differently.
 *
 * <p>The question dissolves once you notice it does not need asking. An entry computed identically on
 * every node from inputs every node already has does not need publishing at all. So a serverless index
 * publishes no routing entry, there is nothing to diff or serialize, and each node fills the gap
 * locally. Phase A is what makes the gap legal, which is why it was a prerequisite rather than merely
 * adjacent work.
 *
 * <p>A single static reference rather than an injected service, following
 * {@code EngineNativeSnapshotReleasers}: the call site is deep inside routing resolution, reached from
 * paths that have no plugin context to thread a dependency through.
 *
 * <p>Unset by default, so behaviour is exactly what Phase A shipped until a plugin opts in.
 */
public final class AbsentIndexRoutingSuppliers {

    private static final AtomicReference<BiFunction<ClusterState, IndexMetadata, IndexRoutingTable>> SUPPLIER = new AtomicReference<>();

    /**
     * Indices whose routing must <em>not</em> be published, because it will be supplied instead.
     *
     * <p>Separate from the supplier, and the separation is the whole point. A supplier only ever runs
     * when an index has no published routing entry, and index creation publishes one for every index it
     * creates. Without this predicate the supplier is installed and never invoked -- the mechanism looks
     * wired and is dead. That was missed on the first pass through this area and found only by asking
     * what an integration test would actually exercise.
     */
    private static final AtomicReference<Predicate<IndexMetadata>> UNPUBLISHED = new AtomicReference<>();

    private AbsentIndexRoutingSuppliers() {}

    /**
     * Declares which indices skip routing publication. Registering null clears it.
     *
     * <p>Deliberately not derived from the supplier. A supplier that declines still leaves the index
     * with published routing, which is correct; an index that skips publication and has no supplier
     * would have no routing at all, which is not. Keeping them separate makes the second case a
     * configuration error rather than a silent outage.
     */
    public static void registerUnpublished(Predicate<IndexMetadata> unpublished) {
        UNPUBLISHED.set(unpublished);
    }

    /**
     * Whether an index should have its routing entry published at creation.
     *
     * <p>Defaults to true, so an unconfigured cluster behaves exactly as it always has. A predicate that
     * throws is treated as "publish", because publishing routing that is then ignored is recoverable and
     * not publishing routing that is then needed is not.
     */
    public static boolean shouldPublishRouting(IndexMetadata indexMetadata) {
        Predicate<IndexMetadata> unpublished = UNPUBLISHED.get();
        if (unpublished == null || indexMetadata == null) {
            return true;
        }
        try {
            return unpublished.test(indexMetadata) == false;
        } catch (Exception e) {
            return true;
        }
    }

    /**
     * Installs the supplier. Last registration wins, and a null clears it, which is what lets a test
     * restore the default rather than leaking a supplier into unrelated cases.
     */
    public static void register(BiFunction<ClusterState, IndexMetadata, IndexRoutingTable> supplier) {
        SUPPLIER.set(supplier);
    }

    /**
     * The computed entry for an index with none published, or null to fall back to Phase A's
     * no-shard-available behaviour.
     *
     * <p>A supplier that throws is treated as having no answer rather than being allowed to fail the
     * request. This is a degradation path already; turning it into an error would make a plugin bug
     * worse than the absence it was installed to handle.
     */
    public static IndexRoutingTable supply(ClusterState state, IndexMetadata indexMetadata) {
        BiFunction<ClusterState, IndexMetadata, IndexRoutingTable> supplier = SUPPLIER.get();
        if (supplier == null) {
            return null;
        }
        try {
            return supplier.apply(state, indexMetadata);
        } catch (Exception e) {
            return null;
        }
    }

    /** Whether a supplier is installed. Exposed so callers can skip work they would otherwise discard. */
    public static boolean isRegistered() {
        return SUPPLIER.get() != null;
    }
}
