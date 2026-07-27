/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * The gate that decides whether an index gets a cluster state entry at all.
 *
 * <p>This is Area H's third phase in one predicate. When it answers true for an index, creation records a
 * descriptor and writes nothing to cluster state: no metadata entry, no routing entry, no publication, and
 * none of the O(total indices) work that S19 and S20 measured. Creation becomes one indexing operation
 * whose cost is independent of how many indices exist.
 *
 * <p><b>The measurement that decides whether this was worth building</b> is G2a re-run with the gate open:
 * per-index creation cost against population, today 7.4, 10.3, 34.6 and 98.8 ms at 200, 1k, 3k and 6k. If
 * that curve is not flat, something else is superlinear and Area H should stop rather than continue.
 *
 * <p><b>Why a predicate rather than a setting.</b> The decision is per index, not per cluster: the same
 * cluster carries ordinary indices whose metadata must stay in cluster state and serverless ones whose
 * must not. A setting would force the choice for everything at once, and the migration this area needs is
 * exactly the ability to run both at the same time.
 *
 * <p><b>Failure semantics invert here</b>, and that is the part most easily got wrong. During dual write
 * (H2b) the descriptor was redundant, so a publisher that threw could be swallowed. Once this gate is
 * open the descriptor is the only record of the index, so a failed descriptor write must fail the
 * creation. An index that exists in neither place is a lost request; an index whose descriptor write
 * silently failed is a lost index.
 *
 * <p>Closed by default. Nothing changes until something opens it.
 */
public final class DescriptorOnlyCreation {

    private static final AtomicReference<Predicate<IndexMetadata>> GATE = new AtomicReference<>();

    private DescriptorOnlyCreation() {}

    /** Opens the gate for indices the predicate accepts. Registering null closes it entirely. */
    public static void register(Predicate<IndexMetadata> gate) {
        GATE.set(gate);
    }

    public static boolean isRegistered() {
        return GATE.get() != null;
    }

    /**
     * Whether this index should skip its cluster state entry.
     *
     * <p>A predicate that throws answers false, so a broken gate leaves the index in cluster state rather
     * than nowhere. That direction is deliberate: the failure mode of answering true wrongly is an index
     * with no record anywhere, and the failure mode of answering false wrongly is an index that costs
     * what it always cost.
     */
    public static boolean skipsClusterState(IndexMetadata indexMetadata) {
        Predicate<IndexMetadata> gate = GATE.get();
        if (gate == null || indexMetadata == null) {
            return false;
        }
        try {
            return gate.test(indexMetadata);
        } catch (Exception e) {
            return false;
        }
    }
}
