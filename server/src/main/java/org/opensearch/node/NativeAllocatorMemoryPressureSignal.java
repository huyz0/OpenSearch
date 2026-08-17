/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.arrow.spi.PoolGroup;
import org.opensearch.plugin.stats.NativeAllocatorPoolStats;
import org.opensearch.ratelimitting.admissioncontrol.NativeMemoryPressureSignal;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;

import java.util.OptionalDouble;
import java.util.function.Supplier;

/**
 * Phase H of {@code core-pluggability-refactor-plan.md}: adapts the Arrow allocator's concrete {@link
 * NativeAllocatorPoolStats} (grouped by {@link PoolGroup}) onto the generic {@link
 * NativeMemoryPressureSignal} the admission-control package consumes -- see that interface's own javadoc
 * for why this split exists. This class, not {@code NativeMemoryBasedAdmissionController}, is the one
 * place in {@code server} that maps an {@link AdmissionControlActionType} onto a concrete {@link
 * PoolGroup} name, and it lives in {@code org.opensearch.node} deliberately: {@link Node} already has a
 * legitimate, independent reason to depend on {@code libs:opensearch-arrow-spi} (discovering the {@code
 * NativeAllocator} component plugins publish), so co-locating this adapter here means the
 * admission-control package itself needs no arrow-spi dependency at all.
 *
 * <p>Only {@link AdmissionControlActionType#INDEXING} maps to a pool group today, matching what {@code
 * NativeMemoryBasedAdmissionController} checked before this seam existed -- {@code SEARCH} and {@code
 * CLUSTER_ADMIN} both correctly answer "no signal" until a real mapping is needed for them.
 */
final class NativeAllocatorMemoryPressureSignal implements NativeMemoryPressureSignal {

    private final Supplier<NativeAllocatorPoolStats> statsSupplier;

    NativeAllocatorMemoryPressureSignal(Supplier<NativeAllocatorPoolStats> statsSupplier) {
        this.statsSupplier = statsSupplier;
    }

    @Override
    public OptionalDouble utilizationPercentFor(AdmissionControlActionType actionType) {
        if (actionType != AdmissionControlActionType.INDEXING) {
            return OptionalDouble.empty();
        }
        NativeAllocatorPoolStats stats = statsSupplier.get();
        if (stats == null) {
            return OptionalDouble.empty();
        }
        NativeAllocatorPoolStats.PoolStats indexingGroup = stats.getGroupedStats().get(PoolGroup.INDEXING.getName());
        if (indexingGroup == null || indexingGroup.getLimitBytes() <= 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(100.0 * indexingGroup.getAllocatedBytes() / indexingGroup.getLimitBytes());
    }
}
