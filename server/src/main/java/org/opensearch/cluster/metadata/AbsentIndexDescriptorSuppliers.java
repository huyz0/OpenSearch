/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Node-level hook for resolving an index that has no entry in cluster state metadata.
 *
 * <p>Area H's premise is that cluster state should hold no entry per index, which means resolution has to
 * answer from somewhere else. This is that seam, and it is deliberately the same shape as
 * {@code AbsentIndexRoutingSuppliers} from Area C: try the published structure first, fall back to a
 * supplier, and behave exactly as before when nothing is installed.
 *
 * <p><b>Why copy that shape rather than design a new one.</b> Area C's seam was applied across nine call
 * sites and the failures it produced are known and documented: the same absence has to mean the same thing
 * everywhere, the fast path has to be identical rather than merely equivalent, and a caller that silently
 * resolves nothing is worse than one that throws. Reusing the shape inherits those lessons instead of
 * rediscovering them.
 *
 * <p><b>What this does not do.</b> It does not make resolution asynchronous. A descriptor lookup is a
 * realtime GET against one shard, measured flat at about 0.8 ms up to fifty thousand descriptors (S21),
 * but it is still a remote call where a map read was local. Every caller of this seam is on a path that
 * already does remote work, and a supplier that blocks a transport thread would be a worse problem than
 * the one this solves. The supplier is therefore expected to answer from a cache and to decline rather
 * than block when it cannot.
 *
 * <p>Unset by default, so an ordinary cluster resolves exactly as it always has.
 */
public final class AbsentIndexDescriptorSuppliers {

    private static final AtomicReference<Function<String, IndexDescriptor>> SUPPLIER = new AtomicReference<>();

    private AbsentIndexDescriptorSuppliers() {}

    /**
     * Installs the supplier. Registering null clears it, which is what lets a test restore the default
     * rather than leaking a supplier into unrelated cases.
     */
    public static void register(Function<String, IndexDescriptor> supplier) {
        SUPPLIER.set(supplier);
    }

    /** Whether anything is installed, so callers can skip work that would be discarded. */
    public static boolean isRegistered() {
        return SUPPLIER.get() != null;
    }

    /**
     * The descriptor for a name with no metadata entry, or null to fall back to the existing behaviour.
     *
     * <p>A supplier that throws is treated as having no answer rather than being allowed to fail the
     * request, for the reason Area C settled: this is a degradation path already, and turning a plugin
     * bug into a request failure makes the absence worse than it was.
     */
    public static IndexDescriptor supply(String indexName) {
        Function<String, IndexDescriptor> supplier = SUPPLIER.get();
        if (supplier == null || indexName == null) {
            return null;
        }
        try {
            return supplier.apply(indexName);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Whether a name resolves to something that exists, consulting metadata first and the supplier only
     * on a miss.
     *
     * <p>The order is what keeps an ordinary cluster unchanged: a name present in metadata never reaches
     * the supplier, so no cluster that has not opted in pays a remote lookup. A tombstoned descriptor
     * answers false, which is what makes deletion durable without an {@code IndexGraveyard}.
     */
    public static boolean exists(Metadata metadata, String indexName) {
        if (metadata.getIndicesLookup().containsKey(indexName)) {
            return true;
        }
        if (isRegistered() == false) {
            return false;
        }
        IndexDescriptor descriptor = supply(indexName);
        return descriptor != null && descriptor.exists();
    }

    /**
     * Resolves several names at once, so a caller with a list pays one round trip rather than one each.
     *
     * <p>Present because resolution is frequently plural and the per-name form would otherwise turn a
     * ten-index request into ten sequential lookups, which is the shape of mistake that made wake and
     * sleep cost a publication each before they were batched.
     */
    public static List<IndexDescriptor> supplyAll(List<String> indexNames) {
        if (isRegistered() == false) {
            return List.of();
        }
        return indexNames.stream().map(AbsentIndexDescriptorSuppliers::supply).filter(java.util.Objects::nonNull).toList();
    }
}
