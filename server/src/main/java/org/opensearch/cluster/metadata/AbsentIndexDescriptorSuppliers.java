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
    /**
     * Supplies one page of gated indices in pagination order.
     *
     * <p>The whole point is the {@code size} argument. H16 measured that pagination sorts the entire
     * population to produce one page, so making gated indices visible by folding them into that sort would
     * have made the cost problem worse while appearing to fix the correctness one. A pager is asked for a
     * page and returns a page, which is what the descriptor index can actually do cheaply: S24 measured
     * paging by sorted name with {@code search_after} at roughly 33 ms per thousand names.
     */
    @FunctionalInterface
    public interface DescriptorPager {
        /**
         * The next {@code size} gated indices after the given position, in the given order.
         *
         * @param afterName the last name of the previous page, or null for the first page
         * @param afterCreationDate the last creation date of the previous page, ignored when afterName is null
         * @param ascending whether the order is ascending
         * @param size how many to return, which bounds the work
         */
        List<IndexDescriptor> page(String afterName, long afterCreationDate, boolean ascending, int size);
    }

    private static final AtomicReference<DescriptorPager> PAGER = new AtomicReference<>();

    /** Installs the pager. Registering null clears it, which is how a test restores the default. */
    public static void registerPager(DescriptorPager pager) {
        PAGER.set(pager);
    }

    /** Whether anything can supply gated pages. With nothing installed, pagination behaves as before. */
    public static boolean isPagerRegistered() {
        return PAGER.get() != null;
    }

    /**
     * One page of gated indices, or empty when nothing is installed or the pager fails.
     *
     * <p>Failing to empty rather than throwing matches how a failing supplier is treated everywhere else in
     * this class. It does mean a broken pager silently returns short pages, which is the failure mode this
     * area exists to be suspicious of, so callers that need to distinguish the two should ask {@link
     * #isPagerRegistered()} rather than inferring it from an empty result.
     */
    public static List<IndexDescriptor> page(String afterName, long afterCreationDate, boolean ascending, int size) {
        DescriptorPager pager = PAGER.get();
        if (pager == null || size <= 0) {
            return List.of();
        }
        try {
            List<IndexDescriptor> page = pager.page(afterName, afterCreationDate, ascending, size);
            return page == null ? List.of() : page;
        } catch (Exception e) {
            return List.of();
        }
    }

    public static List<IndexDescriptor> supplyAll(List<String> indexNames) {
        if (isRegistered() == false) {
            return List.of();
        }
        return indexNames.stream().map(AbsentIndexDescriptorSuppliers::supply).filter(java.util.Objects::nonNull).toList();
    }
}
