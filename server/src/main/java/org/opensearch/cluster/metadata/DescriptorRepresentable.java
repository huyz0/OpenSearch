/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

/**
 * Every reason an index must keep its cluster state entry, in one place.
 *
 * <p>A gated index has no cluster state entry at all, so anything {@link IndexDescriptor} cannot carry is
 * not degraded when an index is gated, it is gone. Until T7 nothing checked that: {@code ownsIndex} answered
 * on the single setting {@code index.serverless_storage.enabled}, and an index with configuration the
 * descriptor has no field for was gated anyway.
 *
 * <p><b>The list being closed and in one file is the point.</b> This is TiDB's
 * {@code checkAttributesInOrder} ({@code meta/meta.go:1286}), which names every attribute that stops a table
 * being lazily loaded, so adding a feature that needs residency forces an edit to that list. Ours was spread
 * across whatever happened to notice, which is why H19 and H20 were found separately rather than together.
 *
 * <p><b>What is deliberately not here.</b> In-sync allocation IDs, index blocks and rollover info are
 * runtime state rather than declared configuration, and treating them as reasons would make almost every
 * index ineligible while fixing nothing. The entries below are all user-declared configuration that a
 * descriptor silently drops.
 *
 * <p>This gates cluster state residency only, not computed placement. An index that stays in cluster state
 * still gets its placement computed, which is the arrangement that existed before gating was switched on and
 * is known to work. The combination that cannot be served is the reverse, a gated index with a published
 * routing entry, and that remains impossible because gating is a strict subset of placement ownership.
 */
public final class DescriptorRepresentable {

    private DescriptorRepresentable() {}

    /**
     * Why this index cannot be represented by a descriptor, or null if it can.
     *
     * <p>Returns the reason rather than a boolean so a caller can log which feature kept an index resident.
     * An operator asking why one index costs cluster state and its neighbour does not needs the answer, and
     * a boolean makes that question unanswerable.
     */
    public static String whyNotRepresentable(IndexMetadata indexMetadata) {
        if (indexMetadata == null) {
            return "there is no metadata to represent";
        }
        for (AliasMetadata alias : indexMetadata.getAliases().values()) {
            // A descriptor carries alias names and nothing else, so each of these would be dropped in
            // silence. The filter is the one that matters most: it restricts which documents a query
            // through the alias may see, and losing it widens a restricted view rather than breaking it.
            if (alias.filter() != null) {
                return "alias [" + alias.alias() + "] has a filter, which a descriptor cannot carry";
            }
            if (alias.indexRouting() != null || alias.searchRouting() != null) {
                return "alias [" + alias.alias() + "] has routing, which a descriptor cannot carry";
            }
            if (alias.writeIndex() != null) {
                return "alias [" + alias.alias() + "] declares a write index, which a descriptor cannot carry";
            }
            if (alias.isHidden() != null) {
                return "alias [" + alias.alias() + "] declares hidden, which a descriptor cannot carry";
            }
        }
        if (indexMetadata.getCustomData().isEmpty() == false) {
            return "index carries custom metadata " + indexMetadata.getCustomData().keySet() + ", which a descriptor cannot carry";
        }
        return null;
    }

    /** Whether gating this index would lose nothing. */
    public static boolean isRepresentable(IndexMetadata indexMetadata) {
        return whyNotRepresentable(indexMetadata) == null;
    }
}
