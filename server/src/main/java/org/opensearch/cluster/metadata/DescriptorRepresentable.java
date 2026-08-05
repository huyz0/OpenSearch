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
        // T29 widened this from four alias properties to every alias, and the reason is not that a
        // descriptor cannot carry the name. It carries it already. The reason is that nothing can ever
        // change it.
        //
        // Every alias operation is a cluster state update that looks the index up in Metadata and rewrites
        // its IndexMetadata. A gated index is not in Metadata, so there is nothing to rewrite: adding an
        // alias to one does not fail cleanly, it times out with ClusterManagerNotDiscoveredException.
        // An alias could therefore only ever be set at creation and never added, removed or repointed,
        // which is the opposite of what an alias is for. A client is given an alias precisely so the index
        // behind it can change.
        //
        // T29 also measured that resolution never reads the field: naming the alias of a gated index
        // resolved to nothing, silently under the options most clients use. Building that resolution was
        // the alternative to this rule and was rejected, because it would have served a set-once alias
        // with a refresh-bound visibility window, which is a partial feature that invites exactly the
        // usage it cannot support.
        //
        // The four narrower rules this replaces (filter, routing, write index, hidden) are now subsumed.
        // They are worth remembering rather than deleting from history: the filter one mattered most,
        // because losing an alias filter widens a restricted view rather than breaking it, which is a
        // security-shaped failure rather than an availability-shaped one.
        if (indexMetadata.getAliases().isEmpty() == false) {
            return "index declares aliases "
                + indexMetadata.getAliases().keySet()
                + ", and an alias on an index held outside cluster state could never be changed afterwards";
        }
        if (indexMetadata.getCustomData().isEmpty() == false) {
            return "index carries custom metadata " + indexMetadata.getCustomData().keySet() + ", which a descriptor cannot carry";
        }
        // Hidden was already only half-honoured, which is what makes refusing it a correction rather than a
        // narrowing. IndexDescriptor carries the flag and wildcard expansion is the single line that reads
        // it, but IndexDescriptor#toIndexMetadata does not put index.hidden back, so the metadata a node
        // synthesises to open the shard says the index is not hidden. One of the two answers was always
        // going to be wrong, and an index that hides from wildcards while presenting itself as visible
        // everywhere else is the worse of them.
        //
        // Refusing also keeps expansion honest against an object store, where the answer has to be derivable
        // from a listing. A key under the descriptor prefix carries a name and nothing else, so serving
        // hidden from a bucket needs either a GET for every match -- a hundred round trips for a wildcard
        // the cap exists to keep cheap -- or a second keyspace written on create and deleted on delete,
        // which is a consistency problem bought for a flag that has one reader.
        if (indexMetadata.isHidden()) {
            return "index is hidden, and a descriptor's hidden flag is read by wildcard expansion while "
                + "IndexDescriptor#toIndexMetadata drops it, so the index would be hidden from wildcards and "
                + "visible to everything else";
        }
        return null;
    }

    /** Whether gating this index would lose nothing. */
    public static boolean isRepresentable(IndexMetadata indexMetadata) {
        return whyNotRepresentable(indexMetadata) == null;
    }
}
