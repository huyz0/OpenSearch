/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.ops;

import org.opensearch.search.builder.SearchSourceBuilder;

/** What a search gets by default, and what of it cannot be answered across shards: shared by every search entry point. */
public final class SearchRules {

    private SearchRules() {}

    /**
     * The defaults a request gets where its body did not speak.
     *
     * <p>Shared with {@code _msearch}, so a search inside a batch is the same search it would be on its
     * own. {@code track_total_hits} is left exactly as the caller set it -- this used to force it on after
     * parsing, so a caller asking for a cheaper count got the expensive one and was told {@code eq}.
     *
     * @param source the parsed search
     * @param counting whether this is a {@code _count}, which fetches nothing and counts everything
     */
    public static void applyDefaults(SearchSourceBuilder source, boolean counting) {
        if (counting) {
            // A count asks how many, not which. Fetching hits to throw them away would make the cheap
            // question cost the same as the expensive one, and a count is exact by definition.
            source.size(0);
            source.from(0);
            source.trackTotalHits(true);
            return;
        }
        if (source.size() < 0) {
            source.size(10);
        }
        if (source.from() < 0) {
            source.from(0);
        }
    }

    /**
     * Names the part of a search this cannot answer correctly across shards, or null if it can.
     *
     * <p><b>Refused rather than ignored, and the distinction is the whole of the allowlist rule.</b> Every feature below
     * parses happily and runs happily on a single shard; what none of them survives is being run on
     * several shards and having the answers concatenated. An aggregation needs a reduce step this does
     * not have — returning one shard's buckets as if they were the index's would be a number that is
     * wrong and looks right, which is the worst thing this system could hand anybody. A sort needs the
     * merge to compare sort values rather than scores. {@code search_after} and {@code collapse} need
     * coordination across shards that does not exist here.
     *
     * <p>Cheap to lift later, one at a time, and each will need its own merge and its own test. Until
     * then the honest answer to "can you do this" is no.
     *
     * @param source the parsed search
     * @return why it cannot be served, or null
     */
    public static String whatCannotBeMerged(SearchSourceBuilder source) {
        if (source.searchAfter() != null) {
            // The two things that make a cursor a cursor. Without a sort there is no order for "after" to
            // be after, and OpenSearch's own answer to search_after with from is the same refusal: the
            // cursor is the offset, and having both is a request that means two different things at once.
            if (source.sorts() == null || source.sorts().isEmpty()) {
                return "search_after needs a sort: without one there is no order for a cursor to be a position in";
            }
            if (source.from() > 0) {
                return "search_after and from cannot both be given: the cursor is the offset";
            }
            if (source.searchAfter().length != source.sorts().size()) {
                return "search_after must carry one value per sort key, and this carries "
                    + source.searchAfter().length
                    + " for "
                    + source.sorts().size();
            }
        }
        if (source.collapse() != null) {
            return "collapse is not supported: it would have to be applied across shards, not within one";
        }
        if (source.suggest() != null) {
            return "suggest is not supported";
        }
        if (source.profile()) {
            return "profile is not supported";
        }
        if (source.slice() != null) {
            // Refused here rather than left to fail on the shard, where Lucene's own message -- "slice
            // cannot be used outside of a scroll context or PIT context" -- names two things this surface
            // does not have in the terms core has them.
            return "slice is not supported: a sliced search partitions one scroll or point-in-time context, and a "
                + "search here runs over each shard's own reader rather than over a context it could partition";
        }
        if (source.indexBoosts() != null && source.indexBoosts().isEmpty() == false) {
            // The per-index boost is fixed at 1.0 where each shard's request is built, so accepting one
            // would rank a multi-index search as if it had not been given.
            return "indices_boost is not supported: every shard is queried at the same boost";
        }
        return null;
    }
}
