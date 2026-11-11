/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

/** What a whole search found, and how much of the index it managed to look at. */
public final class SearchOutcome {

    private final long total;
    private final java.util.List<org.opensearch.search.SearchHit> hits;
    private final int shards;
    private final int answered;
    private final org.opensearch.search.aggregations.InternalAggregations aggregations;
    private final org.apache.lucene.search.TotalHits.Relation relation;
    private final float maxScore;
    private final boolean timedOut;
    private final Boolean terminatedEarly;
    private final java.util.List<org.opensearch.action.search.ShardSearchFailure> failures;

    /**
     * Creates the outcome.
     *
     * @param total how many matched
     * @param hits the merged page
     * @param shards how many shards the index has
     * @param answered how many answered
     */
    public SearchOutcome(long total, java.util.List<org.opensearch.search.SearchHit> hits, int shards, int answered) {
        this(total, hits, shards, answered, null);
    }

    /**
     * Creates the outcome, with the reduced aggregations.
     *
     * @param total how many matched
     * @param hits the merged page
     * @param shards how many shards the index has
     * @param answered how many answered
     * @param aggregations the combined aggregations, or null if none were asked for
     */
    public SearchOutcome(
        long total,
        java.util.List<org.opensearch.search.SearchHit> hits,
        int shards,
        int answered,
        org.opensearch.search.aggregations.InternalAggregations aggregations
    ) {
        this(
            total,
            hits,
            shards,
            answered,
            aggregations,
            org.apache.lucene.search.TotalHits.Relation.EQUAL_TO,
            Float.NaN,
            false,
            null,
            java.util.List.of()
        );
    }

    /**
     * Creates the outcome with everything a real search response reports about how it went.
     *
     * <p>The first five are what the answer is; the rest are how much to trust it. A shard that
     * timed out, or stopped counting past a ceiling, or could not be reached and why, all used to
     * vanish between the fan-out and the response, which then stated {@code timed_out: false} and
     * {@code relation: eq} as constants and reported a failed shard as a count with no cause.
     *
     * @param total how many matched, summed over the shards that answered
     * @param hits the merged page
     * @param shards how many shards were asked
     * @param answered how many answered
     * @param aggregations the combined aggregations, or null if none were asked for
     * @param relation whether {@code total} is exact or a lower bound
     * @param maxScore the best score over every shard's top docs, or NaN when unscored
     * @param timedOut whether any shard hit the search timeout
     * @param terminatedEarly whether {@code terminate_after} stopped any shard, or null if not asked
     * @param failures why each unanswered shard did not answer
     */
    public SearchOutcome(
        long total,
        java.util.List<org.opensearch.search.SearchHit> hits,
        int shards,
        int answered,
        org.opensearch.search.aggregations.InternalAggregations aggregations,
        org.apache.lucene.search.TotalHits.Relation relation,
        float maxScore,
        boolean timedOut,
        Boolean terminatedEarly,
        java.util.List<org.opensearch.action.search.ShardSearchFailure> failures
    ) {
        this.total = total;
        this.hits = hits;
        this.shards = shards;
        this.answered = answered;
        this.aggregations = aggregations;
        this.relation = relation;
        this.maxScore = maxScore;
        this.timedOut = timedOut;
        this.terminatedEarly = terminatedEarly;
        this.failures = java.util.List.copyOf(failures);
    }

    private int skipped;
    private Boolean totalHitsTracked;

    /**
     * Records whether the admitted request or filtered response includes total hits.
     *
     * @param tracked whether a count may be returned
     * @return this outcome
     */
    public SearchOutcome withTotalHitsTracking(boolean tracked) {
        totalHitsTracked = tracked;
        return this;
    }

    /**
     * Returns the admitted counting policy, or null when the caller's source supplies it.
     *
     * @return whether total hits are included, or null if unspecified
     */
    public Boolean tracksTotalHits() {
        return totalHitsTracked;
    }

    /**
     * Records how many shards were skipped because they provably could not match.
     *
     * @param skipped the count, already included in {@link #answered()}
     * @return this outcome
     */
    public SearchOutcome withSkipped(int skipped) {
        this.skipped = skipped;
        return this;
    }

    /**
     * Returns how many shards were skipped because their pruning digest ruled the query out.
     *
     * @return the count, which {@link #answered()} includes
     */
    public int skipped() {
        return skipped;
    }

    /**
     * Returns whether {@link #total()} is exact or a lower bound.
     *
     * @return the relation
     */
    public org.apache.lucene.search.TotalHits.Relation relation() {
        return relation;
    }

    /**
     * Returns the best score over every shard that answered.
     *
     * @return the score, or NaN when the query was not scored
     */
    public float maxScore() {
        return maxScore;
    }

    /**
     * Returns whether any shard hit the search timeout.
     *
     * @return true if one did
     */
    public boolean timedOut() {
        return timedOut;
    }

    /**
     * Returns whether {@code terminate_after} stopped any shard.
     *
     * @return true or false when it was asked for, null when it was not
     */
    public Boolean terminatedEarly() {
        return terminatedEarly;
    }

    /**
     * Returns why each shard that did not answer did not answer.
     *
     * @return one failure per unanswered shard, in shard order
     */
    public java.util.List<org.opensearch.action.search.ShardSearchFailure> failures() {
        return failures;
    }

    /**
     * Returns the aggregations, already combined across shards.
     *
     * @return the aggregations, or null
     */
    public org.opensearch.search.aggregations.InternalAggregations aggregations() {
        return aggregations;
    }

    /**
     * Returns how many documents matched, summed over the shards that answered.
     *
     * @return the total
     */
    public long total() {
        return total;
    }

    /**
     * Returns the page of hits, merged across shards and cut to the requested window.
     *
     * @return the hits
     */
    public java.util.List<org.opensearch.search.SearchHit> hits() {
        return hits;
    }

    /**
     * Returns how many shards the index has.
     *
     * @return the shard count
     */
    public int shards() {
        return shards;
    }

    /**
     * Returns how many of them answered.
     *
     * @return the answered count
     */
    public int answered() {
        return answered;
    }

    /**
     * Reports whether every shard answered.
     *
     * <p>Stated rather than implied: a caller reading only {@link #total()} has no way to tell a
     * complete answer from one computed over a fraction of the index.
     *
     * @return true when the whole index was searched
     */
    public boolean complete() {
        return answered == shards;
    }
}
