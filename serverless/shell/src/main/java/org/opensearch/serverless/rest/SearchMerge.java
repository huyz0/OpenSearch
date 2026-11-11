/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.shard.SearchOutcome;
import org.opensearch.serverless.shell.ServerlessNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Merges the shards' answers to one search: hits ordered across shards by score or sort values, aggregations
 * reduced, and each answer charged to the request breaker as it arrives. Split out of {@link SearchFanout}.
 */
final class SearchMerge {

    private SearchMerge() {}

    /**
     * Charges each shard's answer to the request breaker as it arrives, before the fan-out keeps it.
     *
     * <p><b>In the task, not in the merge.</b> The merge used to charge every answer after all of them were
     * already resident, so on a two-hundred-shard search the breaker tripped on shard 150's bytes while
     * 149 windows sat in heap unaccounted for -- "a 429 rather than an out-of-memory error" held only if
     * the answers happened to fit first. Charged here, shard N is refused while N-1 are held, which is
     * the bound the breaker exists to give.
     *
     * <p>An answer that arrives after the fan-out has given up on it is released by the task itself: the
     * coordinator can no longer see it, and a charge nobody can release is a leak the breaker would carry
     * until the node restarts.
     */
    static final class Charged {
        final ServerlessNode serving;
        final List<SearchFanout.ShardAnswer> admitted = java.util.Collections.synchronizedList(new ArrayList<>());
        final java.util.concurrent.atomic.AtomicBoolean abandoned = new java.util.concurrent.atomic.AtomicBoolean();

        Charged(ServerlessNode serving) {
            this.serving = serving;
        }

        SearchFanout.ShardAnswer admit(SearchFanout.ShardAnswer answer) {
            if (abandoned.get()) {
                answer.release(serving);
                throw new IllegalStateException("the search gave up waiting before this shard answered");
            }
            final long hitBytes = ResponseMemory.hits(answer.hits);
            final long aggregationBytes = answer.aggregations == null ? 0L : ResponseMemory.of(answer.aggregations);
            final long bytes = hitBytes + aggregationBytes;
            SearchFanout.breaker(serving).addEstimateBytesAndMaybeBreak(bytes, "serverless_search_merge");
            answer.hitBytes = hitBytes;
            answer.aggregationBytes = aggregationBytes;
            answer.reserved.set(bytes);
            admitted.add(answer);
            if (abandoned.get()) {
                answer.release(serving);
                throw new IllegalStateException("the search gave up waiting before this shard answered");
            }
            return answer;
        }

        void abandon() {
            abandoned.set(true);
            for (SearchFanout.ShardAnswer answer : List.copyOf(admitted)) {
                answer.release(serving);
            }
        }
    }

    /** What the shards that answered add up to, before the window is cut. */
    static final class Merged {
        /** Bytes charged to the request breaker for what this holds, given back by {@link #release}. */
        long reserved;

        void release(ServerlessNode serving) {
            if (reserved > 0) {
                SearchFanout.breaker(serving).addWithoutBreaking(-reserved);
                reserved = 0;
            }
        }

        /** Re-counts the reservation against what the pile still holds, after a trim threw hits away. */
        void settle(ServerlessNode serving) {
            final long retained = hitBytes + aggregationBytes;
            if (retained < reserved) {
                SearchFanout.breaker(serving).addWithoutBreaking(retained - reserved);
                reserved = retained;
            }
        }

        long total;
        long hitBytes;
        long aggregationBytes;
        int answered;
        org.apache.lucene.search.TotalHits.Relation relation = org.apache.lucene.search.TotalHits.Relation.EQUAL_TO;
        float maxScore = Float.NaN;
        boolean timedOut;
        Boolean terminatedEarly;
        final List<SearchHit> hits = new ArrayList<>();
        final List<org.opensearch.search.aggregations.InternalAggregations> aggregations = new ArrayList<>();

        SearchOutcome outcome(
            ServerlessNode serving,
            SearchSourceBuilder source,
            int shards,
            int from,
            int size,
            List<org.opensearch.action.search.ShardSearchFailure> failures
        ) {
            hits.sort(order(source));
            final List<SearchHit> page = hits.stream().skip(from).limit(size).collect(java.util.stream.Collectors.toList());
            // In shard order rather than in the order the failures happened to be recorded, so two runs of
            // the same broken search report the same list.
            final List<org.opensearch.action.search.ShardSearchFailure> ordered = new ArrayList<>(failures);
            ordered.sort(
                java.util.Comparator.comparing((org.opensearch.action.search.ShardSearchFailure f) -> f.index() == null ? "" : f.index())
                    .thenComparingInt(org.opensearch.action.search.ShardSearchFailure::shardId)
            );
            return new SearchOutcome(
                ceiling(total, relation, source),
                page,
                shards,
                answered,
                reduce(serving, source, aggregations),
                relation,
                maxScore,
                timedOut,
                terminatedEarly,
                ordered
            );
        }
    }

    /**
     * Adds up what the shards answered, the way a classic coordinator's reduce does.
     *
     * <p>A total is a sum, and it is exact only while every shard's was; a search that stopped counting on
     * one shard has a lower bound, not a count. {@code timed_out} is true if any shard ran out of time.
     * {@code terminated_early} is null unless some shard was asked, and true if any of those stopped
     * early. The best score is the best over every shard's own best -- computed over everything each
     * shard looked at, not over the page that survived the cut, which is what real OpenSearch's
     * {@code max_score} means.
     */
    static Merged merge(ServerlessNode serving, List<SearchFanout.ShardAnswer> answers, SearchSourceBuilder source) {
        final Merged merged = new Merged();
        // The window every shard was asked for. The merge keeps at most twice that many hits at any moment:
        // once the pile is that deep it is sorted and cut back to the window, so a wide index costs the
        // coordinator two windows of hits rather than a window per shard -- and the breaker is told, since
        // a reservation that kept counting the hits a trim threw away was a bound on nothing.
        final int window = Math.max(1, Math.max(0, source.from()) + Math.max(0, source.size()));
        try {
            final java.util.Comparator<SearchHit> order = order(source);
            for (SearchFanout.ShardAnswer answer : answers) {
                if (answer == null) {
                    continue;
                }
                // Already charged to core's request breaker by the task that produced it, so a reply too
                // large for this node was a 429 before it was kept; the reservation moves to the pile here
                // and is given back when the pile is.
                merged.reserved += answer.reserved.getAndSet(0L);
                merged.hitBytes += answer.hitBytes;
                merged.aggregationBytes += answer.aggregationBytes;
                merged.hits.addAll(answer.hits);
                // The fan-out and its admission tracker still retain the answer object. Transfer the
                // payload too, so trimming the merge really releases the references being uncharged.
                answer.hits = List.of();
                if (merged.hits.size() > 2 * window) {
                    merged.hits.sort(order);
                    final var discarded = merged.hits.subList(window, merged.hits.size());
                    // Each discarded hit is visited at most once more. Retained large fields and
                    // aggregation trees are not walked again for every incoming shard.
                    merged.hitBytes -= ResponseMemory.hits(discarded);
                    discarded.clear();
                    merged.settle(serving);
                }
                absorbInto(merged, answer);
            }
        } catch (RuntimeException | Error e) {
            merged.release(serving);
            // A failure partway through a merge must also release answers not transferred yet.
            for (SearchFanout.ShardAnswer answer : answers) {
                if (answer != null) {
                    answer.release(serving);
                }
            }
            throw e;
        }
        return merged;
    }

    /**
     * Caps a lower-bound total at what the caller asked to count, the way core's reduce does.
     *
     * <p>Each shard stops counting at {@code track_total_hits} and reports "at least this many"; the sum
     * of those floors is still a floor, but a caller who asked to count to a hundred and reads "at least
     * three hundred" has been told more than they asked for and less than the truth, and a client that
     * renders "more than N" from the value it sent is surprised. Exact totals are left alone.
     */
    static long ceiling(long total, org.apache.lucene.search.TotalHits.Relation relation, SearchSourceBuilder source) {
        if (relation != org.apache.lucene.search.TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO) {
            return total;
        }
        final int upTo = source.trackTotalHitsUpTo() == null
            ? org.opensearch.search.internal.SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO
            : source.trackTotalHitsUpTo();
        if (upTo <= 0 || upTo == org.opensearch.search.internal.SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
            return total;
        }
        return Math.min(total, upTo);
    }

    /** Folds everything but the hits of one shard's answer into the running total. */
    static void absorbInto(Merged merged, SearchFanout.ShardAnswer answer) {
        merged.total += answer.total;
        if (answer.aggregations != null) {
            merged.aggregations.add(answer.aggregations);
            answer.aggregations = null;
        }
        if (answer.relation == org.apache.lucene.search.TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO) {
            merged.relation = org.apache.lucene.search.TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
        }
        if (Float.isNaN(answer.maxScore) == false && (Float.isNaN(merged.maxScore) || answer.maxScore > merged.maxScore)) {
            merged.maxScore = answer.maxScore;
        }
        merged.timedOut |= answer.timedOut;
        if (answer.terminatedEarly != null) {
            merged.terminatedEarly = merged.terminatedEarly == null
                ? answer.terminatedEarly
                : merged.terminatedEarly || answer.terminatedEarly;
        }
        merged.answered++;
    }

    /**
     * Combines what each shard counted into one answer.
     *
     * <p><b>This is the reduce phase, and it is core's own.</b> A shard's terms aggregation holds that
     * shard's top terms and that shard's counts; adding them is not summing numbers but merging ordered
     * buckets, deciding which terms survive, and propagating the error bounds that say how wrong the result
     * might be. {@code InternalAggregations#topLevelReduce} is the code a classic node runs for exactly
     * this, and writing a second one would have been a promise to keep it correct forever.
     *
     * <p><b>Reduced once, finally, rather than in stages.</b> A classic coordinator reduces in batches as
     * shard results arrive, to bound memory. This fan-out already holds every shard's answer before it
     * merges hits, so a partial reduction would save nothing and add a state machine — and the shard counts
     * are bounded by the fan-out's own concurrency rather than by cluster size.
     *
     * <p>Pipeline aggregations come from the request's own tree, so a pipeline the caller asked for runs
     * rather than silently disappearing.
     *
     * @param serving the coordinating node
     * @param source what the client asked for
     * @param perShard each shard's unreduced aggregations
     * @return the combined aggregations, or null when none were asked for
     */
    static org.opensearch.search.aggregations.InternalAggregations reduce(
        ServerlessNode serving,
        SearchSourceBuilder source,
        List<org.opensearch.search.aggregations.InternalAggregations> perShard
    ) {
        if (perShard.isEmpty()) {
            return null;
        }
        final org.opensearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree pipelines = source.aggregations() == null
            ? org.opensearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree.EMPTY
            : source.aggregations().buildPipelineTree();
        return org.opensearch.search.aggregations.InternalAggregations.topLevelReduce(
            perShard,
            org.opensearch.search.aggregations.InternalAggregation.ReduceContext.forFinalReduction(
                serving.bigArrays(),
                serving.scriptService(),
                // Core's bucket bound and core's breaker: a reduce that would build more buckets than
                // search.max_buckets allows is refused, where a no-op consumer let it build them all.
                new org.opensearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer(
                    org.opensearch.search.aggregations.MultiBucketConsumerService.MAX_BUCKET_SETTING.get(serving.settings()),
                    SearchFanout.breaker(serving)
                ),
                pipelines
            )
        );
    }

    /**
     * How hits from different shards are ranked against each other.
     *
     * <p>By score unless the client asked for a sort, in which case by the sort keys each shard attached to
     * its hits. That is the whole of what "sort is not supported" used to mean: every shard could already
     * sort itself — the query goes through the same {@code SearchService} a classic node uses — and the
     * missing piece was a comparator here. Merging sorted shards by score would have returned the right
     * documents in the wrong order, which is worse than refusing.
     *
     * @param source what the client asked for
     * @return the comparator to merge with
     */
    static java.util.Comparator<SearchHit> order(SearchSourceBuilder source) {
        if (source.sorts() == null || source.sorts().isEmpty()) {
            // Descending by score. An unscored hit sorts last rather than unpredictably: a shard that
            // returned hits with no score should not be able to take the top of the page from one that
            // scored them.
            return (a, b) -> Float.compare(score(b), score(a));
        }
        // Which way round each key runs comes from the request, not from the hits: a hit carries its sort
        // values and no idea whether smaller means earlier.
        final boolean[] descending = new boolean[source.sorts().size()];
        final boolean[] missingFirst = new boolean[source.sorts().size()];
        for (int i = 0; i < descending.length; i++) {
            descending[i] = source.sorts().get(i).order() == org.opensearch.search.sort.SortOrder.DESC;
            // Where a document without the field goes, which core decides independently of direction:
            // missing last unless the sort says _first. The merge used to negate the null placement along
            // with the comparison, so a descending keyword sort put missing documents first.
            missingFirst[i] = source.sorts().get(i) instanceof org.opensearch.search.sort.FieldSortBuilder field
                && "_first".equals(field.missing());
        }
        return (a, b) -> compareSortValues(a, b, descending, missingFirst);
    }

    /**
     * Compares two hits on their sort keys, in order, until one differs.
     *
     * <p><b>A hit with no sort values sorts last</b>, for the same reason an unscored one does: a shard
     * that answered without them must not be able to take the top of the page from one that did. That can
     * happen where it should not — a shard running an older build, a field missing from one shard's mapping
     * — and the failure it prevents is a page that silently begins in the wrong place.
     */
    static int compareSortValues(SearchHit a, SearchHit b, boolean[] descending, boolean[] missingFirst) {
        final Object[] left = a.getRawSortValues();
        final Object[] right = b.getRawSortValues();
        if (left == null || left.length == 0) {
            return (right == null || right.length == 0) ? 0 : 1;
        }
        if (right == null || right.length == 0) {
            return -1;
        }
        for (int i = 0; i < left.length && i < right.length; i++) {
            if (left[i] == null || right[i] == null) {
                if (left[i] == null && right[i] == null) {
                    continue;
                }
                // Placed by the sort's own missing rule, not by direction.
                final boolean first = i < missingFirst.length && missingFirst[i];
                return (left[i] == null) == first ? -1 : 1;
            }
            final int comparison = compareOne(left[i], right[i]);
            if (comparison != 0) {
                return i < descending.length && descending[i] ? -comparison : comparison;
            }
        }
        return 0;
    }

    /**
     * Compares one pair of sort values.
     *
     * <p>They arrive as whatever Lucene sorted on — a {@code Long} for a date or a number, a
     * {@code BytesRef} for a keyword — and both sides of a comparison are the same kind because they came
     * from the same field. A missing value sorts last, which is Lucene's own convention for one.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    static int compareOne(Object left, Object right) {
        if (left == null) {
            return right == null ? 0 : 1;
        }
        if (right == null) {
            return -1;
        }
        if (left instanceof Comparable comparable && left.getClass() == right.getClass()) {
            return comparable.compareTo(right);
        }
        // A common representation is required for a valid merge. String ordering invents an order
        // for incompatible fields, and coercing longs to doubles can lose integer precision.
        throw new IllegalArgumentException(
            "cannot merge incompatible sort values across indices; use consistent field mappings or numeric_type for numeric sorts"
        );
    }

    /** An unscored hit sorts last rather than unpredictably. */
    static float score(SearchHit hit) {
        return Float.isNaN(hit.getScore()) ? Float.NEGATIVE_INFINITY : hit.getScore();
    }
}
