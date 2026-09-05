/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shard.ShardOperations;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The fan-out: ask every shard of an index, then merge what comes back.
 *
 * <p>Lifted out of {@code SearchHandler} so that a plugin's {@code Client} runs the same search a user
 * does. A search that fanned out one way for REST and another for a plugin would be two search engines
 * sharing a name, and the second one would be the untested one.
 *
 * <p>Lives in {@code rest} rather than {@code shard} only because that is where its collaborators already
 * are; {@link ShardOperations#search} is the entry point callers should use.
 */
public final class SearchFanout {

    private SearchFanout() {}

    /**
     * Runs one search across every shard and merges the answers.
     *
     * @param serving the node coordinating the search
     * @param metadata the metadata plane
     * @param index the index
     * @param source the query, with its own from and size
     * @return what was found and how completely
     * @throws IOException if the search fails outright
     */
    public static ShardOperations.SearchOutcome run(
        ServerlessNode serving,
        MetadataPlane metadata,
        org.opensearch.serverless.cluster.IndexDescriptor index,
        SearchSourceBuilder source
    ) throws IOException {
        return run(serving, metadata, java.util.Map.of(index.name(), index), source);
    }

    /**
     * Runs one search across every shard of several indices and merges the answers.
     *
     * <p><b>One fan-out, not one per index.</b> Searching three indices of two shards each is six shards
     * asked at once, not three searches run in sequence and stitched — which is the difference between a
     * latency of the slowest shard and a latency of the sum. It also means the window is cut once, over
     * everything, so page two of a search over three indices is the same page two it would be over one.
     *
     * <p>Coverage is reported over the whole set: {@code _shards.total} is every shard of every index
     * asked for, so a caller can still tell a complete answer from one computed over part of it, which is
     * the only reason those numbers are in the response.
     *
     * @param serving the node coordinating the search
     * @param metadata the metadata plane
     * @param indices each index and how many shards it has
     * @param source the query, with its own from and size
     * @return what was found and how completely
     * @throws IOException if the search fails outright
     */
    /**
     * Runs one search over a frozen view rather than over whatever the shards hold now.
     *
     * <p><b>Fans out by placement now, the same as a live search.</b> Every shard used to be opened
     * unconditionally on the coordinating node, which meant a wide view's whole cost -- heap, file
     * descriptors, the shard cap itself -- landed on whichever node happened to answer the HTTP request,
     * and a second coordinator (the next page, on a load balancer with no session affinity) paid it again
     * from nothing. This now tries the shard locally, then the {@link org.opensearch.serverless.cluster
     * .ReaderPlacement}-preferred peer over the wire, and only opens it here itself if neither answers --
     * the identical hint-with-fallback shape {@link #askOneShard} already uses for a live shard, keyed the
     * same way (by index and shard number) so a view's placement tends to agree with its live shard's,
     * where whatever segment files the two share are more likely already cached.
     *
     * <p>Nothing here is required to be right. A stale or unlucky placement decision costs a network hop
     * or a cold open, never a wrong or missing answer -- the same promise {@code ReaderPlacement}'s own
     * javadoc makes, extended to this path rather than reinvented for it.
     *
     * @param serving the node coordinating the search
     * @param metadata the metadata plane
     * @param pit the frozen view
     * @param source the query, with its own from and size
     * @return what was found and how completely
     * @throws IOException if the search fails outright
     */
    public static ShardOperations.SearchOutcome runFrozen(
        ServerlessNode serving,
        MetadataPlane metadata,
        org.opensearch.serverless.metadata.PointInTime pit,
        SearchSourceBuilder source
    ) throws IOException {
        // The same defaults every entry point applies, applied here so that none can forget: a source
        // whose size was never set used to ask each shard for zero hits and answer "total 12, hits []".
        SearchHandler.applyDefaults(source, false);
        final int from = Math.max(0, source.from());
        final int size = Math.max(0, source.size());
        final SearchSourceBuilder perShard = source.shallowCopy();
        perShard.from(0);
        perShard.size(from + size);

        // The same reason the live fan-out keeps its first failure: a view that cannot be opened must say
        // why. Without this the response was "no shard could be opened" with nothing behind it, which is
        // the shape of an answer and none of the content.
        final java.util.concurrent.atomic.AtomicReference<Exception> firstFailure = new java.util.concurrent.atomic.AtomicReference<>();
        final List<org.opensearch.action.search.ShardSearchFailure> failures = java.util.Collections.synchronizedList(new ArrayList<>());
        final Charged charged = new Charged(serving);
        // One instant for every shard of this request, as core fixes one absoluteStartMillis on the
        // coordinator: each shard used to take its own clock, so "now-1s" meant a different boundary on
        // each side of a chunk or a node.
        final long nowInMillis = System.currentTimeMillis();
        final List<java.util.concurrent.Callable<ShardAnswer>> tasks = new ArrayList<>();
        for (Integer shard : pit.shards().keySet()) {
            tasks.add(() -> {
                try {
                    return charged.admit(askOneFrozenShard(serving, metadata, pit, shard, perShard, nowInMillis));
                } catch (Exception e) {
                    firstFailure.compareAndSet(null, e);
                    failures.add(failure(pit.index(), shard, e));
                    throw e;
                }
            });
        }
        final List<ShardAnswer> answers;
        try {
            answers = Fanout.run(serving.threadPool().executor(ThreadPool.Names.GENERIC), Fanout.DEFAULT_CONCURRENCY, tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            charged.abandon();
            throw new IOException("interrupted while searching the point in time " + pit.id(), e);
        }
        final Merged merged = merge(serving, answers, source);
        try {
            if (merged.answered == 0) {
                final Exception cause = firstFailure.get();
                if (cause instanceof RuntimeException runtime) {
                    throw runtime;
                }
                throw new IOException("no shard of the point in time " + pit.id() + " could be opened", cause);
            }
            return merged.outcome(serving, source, pit.shards().size(), from, size, failures);
        } finally {
            merged.release(serving);
        }
    }

    public static ShardOperations.SearchOutcome run(
        ServerlessNode serving,
        MetadataPlane metadata,
        java.util.Map<String, org.opensearch.serverless.cluster.IndexDescriptor> indices,
        SearchSourceBuilder source
    ) throws IOException {
        // Every entry point's defaults, in the one place every entry point passes through. The REST
        // handlers and the plugin client each applied them; a caller holding the operations object
        // directly did not, and an unset size became a request for zero hits.
        SearchHandler.applyDefaults(source, false);
        final int from = Math.max(0, source.from());
        final int size = Math.max(0, source.size());
        // What each shard is asked for. A shard cannot know how its hits rank against another's, so it
        // has to offer enough to cover the whole window on its own.
        //
        // A cursored search needs nothing special here, and a branch for it was written and deleted: every
        // hit past a cursor is already past it, so a shard need only offer its own next page -- and since
        // search_after and from are refused together, from is zero and from + size is already exactly that.
        // The canary for the branch could not fail, which is how it was found to be doing nothing.
        final SearchSourceBuilder perShard = source.shallowCopy();
        perShard.from(0);
        perShard.size(from + size);

        // Every shard at once, up to a bound. This loop used to run the shards one after another, so a
        // query's latency was the sum of its shards rather than the slowest of them -- and the shape of
        // the loop was the only reason. Each task answers for exactly one shard and swallows nothing: a
        // shard that cannot be reached comes back null and shows up in the coverage this already reports.
        int shards = 0;
        // Why a shard did not answer, kept rather than only logged. A fan-out treats an unanswered shard as
        // a hole in the coverage it reports, which is right while some other shard did answer. When none
        // did there is no answer to report coverage over, and "0 hits" with a flag beside it is exactly the
        // confident empty answer this surface exists to avoid -- so the first failure is carried out and
        // becomes the response.
        final java.util.concurrent.atomic.AtomicReference<Exception> firstFailure = new java.util.concurrent.atomic.AtomicReference<>();
        // Why each shard that did not answer did not answer, kept for the response rather than only for
        // the log. _shards.failed used to be a bare count: a caller could see that a shard was missing
        // from the answer and had no way to learn whether it was a circuit breaker, a peer that timed out
        // or a shard nobody is serving -- which are three different things to do next.
        final List<org.opensearch.action.search.ShardSearchFailure> failures = java.util.Collections.synchronizedList(new ArrayList<>());
        final Charged charged = new Charged(serving);
        // One instant for every shard of this request; see runFrozen.
        final long nowInMillis = System.currentTimeMillis();
        final List<java.util.concurrent.Callable<ShardAnswer>> tasks = new ArrayList<>();
        for (java.util.Map.Entry<String, org.opensearch.serverless.cluster.IndexDescriptor> index : indices.entrySet()) {
            for (int shard = 0; shard < index.getValue().numberOfShards(); shard++) {
                final int number = shard;
                final String name = index.getKey();
                final org.opensearch.serverless.cluster.IndexDescriptor descriptor = index.getValue();
                tasks.add(() -> {
                    try {
                        final ShardAnswer answer = askOneShard(serving, metadata, descriptor, number, perShard, nowInMillis);
                        if (answer == null) {
                            failures.add(
                                failure(
                                    name,
                                    number,
                                    new org.opensearch.action.NoShardAvailableActionException(
                                        new ShardId(new org.opensearch.core.index.Index(name, "_na_"), number),
                                        "no node is serving shard " + number + " of " + name
                                    )
                                )
                            );
                            return null;
                        }
                        return charged.admit(answer);
                    } catch (Exception e) {
                        firstFailure.compareAndSet(null, e);
                        failures.add(failure(name, number, e));
                        throw e;
                    }
                });
                shards++;
            }
        }
        final List<ShardAnswer> answers;
        try {
            // GENERIC, and no longer the fan-out pool, for the peers' sake rather than this node's. The
            // fan-out pool serves the searches other nodes forward here; a task that forwards blocks its
            // thread until the peer answers, and two nodes whose fan-out pools were full of threads waiting
            // on each other had nothing left to serve each other with -- a deadlock resolved only by the
            // forward timeout. Outgoing waits now live on GENERIC, incoming work on the fan-out pool, and
            // Fanout's caller-runs rule keeps a full GENERIC from parking the coordinator behind itself.
            answers = Fanout.run(serving.threadPool().executor(ThreadPool.Names.GENERIC), Fanout.DEFAULT_CONCURRENCY, tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            charged.abandon();
            throw new IOException("interrupted while searching " + indices.keySet(), e);
        }

        final Merged merged = merge(serving, answers, source);
        try {
            if (merged.answered == 0 && shards > 0) {
                final Exception cause = firstFailure.get();
                if (cause instanceof RuntimeException runtime) {
                    // Rethrown as itself, so a circuit-breaking exception still answers 429 and a security
                    // refusal still answers 403 rather than every failure collapsing into one status.
                    throw runtime;
                }
                if (cause != null) {
                    throw new IOException("no shard of " + indices.keySet() + " could answer this search", cause);
                }
                throw new IOException("no shard of " + indices.keySet() + " could answer this search; no node is serving them");
            }
            return merged.outcome(serving, source, shards, from, size, failures);
        } finally {
            merged.release(serving);
        }
    }

    /** The breaker a coordinator's working set is charged to, which is core's request breaker. */
    private static org.opensearch.core.common.breaker.CircuitBreaker breaker(ServerlessNode serving) {
        return serving.circuitBreakerService().getBreaker(org.opensearch.core.common.breaker.CircuitBreaker.REQUEST);
    }

    /** The bytes a hit holds on the coordinator: its id and its source, which is what a wide window costs. */
    private static long bytesOf(List<SearchHit> hits) {
        long bytes = 0L;
        for (SearchHit hit : hits) {
            bytes += (hit.getId() == null ? 0L : hit.getId().length()) + (hit.getSourceRef() == null ? 0L : hit.getSourceRef().length());
        }
        return bytes;
    }

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
    private static final class Charged {
        private final ServerlessNode serving;
        private final List<ShardAnswer> admitted = java.util.Collections.synchronizedList(new ArrayList<>());
        private final java.util.concurrent.atomic.AtomicBoolean abandoned = new java.util.concurrent.atomic.AtomicBoolean();

        Charged(ServerlessNode serving) {
            this.serving = serving;
        }

        ShardAnswer admit(ShardAnswer answer) {
            final long bytes = bytesOf(answer.hits);
            breaker(serving).addEstimateBytesAndMaybeBreak(bytes, "serverless_search_merge");
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
            for (ShardAnswer answer : List.copyOf(admitted)) {
                answer.release(serving);
            }
        }
    }

    /** What the shards that answered add up to, before the window is cut. */
    private static final class Merged {
        /** Bytes charged to the request breaker for what this holds, given back by {@link #release}. */
        private long reserved;

        private void release(ServerlessNode serving) {
            if (reserved > 0) {
                breaker(serving).addWithoutBreaking(-reserved);
                reserved = 0;
            }
        }

        /** Re-counts the reservation against what the pile still holds, after a trim threw hits away. */
        private void settle(ServerlessNode serving) {
            final long retained = bytesOf(hits);
            if (retained < reserved) {
                breaker(serving).addWithoutBreaking(retained - reserved);
                reserved = retained;
            }
        }

        private long total;
        private int answered;
        private org.apache.lucene.search.TotalHits.Relation relation = org.apache.lucene.search.TotalHits.Relation.EQUAL_TO;
        private float maxScore = Float.NaN;
        private boolean timedOut;
        private Boolean terminatedEarly;
        private final List<SearchHit> hits = new ArrayList<>();
        private final List<org.opensearch.search.aggregations.InternalAggregations> aggregations = new ArrayList<>();

        private ShardOperations.SearchOutcome outcome(
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
            return new ShardOperations.SearchOutcome(
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
    private static Merged merge(ServerlessNode serving, List<ShardAnswer> answers, SearchSourceBuilder source) {
        final Merged merged = new Merged();
        // The window every shard was asked for. The merge keeps at most twice that many hits at any moment:
        // once the pile is that deep it is sorted and cut back to the window, so a wide index costs the
        // coordinator two windows of hits rather than a window per shard -- and the breaker is told, since
        // a reservation that kept counting the hits a trim threw away was a bound on nothing.
        final int window = Math.max(1, Math.max(0, source.from()) + Math.max(0, source.size()));
        final java.util.Comparator<SearchHit> order = order(source);
        try {
            for (ShardAnswer answer : answers) {
                if (answer == null) {
                    continue;
                }
                // Already charged to core's request breaker by the task that produced it, so a reply too
                // large for this node was a 429 before it was kept; the reservation moves to the pile here
                // and is given back when the pile is.
                merged.reserved += answer.reserved.getAndSet(0L);
                merged.hits.addAll(answer.hits);
                if (merged.hits.size() > 2 * window) {
                    merged.hits.sort(order);
                    merged.hits.subList(window, merged.hits.size()).clear();
                    merged.settle(serving);
                }
                absorbInto(merged, answer);
            }
        } catch (RuntimeException e) {
            merged.release(serving);
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
    private static long ceiling(long total, org.apache.lucene.search.TotalHits.Relation relation, SearchSourceBuilder source) {
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
    private static void absorbInto(Merged merged, ShardAnswer answer) {
        merged.total += answer.total;
        if (answer.aggregations != null) {
            merged.aggregations.add(answer.aggregations);
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

    /** One shard's reason for not answering, addressed the way a real response addresses one. */
    private static org.opensearch.action.search.ShardSearchFailure failure(String index, int shard, Exception cause) {
        return new org.opensearch.action.search.ShardSearchFailure(
            cause,
            new org.opensearch.search.SearchShardTarget(
                null,
                new ShardId(new org.opensearch.core.index.Index(index, "_na_"), shard),
                null,
                org.opensearch.action.OriginalIndices.NONE
            )
        );
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
    private static org.opensearch.search.aggregations.InternalAggregations reduce(
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
                    breaker(serving)
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
    private static java.util.Comparator<SearchHit> order(SearchSourceBuilder source) {
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
    private static int compareSortValues(SearchHit a, SearchHit b, boolean[] descending, boolean[] missingFirst) {
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
    private static int compareOne(Object left, Object right) {
        if (left == null) {
            return right == null ? 0 : 1;
        }
        if (right == null) {
            return -1;
        }
        if (left instanceof Comparable comparable && left.getClass() == right.getClass()) {
            return comparable.compareTo(right);
        }
        if (left instanceof Number l && right instanceof Number r) {
            // A long from one index and a double from another are still numbers, and comparing their
            // rendered forms put "10" before "9.5".
            return Double.compare(l.doubleValue(), r.doubleValue());
        }
        // Different types for the same key means the shards disagree about the field. Ordering them by
        // their rendered form is arbitrary but stable, which beats an exception in the middle of a merge.
        return String.valueOf(left).compareTo(String.valueOf(right));
    }

    /** An unscored hit sorts last rather than unpredictably. */
    private static float score(SearchHit hit) {
        return Float.isNaN(hit.getScore()) ? Float.NEGATIVE_INFINITY : hit.getScore();
    }

    /** One shard's contribution: what it matched, and what it returned. */
    private static final class ShardAnswer {
        private final long total;
        private final List<SearchHit> hits;
        private final org.opensearch.search.aggregations.InternalAggregations aggregations;
        private final org.apache.lucene.search.TotalHits.Relation relation;
        private final float maxScore;
        private final boolean timedOut;
        private final Boolean terminatedEarly;
        /** Bytes this answer holds on the request breaker until the merge takes them over, or {@link #release}. */
        private final java.util.concurrent.atomic.AtomicLong reserved = new java.util.concurrent.atomic.AtomicLong();

        /** Gives back whatever this answer still holds; safe to call more than once. */
        private void release(ServerlessNode serving) {
            final long held = reserved.getAndSet(0L);
            if (held > 0) {
                breaker(serving).addWithoutBreaking(-held);
            }
        }

        ShardAnswer(org.opensearch.serverless.shard.ShardQuery.Result result) {
            this(
                result.total(),
                result.hits(),
                result.aggregations(),
                result.relation(),
                result.maxScore(),
                result.timedOut(),
                result.terminatedEarly()
            );
        }

        ShardAnswer(org.opensearch.serverless.transport.ForwardedSearchResponse answer) {
            this(
                answer.total(),
                answer.hits(),
                answer.aggregations(),
                answer.relation(),
                answer.maxScore(),
                answer.timedOut(),
                answer.terminatedEarly()
            );
        }

        private ShardAnswer(
            long total,
            List<SearchHit> hits,
            org.opensearch.search.aggregations.InternalAggregations aggregations,
            org.apache.lucene.search.TotalHits.Relation relation,
            float maxScore,
            boolean timedOut,
            Boolean terminatedEarly
        ) {
            this.total = total;
            this.hits = hits;
            this.aggregations = aggregations;
            this.relation = relation;
            this.maxScore = maxScore;
            this.timedOut = timedOut;
            this.terminatedEarly = terminatedEarly;
        }
    }

    private static final org.apache.logging.log4j.Logger LOG = org.apache.logging.log4j.LogManager.getLogger(SearchFanout.class);

    /**
     * Answers for exactly one shard, wherever it happens to live.
     *
     * <p>Lifted out of the fan-out loop unchanged in behaviour: serve it here if it is open here,
     * otherwise pick a reader by placement, otherwise fall back to the shard's owner.
     *
     * @param serving the node running the search
     * @param metadata the metadata plane
     * @param descriptor the index
     * @param shard the shard number
     * @param perShard the query, sized to cover the whole window on its own
     * @param nowInMillis the request's one instant, for every shard's "now"
     * @return what the shard answered, or null if no candidate could answer for it
     * @throws IOException if the shard is here and querying it fails
     */
    private static ShardAnswer askOneShard(
        ServerlessNode serving,
        MetadataPlane metadata,
        org.opensearch.serverless.cluster.IndexDescriptor descriptor,
        int shard,
        SearchSourceBuilder perShard,
        long nowInMillis
    ) throws IOException {
        final String index = descriptor.name();
        Exception lastFailure = null;
        final ShardId local = localShard(serving, descriptor, shard);
        if (local != null) {
            serving.markUsed(local);
            try {
                return new ShardAnswer(queryHere(serving, local, perShard, nowInMillis));
            } catch (Exception e) {
                if (copyUnusable(e) == false) {
                    // The query's own failure, not this copy's: a script that does not compile, a breaker
                    // that refused the aggregation. Another copy would answer the same way, and retrying
                    // it here -- through the owner, which on one node is this node -- turned a 400 and a
                    // 429 into "no shard could answer", a 500. Thrown as itself, so it keeps its status.
                    throw e;
                }
                // The local copy is not the only copy. A reader closed under a running query -- a pass
                // found its commit superseded and released it -- used to be this shard's final answer,
                // with a peer that could have served it never asked. It is now the first attempt, not
                // the only one; if nobody else can answer either, its reason is still the one reported.
                LOG.warn("shard " + shard + " of " + index + " was open here and could not answer; trying elsewhere", e);
                lastFailure = e;
            }
        }
        // Not here, or not answerable here. Choose a reader by placement -- NOT the shard's owner, which
        // is the writer: routing searches to writers would couple search capacity to write capacity and
        // make per-index search scale-to-zero meaningless. Placement is a cache-affinity hint, so the
        // owner remains a last resort for the case where no search node exists at all.
        final List<String> targets = new ArrayList<>(
            org.opensearch.serverless.cluster.ReaderPlacement.candidatesFor(
                index,
                shard,
                metadata.membership().current(),
                ServerlessNode.ROLE_SEARCH,
                2
            )
        );
        // The owner is the last resort, and its head is read only when the placement candidates could
        // not answer: a head read per shard per search was the largest single cost of a hot search.
        boolean triedOwner = false;
        for (int attempt = 0; attempt <= targets.size(); attempt++) {
            final String target;
            if (attempt < targets.size()) {
                target = targets.get(attempt);
            } else if (triedOwner == false) {
                triedOwner = true;
                final String owner = metadata.heads().read(index, shard).map(h -> h.ownerNodeId()).orElse(null);
                if (owner == null || targets.contains(owner)) {
                    break;
                }
                target = owner;
            } else {
                break;
            }
            try {
                if (serving.localNode().getId().equals(target)) {
                    // We are the placement for this shard but do not hold it yet. Open it here rather
                    // than asking ourselves over the network.
                    final ShardId opened = serving.serveAsReader(metadata, index, shard);
                    serving.markUsed(opened);
                    return new ShardAnswer(queryHere(serving, opened, perShard, nowInMillis));
                }
                // The search bound, not the write bound: a peer that is merely busy should cost latency,
                // not coverage.
                final var peer = serving.router().peer(target, serving.router().searchForwardTimeout());
                if (peer.isEmpty()) {
                    continue;
                }
                return new ShardAnswer(
                    serving.router()
                        .forwardSearch(
                            peer.get(),
                            new org.opensearch.serverless.transport.ForwardedSearchRequest(
                                index,
                                shard,
                                perShard,
                                descriptor.uuid(),
                                nowInMillis
                            )
                        )
                );
            } catch (Exception e) {
                // Try the next candidate. A shard that failed to answer is not a shard with no matches,
                // so it only counts as searched if one of them succeeded.
                LOG.warn("shard " + shard + " of " + index + " was not served by " + target, e);
                lastFailure = e;
            }
        }
        if (lastFailure != null) {
            // Every candidate was tried and the last one's reason is the best account of why. Thrown
            // rather than swallowed into a null, so the response can carry it in _shards.failures instead
            // of a count with nothing behind it -- and thrown as itself, so a runtime failure keeps the
            // status it carries rather than arriving as an anonymous I/O error.
            if (lastFailure instanceof IOException io) {
                throw io;
            }
            if (lastFailure instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IOException(lastFailure);
        }
        return null;
    }

    /**
     * Whether a local query failed because this copy of the shard is gone, rather than because of the
     * query: a reader released under it, a shard closed or no longer here. Only those are worth asking
     * another copy about; everything else is the same answer everywhere.
     */
    private static boolean copyUnusable(Throwable e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof org.apache.lucene.store.AlreadyClosedException
                || cause instanceof org.opensearch.index.shard.IndexShardClosedException
                || cause instanceof org.opensearch.index.shard.ShardNotFoundException
                || cause instanceof org.opensearch.index.IndexNotFoundException) {
                return true;
            }
            if (cause == cause.getCause()) {
                break;
            }
        }
        return false;
    }

    private static ShardId localShard(ServerlessNode serving, org.opensearch.serverless.cluster.IndexDescriptor descriptor, int shard) {
        // By uuid as well as name: a shard of a deleted index of the same name is still open here until
        // the next heartbeat notices, and it must not answer for the recreated one.
        return serving.reconciler()
            .openShards()
            .stream()
            .filter(s -> s.getIndexName().equals(descriptor.name()) && s.id() == shard && s.getIndex().getUUID().equals(descriptor.uuid()))
            .findFirst()
            .orElse(null);
    }

    /**
     * Answers for exactly one shard of a frozen view, wherever it ends up being opened.
     *
     * <p>{@link #askOneShard}'s shape, not a fresh design: serve it here if this node already holds this
     * view's shard, otherwise pick a reader by {@code ReaderPlacement} and forward, otherwise open it here
     * as the fallback that guarantees the search still completes. The one real difference is what "here"
     * means when every placement candidate is unreachable -- a live shard falls back to its owner, the
     * writer; a view has no owner, only whoever has already opened it, so the fallback is the coordinator
     * opening it itself -- the same thing every call used to do unconditionally before this existed.
     *
     * @param serving the node coordinating the search
     * @param metadata the metadata plane
     * @param pit the frozen view
     * @param shard the shard number
     * @param perShard the query, sized to cover the whole window on its own
     * @param nowInMillis the request's one instant, for every shard's "now"
     * @return what the shard answered
     * @throws Exception if the shard could not be opened anywhere, local fallback included
     */
    private static ShardAnswer askOneFrozenShard(
        ServerlessNode serving,
        MetadataPlane metadata,
        org.opensearch.serverless.metadata.PointInTime pit,
        int shard,
        SearchSourceBuilder perShard,
        long nowInMillis
    ) throws Exception {
        final ShardId local = localFrozenView(serving, pit, shard);
        if (local != null) {
            return new ShardAnswer(queryHere(serving, local, perShard, nowInMillis));
        }
        // Keyed by the view's underlying index and shard, the same key live placement uses -- not by the
        // view id -- so a view's placement agrees with its live shard's where a node is likely to already
        // hold cached blobs for it, rather than scattering every distinct view of one index at random.
        final List<String> targets = org.opensearch.serverless.cluster.ReaderPlacement.candidatesFor(
            pit.index(),
            shard,
            metadata.membership().current(),
            ServerlessNode.ROLE_SEARCH,
            2
        );
        for (String target : targets) {
            try {
                if (serving.localNode().getId().equals(target)) {
                    // We are the placement for this shard but had not opened it yet. Open it here rather
                    // than forwarding to ourselves over the network.
                    final var shardId = serving.openFrozenView(metadata, pit, shard);
                    return new ShardAnswer(queryHere(serving, shardId, perShard, nowInMillis));
                }
                final var peer = serving.router().peer(target, serving.router().searchForwardTimeout());
                if (peer.isEmpty()) {
                    continue;
                }
                return new ShardAnswer(
                    serving.router()
                        .forwardFrozenSearch(
                            peer.get(),
                            new org.opensearch.serverless.transport.ForwardedFrozenSearchRequest(pit, shard, perShard, nowInMillis)
                        )
                );
            } catch (Exception e) {
                // Try the next candidate, same as the live path: a shard that failed to answer is not a
                // shard with no matches, so it only counts as searched once one candidate has succeeded.
                LOG.warn("shard " + shard + " of the point in time " + pit.id() + " was not served by " + target, e);
            }
        }
        // No candidate answered -- empty membership, everyone unreachable, or this node holds no search
        // role at all. Open it here: the unconditional fallback every call used before placement existed,
        // so a stale or unlucky routing decision costs this node a cold open and never a wrong answer.
        final var shardId = serving.openFrozenView(metadata, pit, shard);
        return new ShardAnswer(queryHere(serving, shardId, perShard, nowInMillis));
    }

    /**
     * Queries a shard this node holds, and tells the reconciler for as long as the query runs.
     *
     * <p>The in-flight count is what keeps a pass from releasing a reader under a running query: a shard
     * with a non-zero count is skipped by the release that would otherwise close it mid-search. The
     * request's one {@code now} travels with the query so every shard evaluates the same instant.
     */
    private static org.opensearch.serverless.shard.ShardQuery.Result queryHere(
        ServerlessNode serving,
        ShardId shardId,
        SearchSourceBuilder perShard,
        long nowInMillis
    ) throws IOException {
        serving.reconciler().enter(shardId);
        try {
            return org.opensearch.serverless.shard.ShardQuery.execute(serving.searchService(), shardId, perShard, nowInMillis);
        } finally {
            serving.reconciler().exit(shardId);
        }
    }

    private static ShardId localFrozenView(ServerlessNode serving, org.opensearch.serverless.metadata.PointInTime pit, int shard) {
        return serving.reconciler()
            .frozenShards()
            .stream()
            .filter(s -> s.getIndex().getUUID().equals(pit.id()) && s.id() == shard)
            .findFirst()
            .orElse(null);
    }
}
