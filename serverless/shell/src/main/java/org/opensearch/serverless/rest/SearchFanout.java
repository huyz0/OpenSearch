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
import org.opensearch.serverless.shard.SearchOutcome;
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
    public static SearchOutcome run(
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
     * the identical hint-with-fallback shape {@link SearchShardCalls#askOneShard} already uses for a live shard, keyed the
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
    public static SearchOutcome runFrozen(
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
        final SearchMerge.Charged charged = new SearchMerge.Charged(serving);
        // One instant for every shard of this request, as core fixes one absoluteStartMillis on the
        // coordinator: each shard used to take its own clock, so "now-1s" meant a different boundary on
        // each side of a chunk or a node.
        final long nowInMillis = System.currentTimeMillis();
        final List<java.util.concurrent.Callable<ShardAnswer>> tasks = new ArrayList<>();
        for (Integer shard : pit.shards().keySet()) {
            tasks.add(() -> {
                try {
                    return charged.admit(SearchShardCalls.askOneFrozenShard(serving, metadata, pit, shard, perShard, nowInMillis));
                } catch (Exception e) {
                    firstFailure.compareAndSet(null, e);
                    failures.add(failure(pit.index(), shard, e));
                    throw e;
                }
            });
        }
        final List<ShardAnswer> answers;
        try {
            answers = Fanout.run(
                serving.threadPool().executor(ThreadPool.Names.GENERIC),
                serving.searchSettings().searchFanoutConcurrency(),
                tasks
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            charged.abandon();
            throw new IOException("interrupted while searching the point in time " + pit.id(), e);
        }
        final SearchMerge.Merged merged = SearchMerge.merge(serving, answers, source);
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

    /**
     * Runs one search across the shards of every index named, and merges what comes back.
     *
     * @param serving the node coordinating the search
     * @param metadata the metadata plane, for routing and for shards this node does not hold
     * @param indices the indices to search, by name
     * @param source the search itself
     * @return the merged hits, with the coverage this answer was assembled from
     * @throws IOException if the fan-out cannot be run
     */
    public static SearchOutcome run(
        ServerlessNode serving,
        MetadataPlane metadata,
        java.util.Map<String, org.opensearch.serverless.cluster.IndexDescriptor> indices,
        SearchSourceBuilder source
    ) throws IOException {
        return run(serving, metadata, indices, source, Options.DEFAULT);
    }

    /**
     * How a wide search may be narrowed.
     *
     * @param allowPartialActivation whether a search needing more cold shards than the activation budget
     *     answers from the first ones within it, reporting the rest as failed, rather than being refused
     * @param preSkippedShards shards resolution already ruled out, reported as skipped and successful
     * @param nowMillis the request's single now, or zero to take one here
     * @param judged indices the rollups already settled may match, whose shards are not checked again
     */
    public record Options(boolean allowPartialActivation, int preSkippedShards, long nowMillis, java.util.Set<String> judged) {
        /** Refuse past the budget; nothing skipped before the fan-out; the fan-out's own clock. */
        public static final Options DEFAULT = new Options(false, 0, 0L, java.util.Set.of());

        /**
         * Options with only the partial-activation choice.
         *
         * @param allowPartialActivation whether to answer from the shards within the budget
         */
        public Options(boolean allowPartialActivation) {
            this(allowPartialActivation, 0, 0L, java.util.Set.of());
        }
    }

    /** One shard of one index, as the fan-out addresses it. */
    private record Target(org.opensearch.serverless.cluster.IndexDescriptor descriptor, int shard) {
    }

    /**
     * Runs one search across the shards of every index named, pruning the shards it provably cannot match
     * and refusing -- or, on request, narrowing -- a search that would activate more cold shards than the
     * node's budget.
     *
     * <p><b>Two phases, as core's {@code can_match} is.</b> The first reads, for each shard not open here,
     * its head and its manifest, concurrently: a shard with no owner is exactly its published commit, and if
     * that commit's {@link org.opensearch.serverless.store.PruningDigest} says the query cannot match, the
     * shard is skipped -- reported in {@code _shards.skipped} and counted as successful, as core counts one.
     * The phase is skipped outright when the query has nothing a digest can judge. The second phase is the
     * ordinary fan-out over what is left, and if every shard was pruned one is searched anyway, so the
     * response has aggregations of the right shape, which is also what core does.
     *
     * <p><b>The activation budget.</b> Every shard the second phase searches that is not already open here
     * may cost a cold open somewhere. More of them than {@code serverless.search.max_activations_per_query}
     * is refused with a 400 before any is opened, unless the caller asked for
     * {@code allow_partial_activation}, in which case the first ones within the budget are searched and the
     * rest reported as failed shards -- a partial answer that says it is partial.
     *
     * @param serving the node coordinating the search
     * @param metadata the metadata plane, for routing and for shards this node does not hold
     * @param indices the indices to search, by name
     * @param source the search itself
     * @param options how the search may be narrowed
     * @return the merged hits, with the coverage this answer was assembled from
     * @throws IOException if the fan-out cannot be run
     */
    public static SearchOutcome run(
        ServerlessNode serving,
        MetadataPlane metadata,
        java.util.Map<String, org.opensearch.serverless.cluster.IndexDescriptor> indices,
        SearchSourceBuilder source,
        Options options
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

        // One instant for every shard of this request; see runFrozen. The caller's, when it judged anything by it.
        final long nowInMillis = options.nowMillis() > 0L ? options.nowMillis() : System.currentTimeMillis();
        final List<Target> everyShard = new ArrayList<>();
        for (org.opensearch.serverless.cluster.IndexDescriptor descriptor : indices.values()) {
            for (int shard : descriptor.unitIds()) {
                everyShard.add(new Target(descriptor, shard));
            }
        }
        final int shards = everyShard.size() + options.preSkippedShards();
        final java.util.Set<Target> pruned = prune(serving, metadata, everyShard, source, nowInMillis, options.judged());
        final List<Target> searched = new ArrayList<>();
        for (Target target : everyShard) {
            if (pruned.contains(target) == false) {
                searched.add(target);
            }
        }
        if (searched.isEmpty() && everyShard.isEmpty() == false) {
            // Everything pruned. One shard still runs, so the answer's aggregations have their shape.
            searched.add(everyShard.get(0));
            pruned.remove(everyShard.get(0));
        }

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

        // The activation budget: shards not open here are the ones that may cost a cold open.
        final int budget = serving.searchSettings().searchActivationBudget();
        int cold = 0;
        for (Target target : searched) {
            if (SearchShardCalls.localShard(serving, target.descriptor(), target.shard()) == null) {
                cold++;
            }
        }
        if (cold > budget) {
            if (options.allowPartialActivation() == false) {
                throw new org.opensearch.OpenSearchStatusException(
                    "this search would activate "
                        + cold
                        + " shards not open on this node, past the budget of "
                        + budget
                        + " (serverless.search.max_activations_per_query). Narrow the indices or the time range, or pass "
                        + "allow_partial_activation=true to search the first "
                        + budget
                        + " and have the rest reported as failed shards",
                    org.opensearch.core.rest.RestStatus.BAD_REQUEST
                );
            }
            int admitted = 0;
            for (java.util.Iterator<Target> it = searched.iterator(); it.hasNext();) {
                final Target target = it.next();
                if (SearchShardCalls.localShard(serving, target.descriptor(), target.shard()) != null) {
                    continue;
                }
                if (admitted++ < budget) {
                    continue;
                }
                it.remove();
                failures.add(
                    failure(
                        target.descriptor().name(),
                        target.shard(),
                        new org.opensearch.OpenSearchStatusException(
                            "not searched: the query's activation budget of " + budget + " was spent on other shards",
                            org.opensearch.core.rest.RestStatus.TOO_MANY_REQUESTS
                        )
                    )
                );
            }
        }

        // Every shard at once, up to a bound. This loop used to run the shards one after another, so a
        // query's latency was the sum of its shards rather than the slowest of them -- and the shape of
        // the loop was the only reason. Each task answers for exactly one shard and swallows nothing: a
        // shard that cannot be reached comes back null and shows up in the coverage this already reports.
        final SearchMerge.Charged charged = new SearchMerge.Charged(serving);
        final List<java.util.concurrent.Callable<ShardAnswer>> tasks = new ArrayList<>();
        for (Target target : searched) {
            final int number = target.shard();
            final String name = target.descriptor().name();
            final org.opensearch.serverless.cluster.IndexDescriptor descriptor = target.descriptor();
            tasks.add(() -> {
                try {
                    final ShardAnswer answer = SearchShardCalls.askOneShard(serving, metadata, descriptor, number, perShard, nowInMillis);
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
        }
        final List<ShardAnswer> answers;
        try {
            // GENERIC, and no longer the fan-out pool, for the peers' sake rather than this node's. The
            // fan-out pool serves the searches other nodes forward here; a task that forwards blocks its
            // thread until the peer answers, and two nodes whose fan-out pools were full of threads waiting
            // on each other had nothing left to serve each other with -- a deadlock resolved only by the
            // forward timeout. Outgoing waits now live on GENERIC, incoming work on the fan-out pool, and
            // Fanout's caller-runs rule keeps a full GENERIC from parking the coordinator behind itself.
            answers = Fanout.run(
                serving.threadPool().executor(ThreadPool.Names.GENERIC),
                serving.searchSettings().searchFanoutConcurrency(),
                tasks
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            charged.abandon();
            throw new IOException("interrupted while searching " + indices.keySet(), e);
        }

        final SearchMerge.Merged merged = SearchMerge.merge(serving, answers, source);
        try {
            if (merged.answered == 0 && searched.isEmpty() == false) {
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
            // A pruned shard is a successful one, as core counts a skipped shard.
            merged.answered += pruned.size() + options.preSkippedShards();
            return merged.outcome(serving, source, shards, from, size, failures).withSkipped(pruned.size() + options.preSkippedShards());
        } finally {
            merged.release(serving);
        }
    }

    /**
     * The shards this search provably cannot match: not open here, owned by nobody, and with a published
     * digest that excludes the query. Empty when the query has nothing a digest can judge, or pruning is off.
     */
    private static java.util.Set<Target> prune(
        ServerlessNode serving,
        MetadataPlane metadata,
        List<Target> candidates,
        SearchSourceBuilder source,
        long nowInMillis,
        java.util.Set<String> judged
    ) throws IOException {
        final java.util.Set<Target> pruned = java.util.concurrent.ConcurrentHashMap.newKeySet();
        if (serving.searchSettings().searchPruning() == false
            || candidates.size() < 2
            || org.opensearch.serverless.store.PruningDigest.mayPrune(source.query()) == false
            || source.suggest() != null
            || hasGlobalAggregation(source)) {
            return pruned;
        }
        final List<java.util.concurrent.Callable<Boolean>> checks = new ArrayList<>();
        for (Target target : candidates) {
            if (judged.contains(target.descriptor().name())) {
                // The rollup entry already said this index may match, and its shards' digests are no wider.
                continue;
            }
            checks.add(() -> {
                final ShardId local = SearchShardCalls.localShard(serving, target.descriptor(), target.shard());
                if (local != null && serving.reconciler().isReader(local) == false) {
                    // Open here as a writer: it may hold refreshed documents its published digest does not
                    // describe. A reader serves exactly the published commit, so it is judged like any other.
                    return false;
                }
                final var head = metadata.heads().read(target.descriptor().name(), target.shard());
                if (head.isPresent() && head.get().ownerNodeId() != null) {
                    return false;
                }
                final var manifest = metadata.segmentPublisher(target.descriptor().name(), target.descriptor().uuid(), target.shard())
                    .readManifest();
                if (manifest.isEmpty() || manifest.get().digest().canMatch(source.query(), nowInMillis)) {
                    return false;
                }
                pruned.add(target);
                return true;
            });
        }
        try {
            Fanout.run(serving.threadPool().executor(ThreadPool.Names.GENERIC), serving.searchSettings().searchFanoutConcurrency(), checks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while pruning", e);
        }
        return pruned;
    }

    /** A global aggregation ignores the query, so no shard can be skipped for what the query excludes. */
    static boolean hasGlobalAggregation(SearchSourceBuilder source) {
        if (source.aggregations() == null) {
            return false;
        }
        for (org.opensearch.search.aggregations.AggregationBuilder aggregation : source.aggregations().getAggregatorFactories()) {
            if (aggregation instanceof org.opensearch.search.aggregations.bucket.global.GlobalAggregationBuilder) {
                return true;
            }
        }
        return false;
    }

    /** The breaker a coordinator's working set is charged to, which is core's request breaker. */
    static org.opensearch.core.common.breaker.CircuitBreaker breaker(ServerlessNode serving) {
        return serving.services().circuitBreakerService().getBreaker(org.opensearch.core.common.breaker.CircuitBreaker.REQUEST);
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

    /** One shard's contribution: what it matched, and what it returned. */
    static final class ShardAnswer {
        final long total;
        List<SearchHit> hits;
        long hitBytes;
        long aggregationBytes;
        org.opensearch.search.aggregations.InternalAggregations aggregations;
        final org.apache.lucene.search.TotalHits.Relation relation;
        final float maxScore;
        final boolean timedOut;
        final Boolean terminatedEarly;
        /** Bytes this answer holds on the request breaker until the merge takes them over, or {@link #release}. */
        final java.util.concurrent.atomic.AtomicLong reserved = new java.util.concurrent.atomic.AtomicLong();

        /** Gives back whatever this answer still holds; safe to call more than once. */
        void release(ServerlessNode serving) {
            final long held = reserved.getAndSet(0L);
            if (held > 0) {
                breaker(serving).addWithoutBreaking(-held);
            }
            hits = List.of();
            aggregations = null;
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

        ShardAnswer(
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

}
