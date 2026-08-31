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
     * @param shards how many shards it has
     * @param source the query, with its own from and size
     * @return what was found and how completely
     * @throws IOException if the search fails outright
     */
    public static ShardOperations.SearchOutcome run(
        ServerlessNode serving,
        MetadataPlane metadata,
        String index,
        int shards,
        SearchSourceBuilder source
    ) throws IOException {
        final int from = Math.max(0, source.from());
        final int size = Math.max(0, source.size());
        // What each shard is asked for. A shard cannot know how its hits rank against another's, so it
        // has to offer enough to cover the whole window on its own.
        final SearchSourceBuilder perShard = source.shallowCopy();
        perShard.from(0);
        perShard.size(from + size);

        // Every shard at once, up to a bound. This loop used to run the shards one after another, so a
        // query's latency was the sum of its shards rather than the slowest of them -- and the shape of
        // the loop was the only reason. Each task answers for exactly one shard and swallows nothing: a
        // shard that cannot be reached comes back null and shows up in the coverage this already reports.
        final List<java.util.concurrent.Callable<ShardAnswer>> tasks = new ArrayList<>(shards);
        for (int shard = 0; shard < shards; shard++) {
            final int number = shard;
            tasks.add(() -> askOneShard(serving, metadata, index, number, perShard));
        }
        final List<ShardAnswer> answers;
        try {
            answers = Fanout.run(serving.threadPool().executor(ThreadPool.Names.GENERIC), Fanout.DEFAULT_CONCURRENCY, tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while searching " + index, e);
        }

        long total = 0;
        int answered = 0;
        final List<SearchHit> merged = new ArrayList<>();
        final List<org.opensearch.search.aggregations.InternalAggregations> shardAggregations = new ArrayList<>();
        for (ShardAnswer answer : answers) {
            if (answer == null) {
                continue;
            }
            total += answer.total;
            merged.addAll(answer.hits);
            if (answer.aggregations != null) {
                shardAggregations.add(answer.aggregations);
            }
            answered++;
        }

        merged.sort(order(source));
        final List<SearchHit> page = merged.stream().skip(from).limit(size).collect(java.util.stream.Collectors.toList());
        return new ShardOperations.SearchOutcome(total, page, shards, answered, reduce(serving, source, shardAggregations));
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
                count -> {},
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
        for (int i = 0; i < descending.length; i++) {
            descending[i] = source.sorts().get(i).order() == org.opensearch.search.sort.SortOrder.DESC;
        }
        return (a, b) -> compareSortValues(a, b, descending);
    }

    /**
     * Compares two hits on their sort keys, in order, until one differs.
     *
     * <p><b>A hit with no sort values sorts last</b>, for the same reason an unscored one does: a shard
     * that answered without them must not be able to take the top of the page from one that did. That can
     * happen where it should not — a shard running an older build, a field missing from one shard's mapping
     * — and the failure it prevents is a page that silently begins in the wrong place.
     */
    private static int compareSortValues(SearchHit a, SearchHit b, boolean[] descending) {
        final Object[] left = a.getRawSortValues();
        final Object[] right = b.getRawSortValues();
        if (left == null || left.length == 0) {
            return (right == null || right.length == 0) ? 0 : 1;
        }
        if (right == null || right.length == 0) {
            return -1;
        }
        for (int i = 0; i < left.length && i < right.length; i++) {
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

        ShardAnswer(long total, List<SearchHit> hits, org.opensearch.search.aggregations.InternalAggregations aggregations) {
            this.total = total;
            this.hits = hits;
            this.aggregations = aggregations;
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
     * @param index the index
     * @param shard the shard number
     * @param perShard the query, sized to cover the whole window on its own
     * @return what the shard answered, or null if no candidate could answer for it
     * @throws IOException if the shard is here and querying it fails
     */
    private static ShardAnswer askOneShard(
        ServerlessNode serving,
        MetadataPlane metadata,
        String index,
        int shard,
        SearchSourceBuilder perShard
    ) throws IOException {
        final ShardId local = localShard(serving, index, shard);
        if (local != null) {
            serving.markUsed(local);
            final var result = org.opensearch.serverless.shard.ShardQuery.execute(serving.searchService(), local, perShard);
            return new ShardAnswer(result.total(), result.hits(), result.aggregations());
        }
        // Not here. Choose a reader by placement -- NOT the shard's owner, which is the writer: routing
        // searches to writers would couple search capacity to write capacity and make per-index search
        // scale-to-zero meaningless. Placement is a cache-affinity hint, so the owner remains a last
        // resort for the case where no search node exists at all.
        final List<String> targets = new ArrayList<>(
            org.opensearch.serverless.cluster.ReaderPlacement.candidatesFor(
                index,
                shard,
                metadata.membership().current(),
                ServerlessNode.ROLE_SEARCH,
                2
            )
        );
        metadata.heads().read(index, shard).map(h -> h.ownerNodeId()).ifPresent(owner -> {
            if (owner != null && targets.contains(owner) == false) {
                targets.add(owner);
            }
        });

        for (String target : targets) {
            try {
                if (serving.localNode().getId().equals(target)) {
                    // We are the placement for this shard but do not hold it yet. Open it here rather
                    // than asking ourselves over the network.
                    final ShardId opened = serving.serveAsReader(metadata, index, shard);
                    serving.markUsed(opened);
                    final var mine = org.opensearch.serverless.shard.ShardQuery.execute(serving.searchService(), opened, perShard);
                    return new ShardAnswer(mine.total(), mine.hits(), mine.aggregations());
                }
                // The search bound, not the write bound: a peer that is merely busy should cost latency,
                // not coverage.
                final var peer = serving.router().peer(target, serving.router().searchForwardTimeout());
                if (peer.isEmpty()) {
                    continue;
                }
                final var answer = serving.router()
                    .forwardSearch(peer.get(), new org.opensearch.serverless.transport.ForwardedSearchRequest(index, shard, perShard));
                return new ShardAnswer(answer.total(), answer.hits(), answer.aggregations());
            } catch (Exception e) {
                // Try the next candidate. A shard that failed to answer is not a shard with no matches,
                // so it only counts as searched if one of them succeeded.
                LOG.warn("shard " + shard + " of " + index + " was not served by " + target, e);
            }
        }
        return null;
    }

    private static ShardId localShard(ServerlessNode serving, String index, int shard) {
        return serving.reconciler()
            .openShards()
            .stream()
            .filter(s -> s.getIndexName().equals(index) && s.id() == shard)
            .findFirst()
            .orElse(null);
    }
}
