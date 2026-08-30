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
        for (ShardAnswer answer : answers) {
            if (answer == null) {
                continue;
            }
            total += answer.total;
            merged.addAll(answer.hits);
            answered++;
        }

        // Descending by score. An unscored hit sorts last rather than unpredictably: a shard that
        // returned hits with no score should not be able to take the top of the page from one that
        // scored them.
        merged.sort((a, b) -> Float.compare(score(b), score(a)));
        final List<SearchHit> page = merged.stream().skip(from).limit(size).collect(java.util.stream.Collectors.toList());
        return new ShardOperations.SearchOutcome(total, page, shards, answered);
    }

    /** An unscored hit sorts last rather than unpredictably. */
    private static float score(SearchHit hit) {
        return Float.isNaN(hit.getScore()) ? Float.NEGATIVE_INFINITY : hit.getScore();
    }

    /** One shard's contribution: what it matched, and what it returned. */
    private static final class ShardAnswer {
        private final long total;
        private final List<SearchHit> hits;

        ShardAnswer(long total, List<SearchHit> hits) {
            this.total = total;
            this.hits = hits;
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
            return new ShardAnswer(result.total(), result.hits());
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
                    return new ShardAnswer(mine.total(), mine.hits());
                }
                // The search bound, not the write bound: a peer that is merely busy should cost latency,
                // not coverage.
                final var peer = serving.router().peer(target, serving.router().searchForwardTimeout());
                if (peer.isEmpty()) {
                    continue;
                }
                final var answer = serving.router()
                    .forwardSearch(peer.get(), new org.opensearch.serverless.transport.ForwardedSearchRequest(index, shard, perShard));
                return new ShardAnswer(answer.total(), answer.hits());
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
