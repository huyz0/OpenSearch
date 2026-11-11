/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;

import java.io.IOException;
import java.util.List;

/**
 * Asks one shard of a search for its answer: the local copy when it is open here, otherwise the owner or a
 * spare reader, and for a point in time the frozen view. Split out of {@link SearchFanout}.
 */
final class SearchShardCalls {

    private SearchShardCalls() {}

    static final org.apache.logging.log4j.Logger LOG = org.apache.logging.log4j.LogManager.getLogger(SearchFanout.class);

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
    static SearchFanout.ShardAnswer askOneShard(
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
            try {
                // A reader here is asked first whether its commit may be behind an acknowledged write nobody is
                // writing; one that is answers for no document, and the next copy is tried.
                serving.ensureReaderCurrent(metadata, index, shard, local);
                final org.opensearch.serverless.shard.ShardQuery.Result result = queryHere(serving, local, perShard, nowInMillis);
                markUsedIfMatched(serving, local, result);
                return new SearchFanout.ShardAnswer(result);
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
        final var members = metadata.membership().current();
        final List<String> targets = org.opensearch.serverless.cluster.ReaderPlacement.candidatesFor(
            index,
            shard,
            members,
            ServerlessNode.ROLE_SEARCH,
            2
        );
        final java.util.Set<String> tried = new java.util.HashSet<>();
        boolean refusedAtTheCap = false;
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
            tried.add(target);
            try {
                final SearchFanout.ShardAnswer answer = askTarget(serving, metadata, descriptor, shard, target, perShard, nowInMillis);
                if (answer != null) {
                    return answer;
                }
            } catch (Exception e) {
                // Try the next candidate. A shard that failed to answer is not a shard with no matches,
                // so it only counts as searched if one of them succeeded.
                LOG.warn("shard " + shard + " of " + index + " was not served by " + target, e);
                lastFailure = e;
                refusedAtTheCap |= atTheCap(e);
            }
        }
        // A candidate was full. Placement is an affinity, not a requirement: any search node can open a reader,
        // and one with room answers where the preferred ones could only refuse. A burst of searches over thousands of
        // indices filled the preferred nodes and failed the rest outright while other nodes had slots free.
        final List<String> spares = refusedAtTheCap
            ? org.opensearch.serverless.cluster.ReaderPlacement.candidatesFor(
                index,
                shard,
                members,
                ServerlessNode.ROLE_SEARCH,
                Integer.MAX_VALUE
            )
            : List.of();
        for (String spare : spares) {
            if (tried.add(spare) == false) {
                continue;
            }
            try {
                final SearchFanout.ShardAnswer answer = askTarget(serving, metadata, descriptor, shard, spare, perShard, nowInMillis);
                if (answer != null) {
                    return answer;
                }
            } catch (Exception e) {
                LOG.debug("shard " + shard + " of " + index + " was not served by " + spare, e);
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
     * Counts a search as use of a shard it was already holding only if the shard matched something.
     *
     * <p>Eviction gives up what nobody has used lately, and a scan that touched a shard and found nothing in it is not
     * use. A fleet's wide search ran every two seconds over every index; nothing rules a held writer out of one, so every
     * held shard looked busy forever, nothing was ever old enough to evict, and full nodes refused new shards outright --
     * a node added to the fleet filled to its cap within two minutes and relieved nothing. A shard just opened for a
     * search still counts as used when it opens.
     */
    static void markUsedIfMatched(ServerlessNode serving, ShardId shardId, org.opensearch.serverless.shard.ShardQuery.Result result) {
        if (result.total() > 0) {
            serving.markUsed(shardId);
        }
    }

    /** Asks one node for a shard: this one by opening a reader, another over the wire; null if it cannot be reached. */
    static SearchFanout.ShardAnswer askTarget(
        ServerlessNode serving,
        MetadataPlane metadata,
        org.opensearch.serverless.cluster.IndexDescriptor descriptor,
        int shard,
        String target,
        SearchSourceBuilder perShard,
        long nowInMillis
    ) throws Exception {
        final String index = descriptor.name();
        if (serving.localNode().getId().equals(target)) {
            // We are the placement for this shard but do not hold it yet. Open it here rather
            // than asking ourselves over the network.
            final ShardId opened = serving.serveAsReader(metadata, index, shard, descriptor.uuid());
            serving.markUsed(opened);
            return new SearchFanout.ShardAnswer(queryHere(serving, opened, perShard, nowInMillis));
        }
        // The search bound, not the write bound: a peer that is merely busy should cost latency,
        // not coverage.
        final var peer = serving.router().peer(target, serving.router().searchForwardTimeout());
        if (peer.isEmpty()) {
            return null;
        }
        return new SearchFanout.ShardAnswer(
            serving.router()
                .forwardSearch(
                    peer.get(),
                    new org.opensearch.serverless.transport.ForwardedSearchRequest(index, shard, perShard, descriptor.uuid(), nowInMillis)
                )
        );
    }

    /** Whether a failure was a node refusing a reader at its shard cap, here or on a peer. */
    static boolean atTheCap(Throwable e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause.getMessage() != null && cause.getMessage().contains(ServerlessNode.AT_THE_CAP)) {
                return true;
            }
            if (cause == cause.getCause()) {
                break;
            }
        }
        return false;
    }

    /**
     * Whether a local query failed because this copy of the shard is gone, rather than because of the
     * query: a reader released under it, a shard closed or no longer here. Only those are worth asking
     * another copy about; everything else is the same answer everywhere.
     */
    static boolean copyUnusable(Throwable e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof org.opensearch.serverless.shell.ShardBehindLogException
                || cause instanceof org.apache.lucene.store.AlreadyClosedException
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

    static ShardId localShard(ServerlessNode serving, org.opensearch.serverless.cluster.IndexDescriptor descriptor, int shard) {
        // By uuid as well as name: a shard of a deleted index of the same name is still open here until
        // the next heartbeat notices, and it must not answer for the recreated one.
        final var held = serving.reconciler().servingShard(new ShardId(descriptor.name(), descriptor.uuid(), shard));
        return held == null ? null : held.shardId();
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
    static SearchFanout.ShardAnswer askOneFrozenShard(
        ServerlessNode serving,
        MetadataPlane metadata,
        org.opensearch.serverless.metadata.PointInTime pit,
        int shard,
        SearchSourceBuilder perShard,
        long nowInMillis
    ) throws Exception {
        final ShardId local = localFrozenView(serving, pit, shard);
        if (local != null) {
            return new SearchFanout.ShardAnswer(queryHere(serving, local, perShard, nowInMillis));
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
                    return new SearchFanout.ShardAnswer(queryHere(serving, shardId, perShard, nowInMillis));
                }
                final var peer = serving.router().peer(target, serving.router().searchForwardTimeout());
                if (peer.isEmpty()) {
                    continue;
                }
                return new SearchFanout.ShardAnswer(
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
        return new SearchFanout.ShardAnswer(queryHere(serving, shardId, perShard, nowInMillis));
    }

    /**
     * Queries a shard this node holds, and tells the reconciler for as long as the query runs.
     *
     * <p>The in-flight count is what keeps a pass from releasing a reader under a running query: a shard
     * with a non-zero count is skipped by the release that would otherwise close it mid-search. The
     * request's one {@code now} travels with the query so every shard evaluates the same instant.
     */
    static org.opensearch.serverless.shard.ShardQuery.Result queryHere(
        ServerlessNode serving,
        ShardId shardId,
        SearchSourceBuilder perShard,
        long nowInMillis
    ) throws IOException {
        final boolean frozen = serving.reconciler().isFrozen(shardId);
        if (frozen == false) {
            // Before the query too, so it runs on the mapping in force now; see ServerlessNode#ensureIncarnationLive.
            serving.ensureIncarnationLive(shardId);
        }
        serving.reconciler().enter(shardId);
        final org.opensearch.serverless.shard.ShardQuery.Result result;
        try {
            result = org.opensearch.serverless.shard.ShardQuery.execute(serving.searchService(), shardId, perShard, nowInMillis);
        } finally {
            serving.reconciler().exit(shardId);
        }
        // An answer from a deleted incarnation is refused rather than merged; see ServerlessNode#INCARNATION_FENCE_MILLIS.
        // Not for a frozen view: it is keyed by its view's id rather than the index's uuid, and outliving
        // its index is what a point in time is for.
        if (frozen == false) {
            serving.ensureIncarnationLive(shardId);
        }
        return result;
    }

    static ShardId localFrozenView(ServerlessNode serving, org.opensearch.serverless.metadata.PointInTime pit, int shard) {
        final ShardId candidate = new ShardId(pit.index(), pit.id(), shard);
        return serving.reconciler().isFrozen(candidate) ? candidate : null;
    }
}
