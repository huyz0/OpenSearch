/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.store.BlockCacheDirectory;
import org.opensearch.serverless.transport.CacheWarmCopy;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Takes a search node that joined to warm from {@link SearchReadiness.State#WARMING} to ready: copies its share of the
 * warm cache from its peers, opens those shards as readers, reads from the object store what no peer could hand over,
 * then marks it ready and renews its lease at once so that coordinators start ranking it.
 *
 * <p>It turns ready once it holds {@value #READY_FRACTION} of the blocks its peers named (nine tenths by default),
 * opening its shards then; the rest of the copy, and the reads of what no peer had, go on after.
 *
 * <p>Bounded by {@value #MAX_WAIT}. A node is never held out of searches longer than that, whatever its peers are
 * doing: past it, the node turns ready and the rest of its cache fills from searches as it would have without warm-up.
 * A shard that fails to open is left to open on its first search, for the same reason.
 */
public final class SearchWarmup {

    private static final Logger LOG = LogManager.getLogger(SearchWarmup.class);

    /** The longest a joining node stays warming before it turns ready regardless. */
    public static final String MAX_WAIT = "serverless.search.warm_on_join.max_wait";

    /**
     * The share of its peers' named warm blocks a joiner holds before it turns ready, from 0 to 1. The rest keeps
     * copying after: the coldest tenth of a hot set is worth less than the minutes it can take, especially from a slow
     * peer.
     */
    public static final String READY_FRACTION = "serverless.search.warm_on_join.ready_fraction";

    /** The default share: nine tenths. */
    static final double DEFAULT_READY_FRACTION = 0.9;

    /** The default bound: long enough to copy a few gigabytes at the default pace. */
    static final TimeValue DEFAULT_MAX_WAIT = TimeValue.timeValueMinutes(3);

    /**
     * What a warm-up did.
     *
     * @param copy what the copy from peers did
     * @param opened shards opened as readers
     * @param openFailed shards that failed to open, left to open on first search
     * @param rangedReads ranged reads made for the blocks no peer handed over
     * @param tookMillis how long it took
     * @param readyMillis how long until it turned ready, or -1 if it did not turn ready itself
     */
    public record Outcome(CacheWarmCopy.Outcome copy, int opened, int openFailed, int rangedReads, long tookMillis, long readyMillis) {
    }

    private SearchWarmup() {}

    /**
     * Warms a node that joined to warm, in the background, and marks it ready when done or at {@value #MAX_WAIT}.
     * Does nothing for a node that is not warming.
     *
     * @param node the node
     * @param plane its metadata plane
     */
    public static void startInBackground(ServerlessNode node, MetadataPlane plane) {
        final SearchReadiness readiness = SearchReadiness.of(node);
        if (readiness.state() != SearchReadiness.State.WARMING) {
            return;
        }
        final AtomicBoolean done = new AtomicBoolean();
        final TimeValue maxWait = node.settings().getAsTime(MAX_WAIT, DEFAULT_MAX_WAIT);
        node.threadPool().schedule(() -> {
            if (done.compareAndSet(false, true)) {
                LOG.warn("search warm-up did not finish within [{}]; serving searches with the cache it has", maxWait);
                turnReady(node, plane);
            }
        }, maxWait, org.opensearch.threadpool.ThreadPool.Names.GENERIC);
        node.threadPool().generic().execute(() -> {
            try {
                final Outcome outcome = warm(node, plane, done);
                LOG.info("search warm-up done: {}", outcome);
            } catch (Exception e) {
                LOG.warn("search warm-up failed; serving searches with the cache it has", e);
                if (done.compareAndSet(false, true)) {
                    turnReady(node, plane);
                }
            }
        });
    }

    /**
     * Warms a node and marks it ready, on the calling thread.
     *
     * @param node the node
     * @param plane its metadata plane
     * @return what was done
     * @throws java.io.IOException if the membership cannot be read
     */
    public static Outcome warm(ServerlessNode node, MetadataPlane plane) throws java.io.IOException {
        return warm(node, plane, new AtomicBoolean());
    }

    private static Outcome warm(ServerlessNode node, MetadataPlane plane, AtomicBoolean done) throws java.io.IOException {
        final long started = System.nanoTime();
        final double fraction = node.settings().getAsDouble(READY_FRACTION, DEFAULT_READY_FRACTION);
        final Opening opening = new Opening(node, plane);
        // At the share: open what is held so far and turn ready; the copy goes on underneath for the rest.
        final CacheWarmCopy.Outcome copy = CacheWarmCopy.of(node).copyFromPeers(fraction, shards -> {
            opening.open(shards);
            if (done.compareAndSet(false, true)) {
                turnReady(node, plane);
                opening.readyAt = System.nanoTime();
            }
        });
        final int ranged = opening.finish(copy);
        if (done.compareAndSet(false, true)) {
            turnReady(node, plane);
            opening.readyAt = System.nanoTime();
        }
        return opening.outcome(copy, ranged, started);
    }

    /**
     * Takes over a draining peer's share of its readers: copies their warm blocks from it, opens them here, and reads
     * what it did not hand over. The node stays as ready as it was; it was serving searches all along.
     *
     * @param node this node
     * @param plane its metadata plane
     * @param drainingId the draining peer
     * @return what was done
     */
    public static Outcome takeOverFrom(ServerlessNode node, MetadataPlane plane, String drainingId) {
        final long started = System.nanoTime();
        final Opening opening = new Opening(node, plane);
        final CacheWarmCopy.Outcome copy = CacheWarmCopy.of(node).copyFrom(List.of(drainingId));
        return opening.outcome(copy, opening.finish(copy), started);
    }

    /** Opens a share's shards, each once, whether at the ready point or after the copy. */
    private static final class Opening {
        private final ServerlessNode node;
        private final MetadataPlane plane;
        private final Map<CacheWarmCopy.Shard, ShardId> opened = new java.util.concurrent.ConcurrentHashMap<>();
        private final Set<CacheWarmCopy.Shard> failed = java.util.concurrent.ConcurrentHashMap.newKeySet();
        private volatile long readyAt = -1;

        Opening(ServerlessNode node, MetadataPlane plane) {
            this.node = node;
            this.plane = plane;
        }

        void open(Set<CacheWarmCopy.Shard> shards) {
            for (CacheWarmCopy.Shard shard : shards) {
                if (opened.containsKey(shard) || failed.contains(shard)) {
                    continue;
                }
                try {
                    opened.put(shard, node.readers().serveAsReader(plane, shard.indexName(), shard.shard(), shard.indexUuid()));
                } catch (Exception e) {
                    failed.add(shard);
                    LOG.debug(
                        "could not open shard [{}][{}] ahead of its searches; it opens on its first",
                        shard.indexName(),
                        shard.shard(),
                        e
                    );
                }
            }
        }

        /** Opens what is not open yet and reads from the store what the copy did not bring; returns the ranged reads. */
        int finish(CacheWarmCopy.Outcome copy) {
            open(copy.shards());
            int ranged = 0;
            for (Map.Entry<CacheWarmCopy.Shard, ShardId> shard : opened.entrySet()) {
                try {
                    ranged += fetchFromStore(node, shard.getValue(), copy.unheld().getOrDefault(shard.getKey(), List.of()));
                } catch (Exception e) {
                    LOG.debug("could not read the rest of shard [{}] ahead of its searches", shard.getKey(), e);
                }
            }
            return ranged;
        }

        Outcome outcome(CacheWarmCopy.Outcome copy, int ranged, long started) {
            final long ready = readyAt;
            return new Outcome(
                copy,
                opened.size(),
                failed.size(),
                ranged,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started),
                ready < 0 ? -1 : TimeUnit.NANOSECONDS.toMillis(ready - started)
            );
        }
    }

    /** Marks the node ready and renews its lease now rather than at the next pass, so it is ranked as a reader at once. */
    private static void turnReady(ServerlessNode node, MetadataPlane plane) {
        SearchReadiness.of(node).markReady();
        try {
            node.leases().renewLease(plane);
        } catch (Exception e) {
            LOG.warn("could not renew the lease after warming; the next pass advertises the search role", e);
        }
    }

    /**
     * Reads from the object store those of some blocks of a shard open here that this node does not hold, coalescing
     * consecutive ones into ranged reads: the blocks a peer named as warm but did not hand over.
     *
     * @param node the node
     * @param id a shard open here
     * @param keys block cache keys
     * @return how many ranged reads were made
     * @throws java.io.IOException if the shard's local files cannot be listed
     */
    public static int fetchFromStore(ServerlessNode node, ShardId id, List<String> keys) throws java.io.IOException {
        if (keys.isEmpty()) {
            return 0;
        }
        final BlockCacheDirectory directory = blockDirectoryOf(node, id);
        return directory == null ? 0 : directory.fetchMissing(keys, node.threadPool().generic());
    }

    private static BlockCacheDirectory blockDirectoryOf(ServerlessNode node, ShardId id) {
        final var service = node.indicesService().indexService(id.getIndex());
        if (service == null) {
            return null;
        }
        final var shard = service.getShardOrNull(id.id());
        if (shard == null) {
            return null;
        }
        Directory directory = shard.store().directory();
        while (directory instanceof FilterDirectory filter) {
            if (directory instanceof BlockCacheDirectory blocks) {
                return blocks;
            }
            directory = filter.getDelegate();
        }
        return null;
    }
}
