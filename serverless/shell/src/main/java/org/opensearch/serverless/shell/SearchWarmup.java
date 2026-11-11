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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Takes a search node that joined to warm from {@link SearchReadiness.State#WARMING} to ready: copies its share of the
 * warm cache from its peers, opens those shards as readers, reads from the object store what no peer could hand over,
 * then marks it ready and renews its lease at once so that coordinators start ranking it.
 *
 * <p>Bounded by {@value #MAX_WAIT}. A node is never held out of searches longer than that, whatever its peers are
 * doing: past it, the node turns ready and the rest of its cache fills from searches as it would have without warm-up.
 * A shard that fails to open is left to open on its first search, for the same reason.
 */
public final class SearchWarmup {

    private static final Logger LOG = LogManager.getLogger(SearchWarmup.class);

    /** The longest a joining node stays warming before it turns ready regardless. */
    public static final String MAX_WAIT = "serverless.search.warm_on_join.max_wait";

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
     */
    public record Outcome(CacheWarmCopy.Outcome copy, int opened, int openFailed, int rangedReads, long tookMillis) {
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
        final CacheWarmCopy.Outcome copy = CacheWarmCopy.of(node).copyFromPeers();
        int opened = 0;
        int failed = 0;
        int ranged = 0;
        for (CacheWarmCopy.Shard shard : copy.shards()) {
            if (done.get()) {
                break;
            }
            try {
                final ShardId id = node.readers().serveAsReader(plane, shard.indexName(), shard.shard(), shard.indexUuid());
                opened++;
                ranged += fetchFromStore(node, id, copy.unheld().getOrDefault(shard, List.of()));
            } catch (Exception e) {
                failed++;
                LOG.debug("warm-up could not open shard [{}][{}]; it opens on its first search", shard.indexName(), shard.shard(), e);
            }
        }
        if (done.compareAndSet(false, true)) {
            turnReady(node, plane);
        }
        return new Outcome(copy, opened, failed, ranged, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
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
