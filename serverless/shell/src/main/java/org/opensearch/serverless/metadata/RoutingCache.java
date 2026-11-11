/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.serverless.cluster.IndexDescriptor;

import java.io.IOException;
import java.util.function.LongSupplier;

/**
 * The resolutions a node's routing lookups may answer from: an index or alias name, what it resolved to, and when.
 *
 * <p>See {@link #resolveForRouting} for why routing alone may use an earlier read, and what a stale answer can and
 * cannot do.
 */
final class RoutingCache {

    /** Reads a name through to the store. */
    @FunctionalInterface
    interface Resolver {
        /**
         * Resolves a name from the store.
         *
         * @param name the index or alias name
         * @return what it names
         * @throws IOException if the register cannot be read or parsed
         */
        DescriptorResolution read(String name) throws IOException;
    }

    private final Resolver resolve;
    private final LongSupplier clock;

    /**
     * Creates an empty cache.
     *
     * @param resolve reads a name through to the store
     * @param clock source of wall-clock millis, the same one the metadata plane uses
     */
    RoutingCache(Resolver resolve, LongSupplier clock) {
        this.resolve = resolve;
        this.clock = clock;
    }

    /**
     * How long a routing lookup may answer from a resolution it read earlier.
     *
     * <p>One second, which is the interval {@code SearchHandler} already probes membership on and is
     * chosen to match it rather than independently. Both are the same trade: a fact that changes rarely,
     * read on the critical path of every request, is worth serving from a snapshot that is at most this
     * far behind.
     */
    static final long DEFAULT_ROUTING_CACHE_MILLIS = 1_000L;

    /** A resolution and when it was read. */
    private record CachedResolution(DescriptorResolution resolution, long atMillis, long readStartedNanos, long weight) {
    }

    /** Approximate retained memory budget, including the lazily rendered routing metadata. */
    static final long DEFAULT_ROUTING_CACHE_MAXIMUM_WEIGHT = 64L * 1024 * 1024;

    private final org.opensearch.common.cache.Cache<String, CachedResolution> routingCache = org.opensearch.common.cache.CacheBuilder.<
        String,
        CachedResolution>builder().setMaximumWeight(DEFAULT_ROUTING_CACHE_MAXIMUM_WEIGHT).weigher((name, value) -> value.weight()).build();

    private final Object routingCacheMutation = new Object();

    private long routingCacheEpoch;

    private volatile long routingCacheMillis = DEFAULT_ROUTING_CACHE_MILLIS;

    /**
     * Resolves a name for <em>routing</em>, which may be answered from a recent read.
     *
     * <p><b>Why this is separate from {@link MetadataPlane#resolve} rather than replacing it.</b> A descriptor read was
     * the whole object-store cost of a warm search and half the cost of a write -- one register GET per
     * request per index named, for a fact that changes when an operator changes it. But most callers of
     * {@code resolve} are <em>mutating</em>: they need the generation they read to hold a compare-and-swap
     * together, and a generation from a second ago is a swap that will be refused or, worse, one that lands
     * on a record it never saw. Those callers keep reading through. Only routing uses this.
     *
     * <p><b>What a stale answer can and cannot do.</b> A descriptor carries the index's uuid, and the uuid
     * is checked again wherever the request lands: {@code place} matches an open shard on uuid as well as
     * name, and a forwarded request is refused by {@code ShardRouter} when the uuid it carries is not the
     * one the shard has. That catches a stale resolution only when the landing node no longer holds the old
     * incarnation's shard. When it still does, the uuids match -- both are the old one -- and this cache
     * alone acknowledged a write into a deleted index. What closes that is the landing node's own fence:
     * it answers from a shard only within {@code WriteFences#INCARNATION_FENCE_MILLIS} of having read
     * that the shard's incarnation still exists, and a delete answers only after that window has passed.
     * So a request routed here can be refused and retried, and is never answered from an incarnation whose
     * delete has returned. Search, get, multi-get and the shard operations behind them route through here
     * on the same terms as writes.
     *
     * <p><b>A miss is never cached.</b> Create-then-write against a name this node has not seen reads
     * through and finds it, so a new index is usable the instant it exists. What ages is only a name
     * already in hand: a mapping or rollover reaching this node up to a second late, which is the window
     * open shards already have for mapping changes, and a deleted index resolving for up to a second more.
     *
     * @param name the index or alias name
     * @return what it names
     * @throws IOException if the register cannot be read or parsed
     */
    DescriptorResolution resolveForRouting(String name) throws IOException {
        if (routingCacheMillis <= 0L) {
            return resolve.read(name);
        }
        final long now = clock.getAsLong();
        final CachedResolution seen = routingCache.get(name);
        if (seen != null && now - seen.atMillis() <= routingCacheMillis) {
            return seen.resolution();
        }
        final long epoch;
        synchronized (routingCacheMutation) {
            epoch = routingCacheEpoch;
        }
        final long readStartedNanos = System.nanoTime();
        final DescriptorResolution fresh = resolve.read(name);
        synchronized (routingCacheMutation) {
            // A read started before a local mutation must not republish the invalidated value.
            if (epoch == routingCacheEpoch) {
                if (fresh.absent()) {
                    routingCache.invalidate(name);
                } else {
                    final long weight = routingCacheWeight(name, fresh);
                    if (weight <= routingCache.getMaximumWeight()) {
                        routingCache.put(name, new CachedResolution(fresh, now, readStartedNanos, weight));
                    } else {
                        // A one-off large mapping must not evict every useful small resolution.
                        routingCache.invalidate(name);
                    }
                    pruneRoutingCache(now);
                }
            }
        }
        return fresh;
    }

    /**
     * When this node last read an index's descriptor from the store and found it at this uuid, by
     * {@link System#nanoTime}, taken as the read began -- for the incarnation fence, which that read confirms as
     * surely as one of its own. A read answered from the cache is not a read and does not move it.
     *
     * @param indexName the index
     * @param uuid the incarnation
     * @return what the read found and when it began, or empty if no cached read found this index at this uuid
     */
    java.util.Optional<ConfirmingRead> readConfirmingIncarnation(String indexName, String uuid) {
        final CachedResolution seen = routingCache.get(indexName);
        if (seen == null || seen.resolution().index() == null || uuid.equals(seen.resolution().index().uuid()) == false) {
            return java.util.Optional.empty();
        }
        return java.util.Optional.of(new ConfirmingRead(seen.resolution().index(), seen.readStartedNanos()));
    }

    /**
     * Past this many names, entries older than the window are dropped.
     *
     * <p>An entry past the window answers nothing -- the next lookup reads through and overwrites it -- so
     * dropping one changes no answer. Without this the map kept every name this node ever wrote to or read,
     * for the life of the process, each with its whole descriptor, mapping included: a coordinator that
     * touched a million daily indices over a year held them all. With it, the map holds what was resolved
     * in the last second, plus up to this many stragglers.
     */
    static final int DEFAULT_ROUTING_CACHE_PRUNE_ABOVE = 10_000;

    private volatile int routingCachePruneAbove = DEFAULT_ROUTING_CACHE_PRUNE_ABOVE;

    private volatile long routingCachePrunedAt = Long.MIN_VALUE;

    /**
     * Sets how many names the routing cache may hold before expired ones are dropped.
     *
     * @param entries the threshold
     */
    void setRoutingCachePruneAbove(int entries) {
        this.routingCachePruneAbove = Math.max(0, entries);
    }

    private void pruneRoutingCache(long now) {
        // At most once per window: a walk per insert would make a busy coordinator pay for its own cache.
        if (routingCache.count() <= routingCachePruneAbove
            || (routingCachePrunedAt != Long.MIN_VALUE && now - routingCachePrunedAt < routingCacheMillis)) {
            return;
        }
        routingCachePrunedAt = now;
        final long window = routingCacheMillis;
        for (String name : routingCache.keysSnapshot()) {
            final CachedResolution seen = routingCache.get(name);
            if (seen != null && now - seen.atMillis() > window) {
                routingCache.invalidate(name, seen);
            }
        }
    }

    /**
     * Returns how many names the routing cache holds, for tests that pin its bound.
     *
     * @return the entry count
     */
    int routingCacheSize() {
        return routingCache.count();
    }

    /**
     * Drops a name from the routing cache, so a change this node just made is visible to its own next
     * request rather than a window later.
     *
     * <p>Only this node's cache: another node's ages out on its own. That asymmetry is deliberate and is
     * what keeps read-your-writes true where a client is most likely to notice it, which is against the
     * node it just wrote to.
     *
     * @param name the index or alias name
     */
    void forgetRouting(String name) {
        synchronized (routingCacheMutation) {
            routingCacheEpoch++;
            routingCache.invalidate(name);
        }
    }

    /**
     * Sets the approximate retained-memory budget for routing resolutions.
     *
     * @param bytes the budget; an individual resolution exceeding it is not retained
     */
    void setRoutingCacheMaximumWeight(long bytes) {
        synchronized (routingCacheMutation) {
            routingCache.setMaximumWeight(bytes);
            routingCache.refresh();
        }
    }

    private static long routingCacheWeight(String name, DescriptorResolution resolution) {
        // Include a fixed object/entry allowance and headroom for parsed routing metadata. Counting
        // only names would leave a handful of large mappings or aliases able to exhaust the heap.
        long weight = 4096L + 2L * name.length();
        final IndexDescriptor index = resolution.index();
        if (index != null) {
            weight += 256L * index.numberOfShards() + 2L * (index.name().length() + index.uuid().length());
            if (index.mapping() != null) {
                weight += 16L * index.mapping().length();
            }
            for (String key : index.extraSettings().keySet()) {
                weight += 128L + 16L * (key.length() + index.extraSettings().get(key, "").length());
            }
            for (String alias : index.aliasedBy()) {
                weight += 64L + 2L * alias.length();
            }
        }
        final var alias = resolution.alias();
        if (alias != null) {
            weight += 2L * alias.name().length();
            for (String member : alias.indices()) {
                weight += 64L + 2L * member.length();
            }
            for (String member : alias.pendingAdditions()) {
                weight += 64L + 2L * member.length();
            }
        }
        return weight;
    }

    /**
     * Sets how long a routing lookup may serve an earlier read; zero reads through every time.
     *
     * @param millis the window
     */
    void setRoutingCacheMillis(long millis) {
        this.routingCacheMillis = Math.max(0L, millis);
    }
}
