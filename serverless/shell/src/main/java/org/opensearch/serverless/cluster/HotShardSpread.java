/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.cluster;

import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/**
 * Spreads a hot shard's searches over more than one reader, chosen by load, while a cold shard keeps one.
 *
 * <p>Placement ranks the search nodes for a shard the same way on every coordinator, and every search used to go to
 * the first of them: one reader per shard, however hot the shard was. A shard searched faster than one reader keeps up
 * with queued on that node while the rest of the fleet sat idle.
 *
 * <p>Each coordinator now counts how often it searches each shard, as a decaying rate. A shard searched more than
 * {@link #TARGET_PER_SECOND} times a second here is spread over as many of its top-ranked readers as that rate needs,
 * up to {@link #MAX_READERS}. Because every coordinator ranks the same way, their spreads nest: the readers of a shard
 * searched a little are a subset of the readers of the same shard searched a lot, so a wider spread opens a reader on
 * the next node in line rather than anywhere. Among the readers in the spread a search goes to the less busy of two
 * picked at random, judged by the searches this coordinator has in flight to each, which keeps a slow reader from
 * collecting a queue.
 *
 * <p>The cost is one more reader per step of spread: a cold open of the shard on that node and its refreshes, paid
 * only while the shard stays hot. A cold shard's searches go where they always went, so its cache affinity and its
 * object-store reads are unchanged.
 */
public final class HotShardSpread {

    /** Searches a second, from one coordinator, that one reader of a shard is expected to absorb. */
    public static final String TARGET_PER_SECOND = "serverless.search.reader_spread.target_per_second";
    /** The most readers one shard's searches are spread over. */
    public static final String MAX_READERS = "serverless.search.reader_spread.max_readers";
    static final double DEFAULT_TARGET_PER_SECOND = 50.0;
    static final int DEFAULT_MAX_READERS = 4;
    /** How quickly a shard's rate forgets: about this long after its searches stop, it narrows again. */
    static final long DECAY_NANOS = 10_000_000_000L;
    /** How quickly a burst widens the spread: a rate this short is read beside the long one, and the wider wins. */
    static final long BURST_NANOS = 1_000_000_000L;
    /** How many shards' rates one coordinator keeps; past this, cold ones are dropped. */
    static final int MAX_TRACKED = 50_000;

    private final double targetPerSecond;
    private final int maxReaders;
    private final LongSupplier nanos;
    private final Map<String, Rate> rates = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> inFlight = new ConcurrentHashMap<>();

    private static final Map<Object, HotShardSpread> BY_NODE = new java.util.WeakHashMap<>();

    /**
     * Returns a node's spread, one per node, built from its settings on first use. Its searches pick readers by it, and
     * a peer asked what a joining node will serve reads how wide each hot shard has gone from it.
     *
     * @param node the node, held weakly
     * @param settings the node's settings
     * @return its spread
     */
    public static HotShardSpread of(Object node, Settings settings) {
        synchronized (BY_NODE) {
            return BY_NODE.computeIfAbsent(node, n -> new HotShardSpread(settings, System::nanoTime));
        }
    }

    /**
     * Builds a spread from a node's settings.
     *
     * @param settings the node's settings
     * @param nanos a monotonic clock, in nanoseconds
     */
    public HotShardSpread(Settings settings, LongSupplier nanos) {
        this(
            settings.getAsDouble(TARGET_PER_SECOND, DEFAULT_TARGET_PER_SECOND),
            settings.getAsInt(MAX_READERS, DEFAULT_MAX_READERS),
            nanos
        );
    }

    /**
     * Builds a spread.
     *
     * @param targetPerSecond searches a second one reader absorbs
     * @param maxReaders the widest spread
     * @param nanos a monotonic clock, in nanoseconds
     */
    public HotShardSpread(double targetPerSecond, int maxReaders, LongSupplier nanos) {
        if (targetPerSecond <= 0.0 || maxReaders < 1) {
            throw new IllegalArgumentException("invalid reader spread [" + targetPerSecond + "][" + maxReaders + "]");
        }
        this.targetPerSecond = targetPerSecond;
        this.maxReaders = maxReaders;
        this.nanos = nanos;
    }

    /**
     * Returns the most readers a search of a shard is ever spread over, which is how many candidates to rank.
     *
     * @return the widest spread
     */
    public int maxReaders() {
        return maxReaders;
    }

    /**
     * Counts one search of a shard and orders its ranked readers for it: the reader to ask first, then the rest in
     * their placement order as fallbacks.
     *
     * @param index the index
     * @param shard the shard number
     * @param ranked the shard's readers, best first, as {@link ReaderPlacement#candidatesFor} ranks them
     * @return the same readers, the one chosen for this search first
     */
    public List<String> order(String index, int shard, List<String> ranked) {
        final int width = Math.min(width(record(index + "#" + shard)), ranked.size());
        if (width <= 1) {
            return ranked;
        }
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int a = random.nextInt(width);
        int b = random.nextInt(width - 1);
        if (b >= a) {
            b++;
        }
        final int chosen = load(ranked.get(b)) < load(ranked.get(a)) ? b : a;
        final List<String> ordered = new ArrayList<>(ranked.size());
        ordered.add(ranked.get(chosen));
        for (int i = 0; i < ranked.size(); i++) {
            if (i != chosen) {
                ordered.add(ranked.get(i));
            }
        }
        return ordered;
    }

    /**
     * Marks a search in flight to a node until the returned handle is closed.
     *
     * @param nodeId the node asked
     * @return closes the mark
     */
    public Releasable begin(String nodeId) {
        final AtomicInteger count = inFlight.computeIfAbsent(nodeId, ignored -> new AtomicInteger());
        count.incrementAndGet();
        return count::decrementAndGet;
    }

    /**
     * Returns how many readers a shard's searches are spread over right now.
     *
     * @param index the index
     * @param shard the shard number
     * @return the spread, at least one
     */
    public int widthOf(String index, int shard) {
        final Rate rate = rates.get(index + "#" + shard);
        return rate == null ? 1 : width(rate.perSecond(nanos.getAsLong()));
    }

    private int width(double perSecond) {
        return (int) Math.max(1L, Math.min(maxReaders, (long) Math.ceil(perSecond / targetPerSecond)));
    }

    private int load(String nodeId) {
        final AtomicInteger count = inFlight.get(nodeId);
        return count == null ? 0 : count.get();
    }

    private double record(String key) {
        final long now = nanos.getAsLong();
        if (rates.size() >= MAX_TRACKED && rates.containsKey(key) == false) {
            rates.values().removeIf(rate -> rate.perSecond(now) < targetPerSecond / 10.0);
        }
        return rates.computeIfAbsent(key, ignored -> new Rate()).add(now);
    }

    /**
     * Two rates that decay exponentially, read as the higher: the short one widens a spread within a second of a burst,
     * before a queue builds on one reader, and the long one keeps it wide through a lull of a few seconds.
     */
    private static final class Rate {
        private double slow;
        private double fast;
        private long atNanos = Long.MIN_VALUE;

        synchronized double add(long now) {
            decay(now);
            slow += 1e9 / DECAY_NANOS;
            fast += 1e9 / BURST_NANOS;
            return Math.max(slow, fast);
        }

        synchronized double perSecond(long now) {
            decay(now);
            return Math.max(slow, fast);
        }

        private void decay(long now) {
            if (atNanos != Long.MIN_VALUE && now > atNanos) {
                slow *= Math.exp(-(double) (now - atNanos) / DECAY_NANOS);
                fast *= Math.exp(-(double) (now - atNanos) / BURST_NANOS);
            }
            atNanos = Math.max(atNanos, now);
        }
    }
}
