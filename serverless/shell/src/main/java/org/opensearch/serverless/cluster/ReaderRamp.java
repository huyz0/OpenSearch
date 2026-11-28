/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.cluster;

import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.serverless.membership.NodeLease;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The slow start of a joining reader: how much of its share of shards a search node takes, from its lease and the clock
 * alone, so every node agrees without asking. The Rust build computes the same numbers (integers only), so the two agree in a
 * mixed cluster.
 *
 * <p>The node decides how warm it is and publishes it in its lease ({@link NodeLease#ramp()}); time only bounds that, from
 * below by a floor curve (a node cannot claim to be warm sooner than {@code minMillis}) and from above by a ceiling curve (a
 * stuck or idle node is fully ramped by {@code maxMillis}). A shard is admitted to a node once the node's ramp passes the
 * shard's fixed admission value for that node, so a shard that has moved to a joiner stays there as the ramp rises.
 */
public final class ReaderRamp {

    /** The full ramp: the node takes all its share. */
    public static final int FULL = 1000;

    /** The bounds on a ramp, milliseconds; a {@code maxMillis} that is not positive is off, every node is full. */
    public record Config(long minMillis, long maxMillis) {
        /** No slow start. */
        public static final Config OFF = new Config(0, 0);

        /** Whether the slow start is on. */
        public boolean isOn() {
            return maxMillis > 0;
        }
    }

    private static volatile Config config = Config.OFF;

    private ReaderRamp() {}

    /**
     * Sets the process's slow start.
     *
     * @param value the bounds
     */
    public static void setConfig(Config value) {
        config = value;
    }

    /**
     * Returns the process's slow start.
     *
     * @return the bounds
     */
    public static Config config() {
        return config;
    }

    private static int curve(long now, long since, long duration) {
        if (duration <= 0) {
            return FULL;
        }
        final long age;
        try {
            age = Math.subtractExact(now, since);
        } catch (ArithmeticException overflow) {
            return now > since ? FULL : 0;
        }
        if (age <= 0) {
            return 0;
        }
        if (age >= duration) {
            return FULL;
        }
        return (int) (Math.multiplyHigh(age, 1000L) == 0 ? age * 1000L / duration : FULL);
    }

    /**
     * The share of its shards, 0 to 1000, that a lease's node takes at a time.
     *
     * @param lease the node's lease
     * @param now the time, epoch milliseconds
     * @param config the bounds
     * @return the ramp
     */
    public static int permille(NodeLease lease, long now, Config config) {
        final long since = lease.searchSince();
        if (config.isOn() == false || since < 0 || since > now) {
            return FULL;
        }
        final int ceiling = curve(now, since, config.maxMillis());
        final int said = lease.ramp();
        if (said < 0) {
            return ceiling;
        }
        return Math.max(ceiling, Math.min(said, curve(now, since, config.minMillis())));
    }

    /**
     * A shard's fixed admission value for a node, 0 to 999.
     *
     * @param nodeId the node
     * @param index the index
     * @param shard the shard
     * @return the value
     */
    public static int admission(String nodeId, String index, int shard) {
        return Math.floorMod(Murmur3HashFunction.hash(nodeId + "/" + index + "~" + shard), 1000);
    }

    /**
     * Whether a node takes a shard at a ramp.
     *
     * @param nodeId the node
     * @param index the index
     * @param shard the shard
     * @param ramp the node's ramp, 0 to 1000
     * @return true if admitted
     */
    public static boolean admitted(String nodeId, String index, int shard, int ramp) {
        return ramp >= FULL || admission(nodeId, index, shard) < ramp;
    }

    /** What the joiner's controller reads: searches answered and store GETs made so far, GETs in thousandths. */
    public record Totals(long searches, long getsMilli) {
    }

    /**
     * The joiner's own judgement of how warm it is: a window of searches that each needed about no store read raises its ramp
     * a step; a window with too few searches, or a cold one, holds it. Time bounds the answer elsewhere, so a node with no
     * traffic is still fully ramped.
     */
    public static final class Controller {
        private final int step;
        private final long warmGetsPerSearchMilli;
        private final long minSearches;
        private int ramp;
        private Totals seen;

        /**
         * A controller at its first step.
         *
         * @param step how far a qualifying window raises the ramp
         * @param warmGetsPerSearchMilli the most store GETs a search may average, in thousandths, for a window to qualify
         * @param minSearches the fewest searches a window needs to say anything
         * @param start where it counts from
         */
        public Controller(int step, long warmGetsPerSearchMilli, long minSearches, Totals start) {
            this.step = step;
            this.warmGetsPerSearchMilli = warmGetsPerSearchMilli;
            this.minSearches = Math.max(1, minSearches);
            // A node ranked behind every shard gets no search, so it could never gather the evidence to move; the first
            // step admits it a tenth of its shards, and the floor curve still holds that back until the time has passed.
            this.ramp = Math.max(0, Math.min(FULL, step));
            this.seen = start;
        }

        /**
         * A controller with the default steps: 100 a window, two GETs a search, twenty searches.
         *
         * @param start where it counts from
         * @return the controller
         */
        public static Controller withDefaults(Totals start) {
            return new Controller(100, 2_000, 20, start);
        }

        /**
         * Reads the totals at a lease renewal.
         *
         * @param now the totals now
         * @return the ramp to publish
         */
        public synchronized int observe(Totals now) {
            final long searches = Math.max(0, now.searches() - seen.searches());
            if (searches < minSearches) {
                return ramp;
            }
            final long gets = Math.max(0, now.getsMilli() - seen.getsMilli());
            seen = now;
            if (gets / searches <= warmGetsPerSearchMilli) {
                ramp = Math.max(0, Math.min(FULL, ramp + step));
            }
            return ramp;
        }

        /**
         * Returns the ramp it publishes.
         *
         * @return 0 to 1000
         */
        public synchronized int ramp() {
            return ramp;
        }
    }

    private static final AtomicLong SEARCHES = new AtomicLong();

    /** Counts one search this node answered, for the joiner's controller. */
    public static void countSearch() {
        SEARCHES.incrementAndGet();
    }

    /**
     * Returns the searches this node has answered.
     *
     * @return the count
     */
    public static long searchesTotal() {
        return SEARCHES.get();
    }
}
