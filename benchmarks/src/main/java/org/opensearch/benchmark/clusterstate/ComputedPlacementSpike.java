/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.clusterstate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The architectural question this exists to settle: can shard placement be <em>computed</em> instead of
 * <em>allocated</em>, and does that actually remove the ceiling S6 measured?
 *
 * <p>S6 found the allocator binds at the order of 100k active shards, where a steady-state reroute is
 * ~1 s and cold allocation is tens of seconds. That cost is global and superlinear: the allocator
 * considers every shard against every node. The target is 100M indices at up to 100 shards each, which
 * is up to 10B shards, so a global pass is not merely slow, it is never going to run.
 *
 * <p>Computed placement inverts the shape. Nothing is placed globally and nothing is stored. Given a
 * shard and the current node list, a hash names the nodes that should hold it, and the answer is
 * derived on demand for the one shard a request actually needs. The cost per lookup is O(nodes), and
 * critically the cost of "placing" 10B shards is never paid, because it is never asked.
 *
 * <p>This measures the four properties that decide whether that trade is real:
 *
 * <ol>
 *   <li><b>Lookup latency</b> -- the per-request cost that replaces a routing-table read.
 *   <li><b>Distribution</b> -- a hash that balances badly is a hotspot generator; the allocator at
 *       least balances deliberately.
 *   <li><b>Disruption on join</b> -- the fraction of shards whose owner changes when a node joins.
 *       {@code hash % N} reshuffles nearly everything and would make every scale event a fleet-wide
 *       cache wipe; rendezvous should move about 1/N.
 *   <li><b>Top-K stability</b> -- with K candidates per shard, how often the previous holder survives
 *       a membership change, since that is what lets traffic stay on a warm cache while a new node
 *       warms up.
 * </ol>
 *
 * <p>Rendezvous hashing (highest random weight) rather than a token ring: no token bookkeeping, no
 * vnode tuning, and the top-K falls out of the same sort that produces the top-1.
 *
 * <pre>{@code
 * java -da -dsa -Xmx2g -cp <runtime classpath> \
 *   org.opensearch.benchmark.clusterstate.ComputedPlacementSpike
 * }</pre>
 */
public final class ComputedPlacementSpike {

    /** Node counts worth reporting: a small cell, a large cell, and a very large one. */
    private static final int[] NODE_COUNTS = { 10, 50, 200 };

    /** Candidates per shard. 3 mirrors a typical replication factor and gives ARS room to choose. */
    private static final int K = 3;

    private static final int SAMPLE_SHARDS = 1_000_000;

    private ComputedPlacementSpike() {}

    public static void main(String[] args) {
        System.out.println("Rendezvous (highest random weight) placement, K=" + K + ", " + SAMPLE_SHARDS + " shards\n");

        for (int nodeCount : NODE_COUNTS) {
            String[] nodes = nodes(nodeCount);

            // 1. Lookup latency. This is the cost paid per request, replacing a routing-table read.
            long start = System.nanoTime();
            long sink = 0;
            for (int i = 0; i < SAMPLE_SHARDS; i++) {
                sink += topK(shardKey(i), nodes, K)[0];
            }
            long elapsedNanos = System.nanoTime() - start;
            double nanosPerLookup = (double) elapsedNanos / SAMPLE_SHARDS;

            // 2. Distribution. Counting primaries only -- if the top-1 is skewed, so is write load.
            int[] owners = new int[nodeCount];
            for (int i = 0; i < SAMPLE_SHARDS; i++) {
                owners[topK(shardKey(i), nodes, K)[0]]++;
            }
            double ideal = (double) SAMPLE_SHARDS / nodeCount;
            int min = Arrays.stream(owners).min().getAsInt();
            int max = Arrays.stream(owners).max().getAsInt();

            System.out.println("nodes=" + nodeCount);
            System.out.printf("  lookup            : %.0f ns  (%.1fM lookups/sec/core)%n", nanosPerLookup, 1000.0 / nanosPerLookup);
            System.out.printf("  distribution      : min %.2fx, max %.2fx of ideal%n", min / ideal, max / ideal);
            System.out.println("  (sink " + (sink == Long.MIN_VALUE ? "?" : "ok") + ")");

            // Membership change. The primary-move fraction is the cold-cache cost of a scale event; the
            // top-K retention is what makes it survivable, because a shard that keeps any previous
            // candidate can still be served from a warm cache while the new owner fills.
            //
            // A single join is the autoscaling case and is nearly free by construction: one new node
            // can displace at most one of K candidates. The large jumps are the cases that actually
            // decide this -- a deploy, a zone recovery, a doubling -- where several candidates can be
            // displaced at once and "lost all K" stops being trivially zero.
            for (int added : new int[] { 1, nodeCount / 2, nodeCount }) {
                if (added < 1) {
                    continue;
                }
                String[] grown = nodes(nodeCount + added);
                int primaryMoved = 0;
                int lostAllCandidates = 0;
                for (int i = 0; i < SAMPLE_SHARDS; i++) {
                    long key = shardKey(i);
                    int[] before = topK(key, nodes, K);
                    int[] after = topK(key, grown, K);
                    if (before[0] != after[0]) {
                        primaryMoved++;
                    }
                    boolean anySurvives = false;
                    for (int b : before) {
                        for (int a : after) {
                            if (a == b) {
                                anySurvives = true;
                                break;
                            }
                        }
                    }
                    if (anySurvives == false) {
                        lostAllCandidates++;
                    }
                }
                System.out.printf(
                    "  +%-4d nodes       : primary moves %5.2f%% (ideal %5.2f%%), lost all K warm %6.3f%%%n",
                    added,
                    100.0 * primaryMoved / SAMPLE_SHARDS,
                    100.0 * added / (nodeCount + added),
                    100.0 * lostAllCandidates / SAMPLE_SHARDS
                );
            }
            System.out.println();
        }
    }

    /**
     * Top-K by rendezvous weight. A partial selection rather than a full sort: K is small and the node
     * count is in the hundreds, so an insertion pass beats sorting the whole array.
     */
    static int[] topK(long key, String[] nodes, int k) {
        int[] best = new int[k];
        long[] bestWeight = new long[k];
        Arrays.fill(bestWeight, Long.MIN_VALUE);
        Arrays.fill(best, -1);

        for (int n = 0; n < nodes.length; n++) {
            long w = weight(key, n);
            // Walk from the weakest kept candidate; most nodes lose immediately and exit on the first
            // comparison, which is what keeps this near O(nodes) rather than O(nodes * k).
            if (w <= bestWeight[k - 1]) {
                continue;
            }
            int pos = k - 1;
            while (pos > 0 && w > bestWeight[pos - 1]) {
                bestWeight[pos] = bestWeight[pos - 1];
                best[pos] = best[pos - 1];
                pos--;
            }
            bestWeight[pos] = w;
            best[pos] = n;
        }
        return best;
    }

    /**
     * Mixing function for (shard, node). This is the 64-bit finalizer from MurmurHash3 applied to a
     * combination of the two, which is cheap and has the avalanche behaviour rendezvous needs -- a weak
     * mix shows up directly as a lopsided distribution in the numbers above.
     */
    private static long weight(long key, int nodeOrdinal) {
        long h = key * 0x9E3779B97F4A7C15L + nodeOrdinal * 0xBF58476D1CE4E5B9L;
        h ^= (h >>> 30);
        h *= 0xBF58476D1CE4E5B9L;
        h ^= (h >>> 27);
        h *= 0x94D049BB133111EBL;
        h ^= (h >>> 31);
        return h;
    }

    /** Stands in for a real (index UUID, shard id) pair; only its distribution matters here. */
    private static long shardKey(int i) {
        return i * 0x2545F4914F6CDD1DL;
    }

    private static String[] nodes(int count) {
        List<String> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add("node-" + i);
        }
        return list.toArray(new String[0]);
    }
}
