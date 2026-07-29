/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Locale;

/**
 * T13. What a cluster state publication costs, against node count.
 *
 * <p>Area H removed the per-index cost of cluster state: a gated index has no entry, so creating one
 * publishes nothing. Every architectural conclusion drawn from that then leaned on a second claim, that at
 * thousands of nodes the remaining O(N) publication is the ceiling rather than index metadata. That claim
 * was asserted three times in a row and measured zero times, which is what this closes.
 *
 * <p><b>What it measures.</b> A persistent cluster settings update is about the smallest real cluster state
 * change available: a few bytes of diff, no reroute, no index metadata. So its round trip is close to the
 * cost of publication itself rather than the cost of whatever provoked it. Timing that against node count
 * gives the slope.
 *
 * <p>Node join is measured separately because it is the operation an elastic cluster performs most, and it
 * is the one gating did nothing about. An index that is never in cluster state still costs nothing when a
 * node arrives; the node arriving still costs a publication to everyone.
 *
 * <p><b>What it cannot measure.</b> Thousands of nodes. Every node here is in one JVM, so the range is
 * single digits and the answer is a slope to extrapolate from, not a figure at scale. S30 is the standing
 * warning about that: its index-count projection overshot the one point where it could be checked by
 * twenty-six percent. A slope measured over four points between one and six nodes should be read as an
 * order of magnitude, not a number.
 *
 * <p>It also cannot see contention that only appears at scale. The publication is a two-phase commit
 * against every node, and at one thousand nodes the tail is set by the slowest of a thousand rather than
 * the slowest of six, which no local run reproduces. That makes this measurement a floor.
 *
 * <p><b>Measured, three runs of the whole test:</b>
 *
 * <pre>
 *   data nodes    run A     run B     run C
 *            1    13.69     16.99     26.60 ms
 *            2    13.41     14.42     26.53 ms
 *            4    14.69     15.47     24.22 ms
 *            6    15.71     16.45     23.00 ms
 *   ratio at 6    1.15x     0.97x     0.86x
 * </pre>
 *
 * <p><b>The slope reverses direction between runs, so there is no measurable growth here.</b> Run-to-run
 * variance spans 13.7 ms to 26.6 ms at a fixed node count, which is far wider than anything the node count
 * does across a sixfold increase. Six times the nodes is free within the resolution of this measurement.
 *
 * <p>That contradicts the reason this test was written. The claim it set out to confirm, that the O(N)
 * publication is what caps a cluster of thousands of nodes, is <em>not supported</em> over the range a
 * single JVM can reach. It is also not refuted, because that range is single digits over loopback and the
 * effects that would make the claim true, real network fan-out and a tail set by the slowest of a thousand
 * nodes, are precisely the ones absent here. The honest state is unproven, which is a different thing from
 * the confident assertion it replaces.
 *
 * <p><b>What is solid is the constant rather than the slope.</b> A cluster state change costs roughly 14 to
 * 27 ms whatever the node count, and that is a per-change floor paid by anything that touches cluster
 * state. It also cross-checks S31 from the other direction: gated creation at 235 per second is 4.3 ms per
 * index, which is less than a single publication costs, so gating is demonstrably skipping the round trip
 * rather than merely making it cheaper. Two independent measurements agreeing is worth more than either.
 *
 * <p>Node join over the same range showed no growth either: 180, 105, 104, 96, 106 and 79 ms against one
 * through six existing nodes, with the first figure carrying JVM warmup.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ClusterStatePublicationCostIT extends OpenSearchIntegTestCase {

    /** As far as one JVM comfortably goes. The question is the slope, not the endpoint. */
    private static final int[] DATA_NODES = { 1, 2, 4, 6 };

    private static final int ROUNDS = 9;

    public void testPublicationCostAgainstNodeCount() throws Exception {
        internalCluster().startClusterManagerOnlyNode();

        StringBuilder table = new StringBuilder("\nT13 cluster state publication cost against node count\n");
        table.append(String.format(Locale.ROOT, "  %6s %8s %18s %14s%n", "nodes", "total", "publish median ms", "vs 1 node"));

        int started = 0;
        double atOneNode = 0;
        for (int dataNodes : DATA_NODES) {
            while (started < dataNodes) {
                internalCluster().startDataOnlyNode();
                started++;
            }
            int total = started + 1;
            ensureStableCluster(total);

            // Warmed with the same work as the measurement, since an under-warmed first arm is how S30
            // inverted a whole curve.
            for (int i = 0; i < 3; i++) {
                publishOnce(i);
            }

            long[] micros = new long[ROUNDS];
            for (int round = 0; round < ROUNDS; round++) {
                micros[round] = publishOnce(round);
            }
            double median = median(micros) / 1000.0;
            if (dataNodes == DATA_NODES[0]) {
                atOneNode = median;
            }

            table.append(String.format(Locale.ROOT, "  %6d %8d %18.2f %13.2fx%n", dataNodes, total, median, median / atOneNode));
        }

        table.append("\n  A settings update is a few bytes of diff, so this is close to publication itself\n");
        table.append("  rather than to whatever provoked it. Slope over single digits, not a figure at scale.\n");
        logger.warn(table.toString());

        assertTrue("the measurement must be non-zero, or this measured nothing", atOneNode > 0);
    }

    /**
     * What it costs a node to join, which is the operation an elastic cluster performs most and the one
     * gating did nothing about.
     */
    public void testNodeJoinCostAgainstNodeCount() throws Exception {
        internalCluster().startClusterManagerOnlyNode();

        StringBuilder table = new StringBuilder("\nT13 node join cost against existing cluster size\n");
        table.append(String.format(Locale.ROOT, "  %14s %16s%n", "existing nodes", "join ms"));

        for (int existing = 1; existing <= 6; existing++) {
            long startedAt = System.nanoTime();
            internalCluster().startDataOnlyNode();
            ensureStableCluster(existing + 1);
            double millis = (System.nanoTime() - startedAt) / 1_000_000.0;
            table.append(String.format(Locale.ROOT, "  %14d %16.1f%n", existing, millis));
        }

        table.append("\n  Includes node startup, so this is an upper bound on the coordination share.\n");
        table.append("  The trend across rows is the part that matters, not any single figure.\n");
        logger.warn(table.toString());
    }

    /**
     * One publication. The value alternates so every call is a real change rather than a no-op that the
     * cluster manager could short-circuit, which would measure nothing at all.
     */
    private long publishOnce(int round) {
        long startedAt = System.nanoTime();
        boolean acked = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put("indices.recovery.max_bytes_per_sec", (40 + (round % 2)) + "mb"))
            .get()
            .isAcknowledged();
        long micros = (System.nanoTime() - startedAt) / 1_000;
        // Acknowledgement is the point rather than a formality: an unacked update would mean some node
        // never applied it, and timing a publication that did not reach everyone measures nothing.
        assertTrue("every node must acknowledge, or this is not a full publication", acked);
        return micros;
    }

    private static long median(long[] values) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return sorted[sorted.length / 2];
    }
}
