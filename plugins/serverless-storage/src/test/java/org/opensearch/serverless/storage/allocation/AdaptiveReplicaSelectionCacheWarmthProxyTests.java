/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.allocation;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.ResponseCollectorService;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

/**
 * Plan item D4 (plan-100m-index-implementation.md, Area D): "the plan assumes ARS's latency signal
 * proxies cache warmth adequately... test it. If it oscillates, blend in
 * {@code ReaderCacheAffinityRecorder}'s direct signal."
 *
 * <p>Exercises core's own real {@link ResponseCollectorService} -- the actual adaptive-replica-
 * selection mechanism {@link org.opensearch.cluster.routing.IndexShardRoutingTable}'s real candidate
 * ranking uses (traced directly: {@code NodeRankComparator} sorts ascending on
 * {@code ComputedNodeStats#rank}, so a lower rank is preferred) -- rather than reasoning about the
 * formula on paper. Response-time magnitudes here (5 ms warm, 50 ms cold, a 10x separation) are
 * illustrative of a local block-cache hit vs. an object-store re-fetch, not measured against a real
 * workload; the qualitative findings (does noise flip the ranking, is a never-queried node
 * distinguishable from a genuinely warm one) do not depend on the exact multiplier.
 */
public class AdaptiveReplicaSelectionCacheWarmthProxyTests extends OpenSearchTestCase {

    private static final long WARM_RESPONSE_NANOS = 5_000_000L; // 5 ms, a local cache hit
    private static final long COLD_RESPONSE_NANOS = 50_000_000L; // 50 ms, an object-store re-fetch
    private static final long SERVICE_TIME_NANOS = 2_000_000L; // held constant -- only response time varies

    private ClusterService clusterService;
    private ResponseCollectorService collector;
    private ThreadPool threadPool;

    @Before
    public void setUpCollector() throws Exception {
        threadPool = new TestThreadPool("ars-cache-warmth-proxy-tests");
        clusterService = ClusterServiceUtils.createClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        collector = new ResponseCollectorService(clusterService);
    }

    @After
    public void tearDownCollector() {
        threadPool.shutdownNow();
    }

    /**
     * The clean case: once both nodes have several consistent samples, ARS's own ranking correctly
     * prefers the warm one. This is the case the plan's design assumed held in general.
     */
    public void testAClearlyWarmNodeConsistentlyOutranksAClearlyColdOne() {
        for (int i = 0; i < 10; i++) {
            collector.addNodeStatistics("warm-node", 0, WARM_RESPONSE_NANOS, SERVICE_TIME_NANOS);
            collector.addNodeStatistics("cold-node", 0, COLD_RESPONSE_NANOS, SERVICE_TIME_NANOS);
        }

        double warmRank = collector.getNodeStatistics("warm-node").orElseThrow().rank(1);
        double coldRank = collector.getNodeStatistics("cold-node").orElseThrow().rank(1);

        assertTrue(
            "a consistently faster (warm) node must rank better (lower) than a consistently slower "
                + "(cold) one -- warm="
                + warmRank
                + " cold="
                + coldRank,
            warmRank < coldRank
        );
    }

    /**
     * The real question D4 asks: does a single anomalous sample (a GC pause, a transient network
     * blip -- not a real cache-state change) flip which node ARS prefers? EWMA smoothing exists
     * precisely to resist this, so this is a real test of whether that smoothing is enough at a
     * realistic alpha (0.3, core's own constant), not an assumption that it is.
     */
    public void testATransientSlowdownOnTheWarmNodeDoesNotFlipTheRanking() {
        // Nine representative fast samples establish the warm node's real baseline first.
        for (int i = 0; i < 9; i++) {
            collector.addNodeStatistics("warm-node", 0, WARM_RESPONSE_NANOS, SERVICE_TIME_NANOS);
        }
        // One anomalous sample -- e.g. a GC pause -- lands on the warm node. Its own cache state
        // didn't change; this is noise, not signal.
        collector.addNodeStatistics("warm-node", 0, COLD_RESPONSE_NANOS * 2, SERVICE_TIME_NANOS);

        for (int i = 0; i < 10; i++) {
            collector.addNodeStatistics("cold-node", 0, COLD_RESPONSE_NANOS, SERVICE_TIME_NANOS);
        }

        double warmRank = collector.getNodeStatistics("warm-node").orElseThrow().rank(1);
        double coldRank = collector.getNodeStatistics("cold-node").orElseThrow().rank(1);

        assertTrue(
            "a single transient anomaly on an otherwise-warm node must not flip it to ranking worse "
                + "than a consistently cold node -- warm="
                + warmRank
                + " cold="
                + coldRank
                + " -- if this fails, ARS's own EWMA smoothing is not sufficient protection against noise "
                + "at this alpha, and ReaderCacheAffinityRecorder's direct signal genuinely needs blending in",
            warmRank < coldRank
        );
    }

    /**
     * The real, concrete gap this investigation actually found: a node ARS has never received a
     * sample for has no entry in its statistics at all, so there is nothing to rank -- it is not
     * "ranked as cold," it is invisible to this signal entirely. A shard reallocated onto a
     * previously-untouched-by-this-shard node (the exact scale-up/rebalance scenario D1's pre-warm
     * mechanism exists for) is indistinguishable, to ARS alone, from a node that has always been
     * warm for it. This -- not oscillation under noise, which the EWMA handles reasonably per the
     * test above -- is the concrete argument for blending in ReaderCacheAffinityRecorder's own
     * direct "this node warmed this shard at this time" bookkeeping rather than relying on ARS's
     * indirect latency signal alone for the specific cold-start moment D1 cares about.
     */
    public void testANeverQueriedNodeHasNoStatisticsAtAllNotAColdRanking() {
        collector.addNodeStatistics("warm-node", 0, WARM_RESPONSE_NANOS, SERVICE_TIME_NANOS);

        assertTrue("the warm node must have real statistics once queried", collector.getNodeStatistics("warm-node").isPresent());
        assertFalse(
            "a node ARS has never received a response-time sample for must have no statistics at "
                + "all -- confirming there is no signal here to distinguish 'genuinely warm' from "
                + "'never asked,' which is exactly the moment a direct warm/cold record would matter",
            collector.getNodeStatistics("never-queried-node").isPresent()
        );
    }
}
