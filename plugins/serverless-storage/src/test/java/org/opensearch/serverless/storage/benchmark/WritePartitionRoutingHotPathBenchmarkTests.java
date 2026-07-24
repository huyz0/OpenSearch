/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.benchmark;

import org.opensearch.Version;
import org.opensearch.action.index.IndexAction;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.storage.resharding.WritePartitionRoutingActionFilter;
import org.opensearch.serverless.storage.resharding.WritePartitionRoutingMetadata;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Times {@link WritePartitionRoutingActionFilter}'s per-document hot-path resolution -- the piece
 * every write against a write-routing-enabled alias runs through. The filter memoizes each alias's
 * {@code partitionIndex -> target index} table per {@link Metadata} snapshot (see that class's own
 * javadoc), so this test's actual regression target is narrower than "does the cache work at all"
 * (already covered functionally by {@code WritePartitionRoutingActionFilterTests}): it's whether a
 * cache-<em>hit</em> call still does any work that scales with the alias's target-index count.
 *
 * <p>It should not, but originally did: {@code rewriteIfAssigned} used to unconditionally build a
 * {@code .stream().map(...).toList()} transformation of every target index on every single call,
 * even on the hit path where the result was immediately discarded (only the cache-miss path, inside
 * {@code computeIfAbsent}'s lambda, ever read it). That made every hit's cost scale linearly with
 * target count -- the opposite of what memoization is for. Confirmed empirically while writing this
 * test: the scaling-ratio assertion below (which this test actually enforces) failed outright
 * against the pre-fix code (500-target hit-path p50 measured several times the 50-target one) and
 * passes comfortably against the fix -- unlike a single hit-vs-miss comparison, which stays true
 * either way since the cache-miss path (a real O(target count) table build) is slower than even an
 * O(target count) hit path at these target counts, so it doesn't distinguish the two.
 *
 * <p>Same "ratio/ordinal comparison, no absolute-time threshold" convention as {@link
 * ColdStartReaderEngineBenchmarkTests}/{@link BlobLatencyBenchmarkTests}: wall-clock numbers on
 * shared CI hardware are inherently noisy, so this compares hit-path p50 latency at two different
 * target counts (10x apart) against each other, not against a fixed absolute duration.
 */
public class WritePartitionRoutingHotPathBenchmarkTests extends OpenSearchTestCase {

    private static final String ALIAS = "write-alias";
    private static final int SMALL_TARGET_COUNT = 50;
    private static final int LARGE_TARGET_COUNT = 500;
    private static final int SAMPLED_HIT_CALLS = 2000;

    public void testCacheHitLatencyDoesNotScaleWithTargetCount() throws Exception {
        long smallTargetCountHitP50Nanos = medianHitLatencyNanos(SMALL_TARGET_COUNT);
        long largeTargetCountHitP50Nanos = medianHitLatencyNanos(LARGE_TARGET_COUNT);

        logger.info(
            "write-partition-routing hit-path p50: {} targets={}ns, {} targets={}ns",
            SMALL_TARGET_COUNT,
            smallTargetCountHitP50Nanos,
            LARGE_TARGET_COUNT,
            largeTargetCountHitP50Nanos
        );
        // The alias has 10x more targets in the second measurement; an O(target count) hit path
        // (the bug) would measure roughly 10x slower there too. A generous 3x ceiling comfortably
        // separates that from an O(1) hit path (the fix), which should barely move at all, while
        // still tolerating real sandbox-hardware noise between the two independent measurements.
        assertTrue(
            "a cache-hit call's latency must not scale with the alias's target-index count -- "
                + LARGE_TARGET_COUNT
                + "-target p50 ("
                + largeTargetCountHitP50Nanos
                + "ns) is more than 3x the "
                + SMALL_TARGET_COUNT
                + "-target p50 ("
                + smallTargetCountHitP50Nanos
                + "ns), consistent with a hit doing per-target work again",
            largeTargetCountHitP50Nanos < smallTargetCountHitP50Nanos * 3
        );
    }

    /** @return the median (p50) latency, in nanoseconds, of a cache-hit call against an alias with {@code targetCount} targets. */
    private static long medianHitLatencyNanos(int targetCount) throws Exception {
        TestThreadPool threadPool = new TestThreadPool("write-partition-routing-hot-path-benchmark");
        try {
            IndexMetadata[] targets = new IndexMetadata[targetCount];
            for (int i = 0; i < targetCount; i++) {
                targets[i] = partitionTarget("target-" + i, i, targetCount);
            }
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
            try {
                ClusterServiceUtils.setState(clusterService, stateWithTargets(targets));
                WritePartitionRoutingActionFilter filter = new WritePartitionRoutingActionFilter();
                filter.setDependencies(clusterService, threadPool);

                // First call against this Metadata snapshot is a genuine cache miss -- excluded from
                // the sampled measurements below, which are all cache hits against the same snapshot.
                timeOneCall(filter, "seed-doc");

                long[] hitNanos = new long[SAMPLED_HIT_CALLS];
                for (int i = 0; i < SAMPLED_HIT_CALLS; i++) {
                    hitNanos[i] = timeOneCall(filter, "doc-" + i);
                }
                Arrays.sort(hitNanos);
                return hitNanos[hitNanos.length / 2];
            } finally {
                clusterService.close();
            }
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    private static long timeOneCall(WritePartitionRoutingActionFilter filter, String id) {
        IndexRequest request = new IndexRequest(ALIAS).id(id).source("f", "v");
        AtomicReference<Boolean> proceeded = new AtomicReference<>(false);
        long start = System.nanoTime();
        filter.apply(null, IndexAction.NAME, request, null, null, recordingChain(proceeded));
        long elapsed = System.nanoTime() - start;
        assertTrue("chain must always proceed", proceeded.get());
        return elapsed;
    }

    private static ActionFilterChain<IndexRequest, IndexResponse> recordingChain(AtomicReference<Boolean> proceeded) {
        return (task, action, request, listener) -> proceeded.set(true);
    }

    private static IndexMetadata partitionTarget(String indexName, int partitionIndex, int numPartitions) {
        IndexMetadata base = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, indexName + "-uuid")
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder(ALIAS).build())
            .build();
        return WritePartitionRoutingMetadata.withAssignment(base, ALIAS, partitionIndex, numPartitions);
    }

    private static ClusterState stateWithTargets(IndexMetadata... targets) {
        Metadata.Builder metadata = Metadata.builder();
        for (IndexMetadata target : targets) {
            metadata.put(target, false);
        }
        return ClusterState.builder(new ClusterName("test")).metadata(metadata).build();
    }
}
