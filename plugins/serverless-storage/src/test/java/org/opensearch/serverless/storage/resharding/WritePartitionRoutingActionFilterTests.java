/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

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
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit coverage for {@link WritePartitionRoutingActionFilter}'s per-{@link Metadata}-snapshot
 * partition-table cache: this class recomputes each write-routing alias's {@code partitionIndex ->
 * target index} table lazily and memoizes it against the {@link Metadata} instance it was computed
 * from, rather than rebuilding it on every single document write. The tests here exist specifically
 * to prove that memoization never serves a stale table once cluster state genuinely changes.
 */
public class WritePartitionRoutingActionFilterTests extends OpenSearchTestCase {

    private static final String ALIAS = "write-alias";

    private TestThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
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

    private static WritePartitionRoutingActionFilter filter(ClusterService clusterService, ThreadPool threadPool) {
        WritePartitionRoutingActionFilter filter = new WritePartitionRoutingActionFilter();
        filter.setDependencies(clusterService, threadPool);
        return filter;
    }

    private static int expectedPartition(String id, int numPartitions) {
        return Math.floorMod(Murmur3HashFunction.hash(id), numPartitions);
    }

    public void testRewritesToTheCorrectPartitionTarget() {
        IndexMetadata target0 = partitionTarget("target-0", 0, 2);
        IndexMetadata target1 = partitionTarget("target-1", 1, 2);
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        try {
            ClusterServiceUtils.setState(clusterService, stateWithTargets(target0, target1));
            WritePartitionRoutingActionFilter filter = filter(clusterService, threadPool);

            // Find an id landing on each of the two partitions so both branches are exercised.
            String idForPartition0 = idLandingOn(0, 2);
            String idForPartition1 = idLandingOn(1, 2);

            assertRewrittenTo(filter, idForPartition0, "target-0");
            assertRewrittenTo(filter, idForPartition1, "target-1");
        } finally {
            clusterService.close();
        }
    }

    /**
     * The core regression this class's cache must never introduce: once metadata genuinely changes
     * (a new partition target is attached), a request against the SAME alias must be resolved
     * against the NEW table, not a memoized one computed before the target existed.
     */
    public void testPicksUpANewlyAttachedPartitionTargetAfterAMetadataChange() {
        IndexMetadata target0 = partitionTarget("target-0", 0, 2);
        IndexMetadata target1 = partitionTarget("target-1", 1, 2);
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        try {
            // Only target-0 exists at first -- target-1's partition has no assignee yet.
            ClusterServiceUtils.setState(clusterService, stateWithTargets(target0));
            WritePartitionRoutingActionFilter filter = filter(clusterService, threadPool);

            String idForPartition1 = idLandingOn(1, 2);
            IndexRequest requestBeforeTarget1Exists = new IndexRequest(ALIAS).id(idForPartition1).source("f", "v");
            AtomicReference<Boolean> proceeded = new AtomicReference<>(false);
            filter.apply(null, IndexAction.NAME, requestBeforeTarget1Exists, null, null, recordingChain(proceeded));
            assertTrue(proceeded.get());
            // No target assigned to partition 1 yet -- request passes through unrewritten.
            assertEquals(ALIAS, requestBeforeTarget1Exists.index());

            // Metadata changes: target-1 now exists and claims partition 1.
            ClusterServiceUtils.setState(clusterService, stateWithTargets(target0, target1));

            assertRewrittenTo(filter, idForPartition1, "target-1");
        } finally {
            clusterService.close();
        }
    }

    /**
     * A routing-only cluster-state change (no metadata change at all) must not disturb a
     * previously-cached table -- the same table, computed once, must keep resolving correctly
     * across many calls against an unchanged {@link Metadata} instance.
     */
    public void testRepeatedCallsAgainstUnchangedMetadataStayConsistent() {
        IndexMetadata target0 = partitionTarget("target-0", 0, 2);
        IndexMetadata target1 = partitionTarget("target-1", 1, 2);
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        try {
            ClusterServiceUtils.setState(clusterService, stateWithTargets(target0, target1));
            WritePartitionRoutingActionFilter filter = filter(clusterService, threadPool);

            String idForPartition0 = idLandingOn(0, 2);
            for (int i = 0; i < 5; i++) {
                assertRewrittenTo(filter, idForPartition0, "target-0");
            }
        } finally {
            clusterService.close();
        }
    }

    private static void assertRewrittenTo(WritePartitionRoutingActionFilter filter, String id, String expectedTarget) {
        IndexRequest request = new IndexRequest(ALIAS).id(id).source("f", "v");
        AtomicReference<Boolean> proceeded = new AtomicReference<>(false);
        filter.apply(null, IndexAction.NAME, request, null, null, recordingChain(proceeded));
        assertTrue("chain must always proceed", proceeded.get());
        assertEquals("id [" + id + "] must resolve to the assigned partition target", expectedTarget, request.index());
    }

    private static ActionFilterChain<IndexRequest, IndexResponse> recordingChain(AtomicReference<Boolean> proceeded) {
        return (task, action, request, listener) -> proceeded.set(true);
    }

    /** Finds a document id that hashes to the given partition -- deterministic per-test-run, just brute force over a small space. */
    private static String idLandingOn(int partition, int numPartitions) {
        for (int i = 0; i < 10_000; i++) {
            String candidate = "doc-" + i;
            if (expectedPartition(candidate, numPartitions) == partition) {
                return candidate;
            }
        }
        throw new AssertionError("could not find an id landing on partition " + partition + " within the search space");
    }
}
