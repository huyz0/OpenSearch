/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.allocation;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;

/**
 * Proves {@link ReaderCacheAffinityRecorder#recordStarted} actually reaches real cluster state,
 * not just that {@link ReaderCacheAffinityMetadata}'s pure accessor logic is correct (see {@link
 * ReaderCacheAffinityMetadataTests} for that) -- the same "cluster-state update task really lands"
 * concern any {@code ClusterStateUpdateTask}-based feature needs proven end to end.
 */
public class ReaderCacheAffinityRecorderTests extends OpenSearchTestCase {

    private static final String INDEX_NAME = "serverless-idx";
    private static final String INDEX_UUID = "the-index-uuid";

    private TestThreadPool threadPool;
    private ClusterService clusterService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);

        IndexMetadata indexMetadata = IndexMetadata.builder(INDEX_NAME)
            .settings(settings(Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, INDEX_UUID))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterState withIndex = ClusterState.builder(clusterService.state())
            .metadata(Metadata.builder(clusterService.state().metadata()).put(indexMetadata, false))
            .build();
        ClusterServiceUtils.setState(clusterService, withIndex);
    }

    @Override
    public void tearDown() throws Exception {
        clusterService.stop();
        clusterService.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testRecordStartedPersistsAReadableAffinityIntoRealClusterState() throws Exception {
        ReaderCacheAffinityRecorder recorder = new ReaderCacheAffinityRecorder(clusterService);
        recorder.recordStarted(INDEX_UUID, 0, "node-with-warm-cache");

        assertBusy(() -> {
            IndexMetadata indexMetadata = clusterService.state().metadata().index(INDEX_NAME);
            assertEquals("node-with-warm-cache", ReaderCacheAffinityMetadata.preferredNodeId(indexMetadata, 0));
        });
    }

    public void testRecordStartedForAnUnknownIndexUuidIsANoOp() throws Exception {
        ReaderCacheAffinityRecorder recorder = new ReaderCacheAffinityRecorder(clusterService);
        recorder.recordStarted("no-such-index-uuid", 0, "some-node");
        // A second, real record for the actual index -- once this one lands, the (queued-earlier,
        // same-executor) unknown-index task above is guaranteed to have already been processed.
        recorder.recordStarted(INDEX_UUID, 0, "node-with-warm-cache");

        assertBusy(() -> {
            IndexMetadata indexMetadata = clusterService.state().metadata().index(INDEX_NAME);
            assertEquals("node-with-warm-cache", ReaderCacheAffinityMetadata.preferredNodeId(indexMetadata, 0));
        });
        assertNull(clusterService.state().metadata().index("no-such-index-uuid"));
    }
}
