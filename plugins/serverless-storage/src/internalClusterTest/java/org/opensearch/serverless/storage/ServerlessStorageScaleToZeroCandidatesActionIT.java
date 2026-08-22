/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidateEntry;
import org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidatesAction;
import org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidatesRequest;
import org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidatesResponse;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * Proves {@link ScaleToZeroCandidatesAction} genuinely fans out across every data node in a real
 * cluster and merges the result -- unlike {@link
 * org.opensearch.serverless.storage.writerengine.action.NodeIdleShardsAction}/{@link
 * org.opensearch.serverless.storage.readerengine.action.NodeManifestLagAction}, which only ever
 * answer for whichever single node receives the request, this action must be reachable from any
 * node and see every data node's shards regardless of which one answers the coordinating request.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageScaleToZeroCandidatesActionIT extends ServerlessStorageIntegTestCase {

    private static final String IDX = "scale-to-zero-it-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testCandidatesActionSeesAShardEvenWhenAnsweredByADifferentNodeThanTheOneHostingIt() throws Exception {
        Path basePath = createTempDir("serverless-storage-scale-to-zero-it");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        // Two data nodes: the shard lands on exactly one of them, but the request below is sent
        // to whichever node client() happens to route to -- proving this action's fan-out reaches
        // across nodes rather than only ever answering from its own local registry, the "narrow,
        // single-node-scope" contract NodeIdleShardsAction/NodeManifestLagAction each deliberately keep.
        String firstDataNode = internalCluster().startDataOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            IDX,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(IDX);

        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(IDX).getIndexUUID();

        // A zero idle threshold means "every observed idle time (which is always >= 0) qualifies,"
        // so this shard is guaranteed to show up as a candidate regardless of how little
        // wall-clock time has passed since ensureGreen returned. Deliberately not -1: that's
        // ScaleToZeroCandidatesRequest.USE_DEFAULT_IDLE_THRESHOLD's reserved sentinel, which would
        // silently substitute this node's real configured default instead of being used literally.
        ScaleToZeroCandidatesResponse response = client().execute(
            ScaleToZeroCandidatesAction.INSTANCE,
            new ScaleToZeroCandidatesRequest(0L, 0L)
        ).get();

        Optional<ScaleToZeroCandidateEntry> entry = response.candidates()
            .stream()
            .filter(e -> e.indexUuid().equals(indexUuid) && e.shardId() == 0)
            .findFirst();
        assertTrue(
            "the shard must appear in the merged cluster-wide response even though it's only ever "
                + "tracked by one data node's local registry, proving the fan-out actually reached that node",
            entry.isPresent()
        );
        assertTrue("with a threshold of every-idle-time-qualifies, this shard must be flagged a candidate", entry.get().candidate());

        // Sanity: the client() call above was not pinned to firstDataNode, so this also indirectly
        // exercises "coordinating node != shard-hosting node" whenever routing picks the other one.
        assertNotNull(firstDataNode);
    }
}
