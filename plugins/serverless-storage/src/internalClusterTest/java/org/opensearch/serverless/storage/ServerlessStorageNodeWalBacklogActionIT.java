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
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.serverless.storage.wal.WalChunkService;
import org.opensearch.serverless.storage.wal.WalRecord;
import org.opensearch.serverless.storage.wal.action.NodeWalBacklogAction;
import org.opensearch.serverless.storage.wal.action.NodeWalBacklogRequest;
import org.opensearch.serverless.storage.wal.action.NodeWalBacklogResponse;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;

import java.nio.file.Path;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Proves {@link NodeWalBacklogAction} genuinely reports the real, live node-shared {@link
 * WalChunkService} instance's state over the transport layer -- not a stale or separate copy --
 * closing rfc-serverless-opensearch.md &sect;10's still-open "ingest tier: WAL upload backlog"
 * autoscaling hook. {@code WalMirroringTranslog#add} flushes after every single operation (see its
 * own javadoc: "gives the same per-write durability guarantee a local translog fsync gives"), so
 * ordinary indexing through the real translog path never leaves anything buffered long enough to
 * observe -- this test instead appends directly to the node's own live {@code WalChunkService} (the
 * same instance {@code TransportNodeWalBacklogAction} reads from) to prove the wiring reflects real,
 * live state rather than being disconnected from it.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageNodeWalBacklogActionIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "node-wal-backlog-it-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(ServerlessStoragePlugin.class)).collect(Collectors.toList());
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    private volatile Path sharedBasePath;

    private Path serverlessStorageBasePath() {
        if (sharedBasePath == null) {
            synchronized (this) {
                if (sharedBasePath == null) {
                    sharedBasePath = randomRepoPath();
                }
            }
        }
        return sharedBasePath;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Path sharedPath = serverlessStorageBasePath();
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), sharedPath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey(), true)
            .build();
    }

    public void testNodeWalBacklogReflectsTheRealLiveWalChunkServiceOverTransport() throws Exception {
        String dataNodeName = internalCluster().startNode(nodeSettings(0));

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);

        Client dataNodeClient = internalCluster().client(dataNodeName);
        ServerlessStoragePlugin plugin = internalCluster().getInstance(ServerlessStoragePlugin.class, dataNodeName);
        WalChunkService walChunkService = plugin.sharedWalChunkService();
        assertNotNull("WAL mirroring is enabled on this node, so a shared WalChunkService must exist", walChunkService);

        NodeWalBacklogResponse beforeAppend = dataNodeClient.execute(NodeWalBacklogAction.INSTANCE, new NodeWalBacklogRequest()).get();
        assertEquals("nothing appended yet", 0, beforeAppend.bufferedRecordCount());
        assertEquals("nothing appended yet", 0L, beforeAppend.totalBufferedBytes());

        // Append directly to the node's own live service, bypassing the translog's per-operation
        // auto-flush (see this class's own javadoc) so there is real backlog to observe.
        walChunkService.append(new WalRecord("some-other-idx", 0, 1, 0, new byte[100]));
        walChunkService.append(new WalRecord("some-other-idx", 1, 1, 0, new byte[50]));

        NodeWalBacklogResponse afterAppend = dataNodeClient.execute(NodeWalBacklogAction.INSTANCE, new NodeWalBacklogRequest()).get();
        assertEquals(
            "the transport action must report the same live service's real buffered record count",
            2,
            afterAppend.bufferedRecordCount()
        );
        assertEquals(
            "the transport action must report the same live service's real buffered payload bytes",
            150L,
            afterAppend.totalBufferedBytes()
        );

        walChunkService.flush();
        NodeWalBacklogResponse afterFlush = dataNodeClient.execute(NodeWalBacklogAction.INSTANCE, new NodeWalBacklogRequest()).get();
        assertEquals("a flush on the live service must be reflected immediately, not cached stale", 0, afterFlush.bufferedRecordCount());
        assertEquals(0L, afterFlush.totalBufferedBytes());
    }
}
