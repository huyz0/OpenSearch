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
import org.opensearch.serverless.storage.security.action.NodeObjectStoreRequestStatsAction;
import org.opensearch.serverless.storage.security.action.NodeObjectStoreRequestStatsRequest;
import org.opensearch.serverless.storage.security.action.NodeObjectStoreRequestStatsResponse;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;

import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Proves {@link NodeObjectStoreRequestStatsAction} genuinely reports a real writer shard's
 * object-store PUT traffic over the transport layer -- not just a direct unit-level call into
 * {@link org.opensearch.serverless.storage.security.ObjectStoreRequestCounter} -- closing
 * rfc-serverless-opensearch.md &sect;18 risk #1's own mitigation, "publish request-count metrics
 * from day one," which until now had no runtime signal at all.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageNodeObjectStoreRequestStatsActionIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "node-object-store-request-stats-it-idx";

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
            .build();
    }

    public void testNodeObjectStoreRequestStatsReportsARealWriterShardsPutTrafficOverTransport() throws Exception {
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

        client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        // TransportNodeObjectStoreRequestStatsAction only ever answers from its own receiving
        // node's local ObjectStoreRequestCounter (same single-node-scope contract as every other
        // action in this family) -- must be routed at the writer node specifically.
        Client dataNodeClient = internalCluster().client(dataNodeName);
        NodeObjectStoreRequestStatsResponse response = assertBusyReturning(
            () -> dataNodeClient.execute(NodeObjectStoreRequestStatsAction.INSTANCE, new NodeObjectStoreRequestStatsRequest()).get()
        );

        assertTrue(
            "a real flush must have driven at least one real writeBlobAtomic (bundle + manifest) through the counted container",
            response.putCount() > 0
        );
    }

    /**
     * Same shape as {@code ServerlessStorageNodeManifestLagActionIT}'s own helper: {@code
     * assertBusy}'s retry loop only catches {@code AssertionError}, not the plain {@code
     * ExecutionException} a transport call wraps failures in.
     */
    private <T> T assertBusyReturning(java.util.concurrent.Callable<T> callable) throws Exception {
        AtomicReference<T> result = new AtomicReference<>();
        assertBusy(() -> {
            try {
                result.set(callable.call());
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }, 30, TimeUnit.SECONDS);
        return result.get();
    }
}
