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
import org.opensearch.serverless.storage.writerengine.action.ShardIdleTimeAction;
import org.opensearch.serverless.storage.writerengine.action.ShardIdleTimeRequest;
import org.opensearch.serverless.storage.writerengine.action.ShardIdleTimeResponse;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

/**
 * Proves {@link ShardIdleTimeAction} genuinely works over the transport layer against a real
 * writer shard in a real cluster -- not just a direct unit-level call into {@link
 * org.opensearch.serverless.storage.writerengine.ShardActivityRegistry} -- covering the Guice
 * injection of {@link ServerlessStoragePlugin} into {@code TransportShardIdleTimeAction} and the
 * real registration path ({@code WriterEngineFactory#newReadWriteEngine} registering the real
 * {@code ObjectStoreWriterEngine} it constructs), both of which only exist once the action and the
 * engine actually run inside a real node (rfc-serverless-opensearch.md &sect;16 Phase 4).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageShardIdleTimeActionIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "shard-idle-time-it-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testShardIdleTimeActionReportsARealWriterEnginesIdleTimeOverTransport() throws Exception {
        Path basePath = createTempDir("serverless-storage-shard-idle-time-it");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);

        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_NAME).getIndexUUID();

        // TransportShardIdleTimeAction only ever answers from its own receiving node's local
        // ShardActivityRegistry (see that class's own javadoc) -- it does no cross-node routing of
        // its own. client() picks an arbitrary node in the cluster, including the cluster-manager-only
        // node, which never hosts a shard at all; dataNodeClient() is what actually lands on the
        // one node that does.
        Client dataNodeClient = internalCluster().dataNodeClient();

        // Before any write, the engine is already registered (WriterEngineFactory#newReadWriteEngine
        // registers it as part of construction, not lazily on first write) and reports as tracked.
        ShardIdleTimeResponse beforeWrite = dataNodeClient.execute(ShardIdleTimeAction.INSTANCE, new ShardIdleTimeRequest(indexUuid, 0))
            .get();
        assertTrue(
            "a real writer engine must be registered as soon as it's constructed, not only after a first write",
            beforeWrite.tracked()
        );

        client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").get();

        ShardIdleTimeResponse afterWrite = dataNodeClient.execute(ShardIdleTimeAction.INSTANCE, new ShardIdleTimeRequest(indexUuid, 0))
            .get();
        assertTrue(afterWrite.tracked());
        assertTrue(
            "idle time right after a real write must be small, not carrying over some stale earlier value",
            afterWrite.millisSinceLastActivity() < 10_000
        );

        // A shard nothing was ever registered for on this node -- either a nonexistent index or,
        // as here, a shard number this single-shard index doesn't have -- must report not tracked,
        // not throw or silently report a stale/default value that could be mistaken for a real one.
        ShardIdleTimeResponse untrackedShard = dataNodeClient.execute(ShardIdleTimeAction.INSTANCE, new ShardIdleTimeRequest(indexUuid, 5))
            .get();
        assertFalse(untrackedShard.tracked());
    }
}
