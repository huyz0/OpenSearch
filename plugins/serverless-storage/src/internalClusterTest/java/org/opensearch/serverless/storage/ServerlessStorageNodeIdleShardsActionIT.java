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
import org.opensearch.serverless.storage.writerengine.action.IdleShardEntry;
import org.opensearch.serverless.storage.writerengine.action.NodeIdleShardsAction;
import org.opensearch.serverless.storage.writerengine.action.NodeIdleShardsRequest;
import org.opensearch.serverless.storage.writerengine.action.NodeIdleShardsResponse;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

/**
 * Proves {@link NodeIdleShardsAction} genuinely lists every writer shard tracked on a real node in
 * a real cluster -- not just a direct unit-level call into {@link
 * org.opensearch.serverless.storage.writerengine.ShardActivityRegistry#snapshotAll} -- the
 * "signal collection" half of &sect;7.3/&sect;10's still-open autoscaling story
 * (rfc-serverless-opensearch.md), letting an external controller discover idle shards on a node
 * without already knowing which (indexUuid, shardId) pairs to ask {@code ShardIdleTimeAction}
 * about one at a time.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageNodeIdleShardsActionIT extends OpenSearchIntegTestCase {

    private static final String FIRST_INDEX = "node-idle-shards-it-idx-a";
    private static final String SECOND_INDEX = "node-idle-shards-it-idx-b";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testNodeIdleShardsListsEveryWriterShardTrackedOnThatNode() throws Exception {
        Path basePath = createTempDir("serverless-storage-node-idle-shards-it");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        Settings.Builder indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true);
        createIndex(FIRST_INDEX, indexSettings.build());
        createIndex(SECOND_INDEX, indexSettings.build());
        ensureGreen(FIRST_INDEX, SECOND_INDEX);

        String firstIndexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(FIRST_INDEX).getIndexUUID();
        String secondIndexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(SECOND_INDEX).getIndexUUID();

        // Same node-routing requirement as ShardIdleTimeAction: this action only ever answers from
        // its own receiving node's local ShardActivityRegistry, so client() (which could pick the
        // cluster-manager-only node that hosts no shards) would silently return zero entries.
        Client dataNodeClient = internalCluster().dataNodeClient();

        NodeIdleShardsResponse response = dataNodeClient.execute(NodeIdleShardsAction.INSTANCE, new NodeIdleShardsRequest()).get();

        assertEquals(
            "both writer shards on this node must be listed -- registration happens at engine "
                + "construction time, not lazily on first write, same as ShardIdleTimeAction's own "
                + "before-any-write assertion",
            2,
            response.entries().size()
        );
        assertTrue(
            "the first index's shard must appear with its own indexUuid, not the second index's or a mangled key",
            response.entries().stream().anyMatch(e -> e.indexUuid().equals(firstIndexUuid) && e.shardId() == 0)
        );
        assertTrue(
            "the second index's shard must appear too, independently of the first",
            response.entries().stream().anyMatch(e -> e.indexUuid().equals(secondIndexUuid) && e.shardId() == 0)
        );
        for (IdleShardEntry entry : response.entries()) {
            assertTrue("every entry's idle time must be a real non-negative measurement", entry.millisSinceLastActivity() >= 0);
        }
    }
}
