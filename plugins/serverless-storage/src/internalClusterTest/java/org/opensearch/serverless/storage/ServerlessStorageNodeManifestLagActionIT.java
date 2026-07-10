/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.Preference;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.serverless.storage.allocation.ReaderShardPlacementAllocationDecider;
import org.opensearch.serverless.storage.readerengine.action.NodeManifestLagAction;
import org.opensearch.serverless.storage.readerengine.action.NodeManifestLagRequest;
import org.opensearch.serverless.storage.readerengine.action.NodeManifestLagResponse;
import org.opensearch.serverless.storage.readerengine.action.ShardLagEntry;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;

import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Proves {@link NodeManifestLagAction} genuinely reports a real search-only reader shard's
 * manifest-generation lag over the transport layer -- not just a direct unit-level call into
 * {@link org.opensearch.serverless.storage.readerengine.ReaderShardActivityRegistry#snapshotAll} --
 * the &sect;10 "search tier: manifest-generation lag" autoscaling hook's own signal-collection
 * half. Same real search-only-replica cluster shape {@code ServerlessStorageSearchOnlyReplicaIT}
 * already proved a reader engine actually opens and serves reads under.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageNodeManifestLagActionIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "node-manifest-lag-it-idx";

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
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), sharedPath.toString())
            .build();
    }

    public void testNodeManifestLagReportsARealSearchOnlyReaderShardsLagOverTransport() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureYellow(INDEX_NAME);

        String searchOnlyNodeName = internalCluster().startNode(
            Settings.builder()
                .put(nodeSettings(0))
                .put("node.roles", "search")
                .put("node.attr." + ReaderShardPlacementAllocationDecider.READER_NODE_ATTRIBUTE, "true")
                .build()
        );
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        // Same "reader engine only picks up a newer manifest on its own background poll" reasoning
        // ServerlessStorageSearchOnlyReplicaIT already documents -- wait for the search-only copy to
        // actually serve the just-flushed document before asserting anything about its lag.
        assertBusy(() -> {
            org.opensearch.action.search.SearchResponse response = client().prepareSearch(INDEX_NAME)
                .setPreference(Preference.SEARCH_REPLICA.type())
                .setSize(0)
                .get();
            org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount(response, 1);
        }, 30, TimeUnit.SECONDS);

        // TransportNodeManifestLagAction only ever answers from its own receiving node's local
        // ReaderShardActivityRegistry (same single-node-scope contract as every other action in this
        // family) -- must be routed at the search-only node specifically, not an arbitrary client.
        Client searchOnlyNodeClient = internalCluster().client(searchOnlyNodeName);

        NodeManifestLagResponse response = assertBusyReturning(
            () -> searchOnlyNodeClient.execute(NodeManifestLagAction.INSTANCE, new NodeManifestLagRequest()).get()
        );

        assertEquals(
            "exactly the one reader shard on this node must be listed -- registration happens as "
                + "part of ReaderEngineFactory#newReadWriteEngine, not lazily",
            1,
            response.entries().size()
        );
        ShardLagEntry entry = response.entries().get(0);
        assertEquals(0, entry.shardId());
        assertEquals(
            "having already caught up to serve the just-flushed document (per the assertBusy search "
                + "above), this shard's own lag must now read zero",
            0L,
            entry.manifestGenerationLag()
        );
    }

    /**
     * Same shape as {@code ServerlessStorageSnapshotRestoreActionIT}'s own helper: {@code
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
