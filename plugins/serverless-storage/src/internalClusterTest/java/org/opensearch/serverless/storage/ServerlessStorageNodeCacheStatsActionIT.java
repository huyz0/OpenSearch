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
import org.opensearch.serverless.storage.format.action.NodeCacheStatsAction;
import org.opensearch.serverless.storage.format.action.NodeCacheStatsRequest;
import org.opensearch.serverless.storage.format.action.NodeCacheStatsResponse;
import org.opensearch.serverless.storage.format.action.ShardCacheStatsEntry;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;

import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Proves {@link NodeCacheStatsAction} genuinely reports a real search-only reader shard's local
 * disk cache stats over the transport layer -- not just a direct unit-level call into {@link
 * org.opensearch.serverless.storage.format.CacheStatsRegistry#snapshotAll} -- closing
 * rfc-serverless-opensearch.md &sect;9's "cache hit-rate and cold-read latency are first-class
 * metrics" gap the same way {@code ServerlessStorageNodeManifestLagActionIT} already proved for
 * manifest-generation lag.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageNodeCacheStatsActionIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "node-cache-stats-it-idx";

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

    public void testNodeCacheStatsReportsARealSearchOnlyReaderShardsDiskCacheStatsOverTransport() throws Exception {
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
        // actually serve the just-flushed document, so its local disk cache has genuinely fetched
        // real segment files through the object store (not just been constructed).
        Client searchOnlyNodeClient = internalCluster().client(searchOnlyNodeName);
        assertBusy(() -> {
            org.opensearch.action.search.SearchResponse response = client().prepareSearch(INDEX_NAME)
                .setPreference(Preference.SEARCH_REPLICA.type())
                .setSize(0)
                .get();
            org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount(response, 1);
        }, 30, TimeUnit.SECONDS);

        // TransportNodeCacheStatsAction only ever answers from its own receiving node's local
        // CacheStatsRegistry (same single-node-scope contract as every other action in this
        // family) -- must be routed at the search-only node specifically, not an arbitrary client.
        NodeCacheStatsResponse response = assertBusyReturning(
            () -> searchOnlyNodeClient.execute(NodeCacheStatsAction.INSTANCE, new NodeCacheStatsRequest()).get()
        );

        assertEquals(
            "exactly the one reader shard on this node must be listed -- registration happens as "
                + "part of getEngineFactory's reader-shard branch, not lazily",
            1,
            response.diskCacheEntries().size()
        );
        ShardCacheStatsEntry entry = response.diskCacheEntries().get(0);
        assertEquals(0, entry.shardId());
        // Not hitCount(): ObjectStoreCommitMaterializer skips any file already present in the
        // shard's local Lucene directory without ever consulting the disk cache at all (see its own
        // javadoc), so a single reader engine's first materialization is *only* ever misses -- a
        // real hit only happens across two independent materializations (e.g. a restart) sharing the
        // same on-disk cache directory, which LocalDiskCachingBundleStoreTests already covers at the
        // unit level. What this IT proves is the wiring: a real reader shard's real object-store
        // reads actually flow through the registered disk cache and are visible over transport.
        assertTrue("the shard's first materialization must have driven real fetches through its disk cache", entry.missCount() > 0);
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
