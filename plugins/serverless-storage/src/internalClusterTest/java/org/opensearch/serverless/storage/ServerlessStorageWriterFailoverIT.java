/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * The end-to-end proof rfc-serverless-opensearch.md's whole WAL/crash-recovery effort (&sect;6.4,
 * &sect;7.1.2) has been building toward: a real multi-node cluster, a real writer node killed, and
 * the shard's data still there afterward -- not a unit-level proof of one piece in isolation, but
 * the actual failure this plugin exists to survive.
 *
 * <p>All nodes share one {@code serverless_storage.base_path} directory, standing in for the object
 * store every node in a real deployment would share -- that's what lets the survivor node see the
 * dead node's published manifest at all.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageWriterFailoverIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "serverless-failover-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    private Settings sharedNodeSettings(Path basePath) {
        return Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();
    }

    public void testWriterShardSurvivesItsNodeBeingKilled() throws Exception {
        Path sharedBasePath = createTempDir("serverless-storage-shared");
        Settings nodeSettings = sharedNodeSettings(sharedBasePath);

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        String node1 = internalCluster().startDataOnlyNode(nodeSettings);
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

        client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").get();
        // Publishing (rfc-serverless-opensearch.md &sect;7.1) only happens on flush, not on refresh
        // -- an explicit flush is what makes this document durable in a manifest the survivor node
        // can actually recover from, rather than relying on WAL replay to catch up something that
        // was never published at all.
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        refresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 1);

        // Whichever node actually holds the primary is the one that must die -- not necessarily
        // node1, since ServerlessStorageExistingShardsAllocator (like any allocator) is free to have
        // placed it on either data node.
        ShardRouting primaryShard = internalCluster().clusterService().state().routingTable().index(INDEX_NAME).shard(0).primaryShard();
        String primaryNodeId = primaryShard.currentNodeId();
        String primaryNodeName = internalCluster().clusterService().state().nodes().get(primaryNodeId).getName();

        internalCluster().stopRandomNode(settings -> primaryNodeName.equals(settings.get("node.name")));

        // The remaining data node is the only place this shard can go -- and it starts with a
        // completely empty local Store, exactly the scenario
        // WriterEngineFactory#recoverMissingLocalStore and ServerlessStorageExistingShardsAllocator
        // exist for (rfc-serverless-opensearch.md &sect;7.1.2). Without either, this would either
        // never leave UNASSIGNED (no allocator willing to place it) or fail recovery outright (no
        // local commit to read) -- ensureGreen succeeding at all is most of what this test proves.
        ensureGreen(INDEX_NAME);

        refresh(INDEX_NAME);
        SearchResponse response = client().prepareSearch(INDEX_NAME).setSize(0).get();
        assertHitCount(response, 1);
    }
}
