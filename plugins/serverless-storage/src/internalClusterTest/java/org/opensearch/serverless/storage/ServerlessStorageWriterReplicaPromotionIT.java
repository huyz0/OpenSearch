/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.exec.Indexer;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.writerengine.ObjectStoreWriterEngine;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Closes the B12/B13 gap <code>write-routing-and-term-authority-progress.md</code> left open:
 * every other crash-recovery test in {@link ServerlessStorageWriterFailoverIT} uses 0 writer
 * replicas, exercising only the already-race-free "shard freshly allocated to a node that never
 * held a copy before" activation path -- {@code Engine#onPrimaryTermBumped}'s own javadoc explains
 * why that path never needed a hook at all. This test is the other, real path: a genuine writer
 * <em>replica</em> (an already-constructed, already-open {@link ObjectStoreWriterEngine}) that
 * gets promoted to primary by core's own real replica-promotion machinery when its node's primary
 * copy is killed -- exactly the live-promotion case {@code IndexShard#bumpPrimaryTerm}'s
 * {@code onResponse} callback now calls {@code Engine#onPrimaryTermBumped} for.
 *
 * <p>Two things are checked, not just one: end-to-end correctness (nothing broke, no data lost --
 * the same black-box proof {@code ServerlessStorageWriterFailoverIT} already establishes for the
 * 0-replica path), and, directly, that the surviving replica's own {@link
 * ObjectStoreWriterEngine#activationWalPositionForTesting()} genuinely reflects a live re-snapshot
 * taken at promotion time, not the stale value from when it was originally constructed as a
 * replica (well before any of this test's own writes existed). Reaching the engine instance uses
 * {@code IndexShard#getIndexerOrNullForTesting()} -- a new, clearly-labeled test-only public
 * accessor, since this is a different Gradle source set/package than {@code IndexShard} itself
 * and this build's own {@code forbiddenApisInternalClusterTest} check forbids reflection as a way
 * around that.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageWriterReplicaPromotionIT extends ServerlessStorageIntegTestCase {

    private static final String INDEX_NAME = "serverless-writer-replica-promotion-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testLivePromotionOfAWriterReplicaReSnapshotsActivationWalPosition() throws Exception {
        Path sharedBasePath = createTempDir("serverless-storage-writer-replica-promotion");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", sharedBasePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), sharedBasePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey(), true)
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        // 1 replica, not 0 -- the whole point: both data nodes end up running a real, already-open
        // ObjectStoreWriterEngine (getEngineFactory selects it off ShardRouting#isSearchOnly(), not
        // primary()), so killing the primary specifically promotes an existing engine in place
        // rather than allocating a shard fresh onto an empty node.
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 1)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().prepareIndex(INDEX_NAME).setId("2").setSource("field", "value2").get();
        refresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 2);

        ClusterService clusterService = internalCluster().clusterService();
        ShardRouting primaryShardBeforeKill = clusterService.state().routingTable().index(INDEX_NAME).shard(0).primaryShard();
        String primaryNodeName = clusterService.state().nodes().get(primaryShardBeforeKill.currentNodeId()).getName();
        ShardRouting replicaShardBeforeKill = clusterService.state().routingTable().index(INDEX_NAME).shard(0).replicaShards().get(0);
        String replicaNodeNameBeforeKill = clusterService.state().nodes().get(replicaShardBeforeKill.currentNodeId()).getName();

        // The replica's own engine, read BEFORE the kill -- this is the stale, construction-time
        // snapshot this test proves gets corrected by promotion, not merely a number pulled out of
        // thin air to compare against.
        long replicaActivationWalPositionBeforeKill = activationWalPositionForTesting(replicaNodeNameBeforeKill);

        internalCluster().stopRandomNode(settings -> primaryNodeName.equals(settings.get("node.name")));

        // The surviving node is a genuine writer REPLICA being promoted, not a fresh empty shard
        // recovering from scratch -- ServerlessStorageWriterFailoverIT's own 0-replica tests never
        // exercise this. Only 2 data nodes total and 1 replica, so after one dies only a single
        // copy remains -- this cluster can never reach GREEN again (no third node to place a
        // replacement replica onto), so YELLOW is the correct, fully-recovered target here.
        // ensureYellow succeeding at all already proves the new Engine#onPrimaryTermBumped call
        // site (fired from inside IndexShard#bumpPrimaryTerm's onResponse callback) did not throw
        // and break promotion.
        ensureYellow(INDEX_NAME);

        refresh(INDEX_NAME);
        SearchResponse response = client().prepareSearch(INDEX_NAME).setSize(0).get();
        assertHitCount(response, 2);

        long replicaActivationWalPositionAfterPromotion = activationWalPositionForTesting(replicaNodeNameBeforeKill);
        assertTrue(
            "the promoted engine's activationWalPosition must reflect a live re-snapshot taken at "
                + "promotion time, not stay stuck at its stale, pre-kill, construction-time value -- "
                + "before="
                + replicaActivationWalPositionBeforeKill
                + ", after="
                + replicaActivationWalPositionAfterPromotion,
            replicaActivationWalPositionAfterPromotion > replicaActivationWalPositionBeforeKill
        );

        // The promoted engine must also still work correctly for new writes, not just have a
        // correctly re-snapshotted bound.
        client().prepareIndex(INDEX_NAME).setId("3").setSource("field", "value3").get();
        refresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 3);
    }

    /**
     * Reaches the real {@link ObjectStoreWriterEngine} instance for {@link #INDEX_NAME}'s shard 0
     * on {@code nodeName} via {@link IndexShard#getIndexerOrNullForTesting()}, then reads its
     * {@link ObjectStoreWriterEngine#activationWalPositionForTesting()}.
     */
    private long activationWalPositionForTesting(String nodeName) throws Exception {
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME));
        IndexShard shard = indexService.getShard(0);

        Indexer indexer = shard.getIndexerOrNullForTesting();

        return IndexShard.applyOnEngine(indexer, (Engine engine) -> {
            assertTrue("expected a real ObjectStoreWriterEngine, got " + engine.getClass(), engine instanceof ObjectStoreWriterEngine);
            return ((ObjectStoreWriterEngine) engine).activationWalPositionForTesting();
        });
    }
}
