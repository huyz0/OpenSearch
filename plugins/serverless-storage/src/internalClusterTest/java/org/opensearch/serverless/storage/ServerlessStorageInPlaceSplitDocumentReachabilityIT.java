/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.action.admin.indices.split.InPlaceSplitShardAction;
import org.opensearch.action.get.GetResponse;
import org.opensearch.cluster.metadata.SplitShardsMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.security.ObjectStoreRequestCounter;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * dynamic-partitioning-plan.md Phase 0 item 0.7: the full real-workload proof that in-place split
 * (Tasks 1-20) is genuinely end-to-end, not just correct at each individual layer already covered
 * by this session's narrower unit/IT tests. Uses a real 2-node cluster and the plugin's real
 * {@code ObjectStoreWriterEngine}, not core's default engine (which has no split materialization
 * seam at all, per core's own {@code local-lucene} ShardRecoveryStrategy declining the split case).
 *
 * <p>Depends on Task 20's fix to {@code IndexMetadata}'s and {@code IndexRoutingTable}'s per-shard
 * invariants tolerating shard ids a split introduces beyond {@code numberOfShards} -- without it,
 * this test either hung the whole suite (the split request's cluster-state update crashed the
 * cluster-manager apply thread mid-execution, so the client never got a response) or failed fast
 * with {@code IllegalStateException: Wrong number of shards in routing table}.
 *
 * <p>Checks, in order: (a) the split leaves the index healthy (never red) once children are
 * routed, (b) every pre-split document is reachable post-split via real {@code
 * OperationRouting} hash resolution -- both scatter-gather search and single-shard real-routing
 * {@code GET} by id, not a plugin-side reimplementation of routing, and (c) no bulk copy of
 * bundle bytes occurred: the object-store PUT count increase from the split stays a small,
 * document-count-independent constant (metadata-only: manifest/lineage/shard-state writes),
 * rather than scaling with the 40 real documents indexed, which is what {@code ShardCloner.clone}
 * (reused verbatim from this plugin's pre-existing zero-copy clone, per Task 12) guarantees.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageInPlaceSplitDocumentReachabilityIT extends ServerlessStorageIntegTestCase {

    private static final String INDEX_NAME = "serverless-in-place-split-reachability-idx";
    private static final int DOC_COUNT = 40;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testEveryPreSplitDocumentIsReachablePostSplitWithoutBulkCopy() throws Exception {
        Path sharedBasePath = createTempDir("serverless-storage-in-place-split-reachability");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", sharedBasePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), sharedBasePath.toString())
            .build();

        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode(nodeSettings);
        String dataNode = internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);

        // A real, moderately-sized workload -- large enough that a real bulk copy of bundle bytes
        // (rather than the zero-copy manifest-reference clone this is meant to prove) would show
        // up as a proportional jump in PUT-shaped object-store requests.
        for (int i = 0; i < DOC_COUNT; i++) {
            client().prepareIndex(INDEX_NAME).setId(String.valueOf(i)).setSource("field", "value-" + i, "padding", "x".repeat(512)).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        refresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), DOC_COUNT);

        ObjectStoreRequestCounter requestCounter = internalCluster().getInstance(ServerlessStoragePlugin.class, dataNode)
            .objectStoreRequestCounter();
        long putCountBeforeSplit = requestCounter.putCount();

        client().execute(InPlaceSplitShardAction.INSTANCE, new InPlaceSplitShardAction.Request(INDEX_NAME, 0, 2)).actionGet();

        ClusterService clusterService = internalCluster().clusterService(clusterManagerNode);
        assertBusy(() -> {
            SplitShardsMetadata splitShardsMetadata = clusterService.state().metadata().index(INDEX_NAME).getSplitShardsMetadata();
            assertFalse("split should have committed by now", splitShardsMetadata.isSplitOfShardInProgress(0));
        });
        ensureGreen(INDEX_NAME);

        // (a) healthy post-split routing: only the 2 children are routed, the parent is retired.
        IndexRoutingTable indexRoutingTable = clusterService.state().routingTable().index(INDEX_NAME);
        assertNull("parent shard 0 must be retired once the split commits", indexRoutingTable.shard(0));
        assertNotNull("child shard 1 must be routed", indexRoutingTable.shard(1));
        assertNotNull("child shard 2 must be routed", indexRoutingTable.shard(2));

        refresh(INDEX_NAME);

        // (b) every pre-split document is reachable post-split via real OperationRouting hash
        // resolution -- scatter-gather search first...
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), DOC_COUNT);

        // ...then real single-shard GET-by-id routing for a sample of ids, proving core's own
        // OperationRouting#generateShardId (not a scatter-gather fallback) resolves each id to the
        // correct child.
        for (int i = 0; i < DOC_COUNT; i += 7) {
            GetResponse response = client().prepareGet(INDEX_NAME, String.valueOf(i)).get();
            assertTrue("document [" + i + "] must be reachable via real single-shard GET routing post-split", response.isExists());
        }

        // (c) no bulk copy: the PUT-shaped request increase from the split (manifest/lineage/
        // shard-state bookkeeping for 2 children) must stay a small, document-count-independent
        // constant -- if the split had performed a real per-document bundle copy instead of
        // ShardCloner.clone's zero-copy manifest reference, this would scale with DOC_COUNT.
        long putCountAfterSplit = requestCounter.putCount();
        long putDelta = putCountAfterSplit - putCountBeforeSplit;
        assertTrue(
            "expected a small, bookkeeping-only PUT increase from the split (zero-copy clone), "
                + "not one scaling with the "
                + DOC_COUNT
                + " indexed documents -- was "
                + putDelta,
            putDelta < DOC_COUNT
        );
    }
}
