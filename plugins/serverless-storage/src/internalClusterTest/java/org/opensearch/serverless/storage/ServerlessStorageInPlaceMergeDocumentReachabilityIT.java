/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.action.admin.indices.split.InPlaceMergeShardClusterStateUpdateRequest;
import org.opensearch.action.admin.indices.split.InPlaceSplitShardAction;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.metadata.MetadataInPlaceMergeShardService;
import org.opensearch.cluster.metadata.SplitShardsMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * dynamic-partitioning-plan.md Phase 2 item 2.1: the full real-workload proof that an in-place
 * <em>merge</em> reverses a split correctly, not merely that the metadata/routing bookkeeping lines
 * up. This is the test the whole resolved-design effort exists to justify -- it deliberately drives
 * the exact case an earlier "revive from one surviving child" spike would have silently corrupted:
 * both children take their own disjoint post-split writes, and pre-split documents are updated and
 * deleted on each child independently (so the two children reference the shared base segment with
 * divergent {@code liveDocs}), before the merge folds them back together.
 *
 * <p>Uses a real 2-node cluster and the plugin's real {@code ObjectStoreWriterEngine} (core's default
 * engine has no merge materialization seam at all, per {@code
 * EngineFactory#recoverInPlaceMergeLocalStore}'s default no-op). Asserts that after the round trip
 * every document is reachable with its correct final state -- pre-split originals (updated ones with
 * their new value, deleted ones gone), plus every post-split write from both children -- via both
 * scatter-gather search and real single-shard {@code GET}-by-id routing.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageInPlaceMergeDocumentReachabilityIT extends ServerlessStorageIntegTestCase {

    private static final String INDEX_NAME = "serverless-in-place-merge-reachability-idx";
    private static final int PRE_SPLIT_COUNT = 40;   // ids 0..39
    private static final int POST_SPLIT_COUNT = 20;  // ids 100..119
    private static final int UPDATE_COUNT = 5;        // ids 0..4 get a new value post-split
    private static final int DELETE_COUNT = 3;        // ids 10..12 get deleted post-split

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testSplitThenMergeRoundTripPreservesEveryDocumentsFinalState() throws Exception {
        Path sharedBasePath = createTempDir("serverless-storage-in-place-merge-reachability");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", sharedBasePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), sharedBasePath.toString())
            .build();

        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);

        // Pre-split workload: 40 documents, each with an original value we can later check was
        // preserved (or correctly overwritten / deleted) across the split-then-merge round trip.
        for (int i = 0; i < PRE_SPLIT_COUNT; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("field", "original-" + i, "padding", "x".repeat(256))
                .get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        refresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), PRE_SPLIT_COUNT);

        // Split shard 0 into two children and wait for the split to commit.
        client().execute(InPlaceSplitShardAction.INSTANCE, new InPlaceSplitShardAction.Request(INDEX_NAME, 0, 2)).actionGet();
        ClusterService clusterService = internalCluster().clusterService(clusterManagerNode);
        assertBusy(() -> {
            SplitShardsMetadata splitShardsMetadata = clusterService.state().metadata().index(INDEX_NAME).getSplitShardsMetadata();
            assertFalse("split should have committed by now", splitShardsMetadata.isSplitOfShardInProgress(0));
        });
        ensureGreen(INDEX_NAME);

        IndexRoutingTable afterSplit = clusterService.state().routingTable().index(INDEX_NAME);
        assertNull("parent shard 0 must be retired once the split commits", afterSplit.shard(0));
        assertNotNull("child shard 1 must be routed", afterSplit.shard(1));
        assertNotNull("child shard 2 must be routed", afterSplit.shard(2));

        // Post-split writes: 20 brand-new documents. Routing sends each to exactly one child by hash,
        // so the two children accumulate disjoint post-split segment sets in their own bundles.
        for (int i = 0; i < POST_SPLIT_COUNT; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(100 + i))
                .setSource("field", "post-" + (100 + i), "padding", "x".repeat(256))
                .get();
        }
        // Post-split updates of pre-split documents: each routes to whichever child owns its hash and
        // records the superseded copy as a per-child soft-delete against the shared base segment --
        // the divergent-liveDocs case the merge must reconcile.
        for (int i = 0; i < UPDATE_COUNT; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("field", "updated-" + i, "padding", "x".repeat(256))
                .get();
        }
        // Post-split deletes of pre-split documents, likewise routed to the owning child.
        for (int i = 10; i < 10 + DELETE_COUNT; i++) {
            client().prepareDelete(INDEX_NAME, String.valueOf(i)).get();
        }

        // Publish every child's post-split state so the merge hook reads it from each child's manifest.
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        refresh(INDEX_NAME);

        int expectedFinalCount = PRE_SPLIT_COUNT - DELETE_COUNT + POST_SPLIT_COUNT;
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), expectedFinalCount);

        // Trigger the in-place merge of shard 0's two children back into the parent. The
        // REST/transport entry point is built separately (step 3); here we drive the same
        // cluster-manager service the action will delegate to.
        MetadataInPlaceMergeShardService mergeService = new MetadataInPlaceMergeShardService(
            clusterService,
            internalCluster().getInstance(AllocationService.class, clusterManagerNode)
        );
        PlainActionFuture<ClusterStateUpdateResponse> mergeFuture = PlainActionFuture.newFuture();
        InPlaceMergeShardClusterStateUpdateRequest mergeRequest = new InPlaceMergeShardClusterStateUpdateRequest(
            "test-in-place-merge",
            INDEX_NAME,
            0
        );
        mergeRequest.ackTimeout(TimeValue.timeValueSeconds(30)).clusterManagerNodeTimeout(TimeValue.timeValueSeconds(30));
        mergeService.merge(mergeRequest, mergeFuture);
        assertTrue("merge cluster-state update must be acknowledged", mergeFuture.actionGet().isAcknowledged());

        // The parent is revived and both children are retired.
        assertBusy(() -> {
            IndexRoutingTable afterMerge = clusterService.state().routingTable().index(INDEX_NAME);
            assertNotNull("parent shard 0 must be revived once the merge commits", afterMerge.shard(0));
            assertNull("child shard 1 must be retired by the merge", afterMerge.shard(1));
            assertNull("child shard 2 must be retired by the merge", afterMerge.shard(2));
        });
        ensureGreen(INDEX_NAME);
        refresh(INDEX_NAME);

        // (a) Every document's final state survives the round trip, by count first...
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), expectedFinalCount);

        // (b) ...then by real single-shard GET-by-id routing (now resolving back to the revived
        // parent shard 0), checking each class of document individually.
        for (int i = 0; i < UPDATE_COUNT; i++) {
            GetResponse updated = client().prepareGet(INDEX_NAME, String.valueOf(i)).get();
            assertTrue("updated pre-split document [" + i + "] must survive the merge", updated.isExists());
            assertEquals("its post-split update must win", "updated-" + i, updated.getSource().get("field"));
        }
        for (int i = 10; i < 10 + DELETE_COUNT; i++) {
            GetResponse deleted = client().prepareGet(INDEX_NAME, String.valueOf(i)).get();
            assertFalse("deleted pre-split document [" + i + "] must stay gone after the merge", deleted.isExists());
        }
        for (int i = UPDATE_COUNT; i < PRE_SPLIT_COUNT; i++) {
            if (i >= 10 && i < 10 + DELETE_COUNT) {
                continue; // deleted above
            }
            GetResponse original = client().prepareGet(INDEX_NAME, String.valueOf(i)).get();
            assertTrue("untouched pre-split document [" + i + "] must survive the merge", original.isExists());
            assertEquals("its original value must be intact", "original-" + i, original.getSource().get("field"));
        }
        for (int i = 0; i < POST_SPLIT_COUNT; i++) {
            int id = 100 + i;
            GetResponse post = client().prepareGet(INDEX_NAME, String.valueOf(id)).get();
            assertTrue("post-split document [" + id + "] must be folded into the merged parent", post.isExists());
            assertEquals("post-" + id, post.getSource().get("field"));
        }
    }
}
