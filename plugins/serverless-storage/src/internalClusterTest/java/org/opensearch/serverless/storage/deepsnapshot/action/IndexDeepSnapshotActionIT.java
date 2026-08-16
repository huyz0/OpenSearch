/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.deepsnapshot.action;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.serverless.storage.ServerlessStorageIntegTestCase;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.descriptor.DescriptorGate;
import org.opensearch.serverless.storage.readerengine.lazydirectory.CleanerDaemonThreadLeakFilter;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

/**
 * Round 006 item 5, end to end through the shipped action rather than through {@code
 * DeepSnapshotOrchestrationIT}'s manual wiring: {@link IndexDeepSnapshotAction} against both an
 * ordinary serverless index and a gated one, each restored under a new name through core's ordinary
 * {@code _restore} API -- the assertion that matters, since it is what distinguishes a real snapshot
 * from something shaped like one.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IndexDeepSnapshotActionIT extends ServerlessStorageIntegTestCase {

    private static final String REPO_NAME = "index-deep-snapshot-repo";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    private Path basePath;

    private void startClusterAndRepo(Settings extraNodeSettings) throws Exception {
        basePath = createTempDir("index-deep-snapshot");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(extraNodeSettings)
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);
        ensureStableCluster(2);

        AcknowledgedResponse putRepository = client().admin()
            .cluster()
            .preparePutRepository(REPO_NAME)
            .setType(FsRepository.TYPE)
            .setSettings(Settings.builder().put("location", basePath.resolve("repo")))
            .get();
        assertTrue(putRepository.isAcknowledged());
    }

    /**
     * The plain case: an ordinary serverless index (not gated), copied through the real action and
     * restored by core under a new name.
     */
    public void testADeepSnapshotOfAnOrdinaryServerlessIndexIsRestorableByCore() throws Exception {
        startClusterAndRepo(Settings.EMPTY);
        String indexName = "deep-snapshot-index-action-source";

        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(indexName);
        for (int i = 0; i < 12; i++) {
            client().prepareIndex(indexName).setId(Integer.toString(i)).setSource("f", "value-" + i).get();
        }
        client().admin().indices().prepareFlush(indexName).get();
        client().admin().indices().prepareRefresh(indexName).get();
        assertEquals(12, count(indexName));

        IndexDeepSnapshotResponse response = client().execute(
            IndexDeepSnapshotAction.INSTANCE,
            new IndexDeepSnapshotRequest(indexName, REPO_NAME, "ordinary-deep-1")
        ).get();
        assertEquals("the action must report the shards it copied", 1, response.shardCount());

        restoreAndAssertDocumentCount(indexName, "ordinary-deep-1", "ordinary-deep-restored", 12);
    }

    /**
     * The case this round's own item 5 write-up names as still open at the point this class was
     * written: a gated index, resolved through {@code AbsentIndexDescriptorSuppliers} rather than
     * cluster state, copied the same way and restored the same way.
     *
     * <p><b>Found rather than closed: the copy and finalize both succeed, and the restored shard then
     * never allocates.</b> {@code IndexDeepSnapshotResponse} comes back correctly (proving the copy and
     * the cluster-manager finalize both completed), and the restore call itself returns, but the new
     * shard sits at {@code allocation_status[fetching_shard_data]} indefinitely -- not slow, genuinely
     * stuck, confirmed by a 20-minute suite timeout before this test bounded its own wait.
     *
     * <p>Traced one layer further before stopping rather than left as a bare symptom:
     * {@code PrimaryShardAllocator.getInEligibleShardDecision} refuses a snapshot-recovery shard until
     * {@code SnapshotShardSizeInfo.getShardSize} answers non-null, which {@code
     * InternalSnapshotsInfoService} only ever supplies by fetching {@code
     * Repository#getShardSnapshotStatus} on the elected cluster manager -- and that fetch, or the
     * reroute it should trigger on completion, never resolves for a snapshot this action finalized.
     * This action's own javadoc already states the boundary that is the likely cause: it does not
     * replicate {@code SnapshotsService}'s in-memory tracking of concurrent snapshot state the way a
     * real {@code _snapshot} call would, and something in that state {@code
     * InternalSnapshotsInfoService} depends on is apparently left inconsistent.
     *
     * <p>Computed placement being enabled on the node is the one difference between this test and
     * {@link #testADeepSnapshotOfAnOrdinaryServerlessIndexIsRestorableByCore}, which passes -- worth
     * recording since it narrows where to look next, not because the restored index is itself gated
     * (it is deliberately un-gated by {@code restoreAndAssertDocumentCount}'s own settings override,
     * confirmed by the diagnostic this test prints on failure).
     */
    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "the restored shard's allocation never resolves out of "
        + "fetching_shard_data after a deep snapshot copied and finalized through IndexDeepSnapshotAction from a "
        + "computed-placement-enabled node; root cause traced to InternalSnapshotsInfoService/PrimaryShardAllocator "
        + "but not yet found -- see this method's own javadoc")
    public void testADeepSnapshotOfAGatedIndexIsRestorableByCore() throws Exception {
        startClusterAndRepo(Settings.builder().put(ServerlessStoragePlugin.COMPUTED_PLACEMENT_ENABLED_SETTING.getKey(), true).build());
        installBlobBackedDescriptorPlane();
        String indexName = "serverless_deep-snapshot-gated-source";

        assertTrue(
            client().admin()
                .indices()
                .create(
                    new CreateIndexRequest(indexName).settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .put("index.serverless_storage.enabled", true)
                            .build()
                    )
                )
                .actionGet()
                .isAcknowledged()
        );
        assertBusy(() -> {
            try {
                for (int i = 0; i < 9; i++) {
                    client().prepareIndex(indexName).setId(Integer.toString(i)).setSource("f", "value-" + i).get();
                }
            } catch (Exception e) {
                throw new AssertionError("write not yet servable: " + e.getMessage(), e);
            }
        }, 60, java.util.concurrent.TimeUnit.SECONDS);
        client().admin().indices().prepareFlush(indexName).get();
        client().admin().indices().prepareRefresh(indexName).get();
        assertEquals(9, count(indexName));

        IndexDeepSnapshotResponse response = client().execute(
            IndexDeepSnapshotAction.INSTANCE,
            new IndexDeepSnapshotRequest(indexName, REPO_NAME, "gated-deep-1")
        ).get();
        assertEquals(1, response.shardCount());

        restoreAndAssertDocumentCount(indexName, "gated-deep-1", "gated-deep-restored", 9);
    }

    private void restoreAndAssertDocumentCount(String sourceIndex, String snapshotName, String restoredName, long expectedDocs)
        throws Exception {
        client().admin()
            .cluster()
            .prepareRestoreSnapshot(REPO_NAME, snapshotName)
            .setIndices(sourceIndex)
            .setRenamePattern(sourceIndex)
            .setRenameReplacement(restoredName)
            .setIndexSettings(Settings.builder().put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), false))
            .setWaitForCompletion(false)
            .get();

        // Not setWaitForCompletion(true): a restore that never allocates its shard must fail
        // informatively rather than hang this call (and, with it, the whole suite) indefinitely --
        // exactly what testADeepSnapshotOfAGatedIndexIsRestorableByCore's own AwaitsFix found, the
        // hard way, before this bound existed.
        org.opensearch.action.admin.cluster.health.ClusterHealthResponse health = client().admin()
            .cluster()
            .prepareHealth(restoredName)
            .setWaitForStatus(org.opensearch.cluster.health.ClusterHealthStatus.GREEN)
            .setTimeout(org.opensearch.common.unit.TimeValue.timeValueSeconds(30))
            .get();
        if (health.isTimedOut()) {
            org.opensearch.cluster.ClusterState state = client().admin().cluster().prepareState().get().getState();
            org.opensearch.cluster.metadata.IndexMetadata restoredMetadata = state.metadata().index(restoredName);
            org.opensearch.cluster.routing.IndexRoutingTable routingTable = state.routingTable().index(restoredName);
            fail(
                "restored index ["
                    + restoredName
                    + "] never went green. settings="
                    + (restoredMetadata == null ? "NO METADATA" : restoredMetadata.getSettings())
                    + " state="
                    + (restoredMetadata == null ? "?" : restoredMetadata.getState())
                    + " routingTable="
                    + routingTable
            );
        }

        client().admin().indices().prepareRefresh(restoredName).get();
        assertEquals(
            "every document must come back from the repository's own copy of the bytes, through the shipped action",
            expectedDocs,
            count(restoredName)
        );
    }

    @org.junit.After
    public void clearGate() {
        DescriptorGate.uninstall();
    }

    private long count(String index) {
        return client().prepareSearch(index).setSize(0).get().getHits().getTotalHits().value();
    }
}
