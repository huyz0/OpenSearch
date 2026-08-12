/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.snapshots.SnapshotException;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;

/**
 * A gated index is genuinely absent from {@code Metadata.indices()} entirely -- its metadata lives
 * off cluster state, resolved through {@link org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers}
 * instead (Area H). Before the fix this test guards, {@code SnapshotsService#shards} read
 * {@code metadata.index(name)} paired with {@code routingTable.index(name)}, found neither, and
 * reported every shard as {@code MISSING} -- surfacing as
 * {@code SnapshotException: Indices don't have primary shards [name]}, which is false and
 * misleading: the index has primary shards, serving live traffic, it simply is not represented in
 * cluster state the way that code expects.
 *
 * <p>The same production fix (in {@code SnapshotsService#createSnapshot}) also covers the sibling
 * case -- a computed-placement (Area C) index, present in {@code Metadata.indices()} but with no
 * published routing table entry -- via {@link org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers}.
 * That branch is not separately exercised here: this settings combination (a descriptor plane
 * installed, per {@link #nodeSettings}) always produces a fully gated index, not a
 * computed-placement-only one, so there is no test fixture in this suite that reaches it in
 * isolation. The code path is a symmetrical few lines beside the one this test does cover, reviewed
 * by reading rather than by a fixture this suite cannot currently construct.
 *
 * <p>This does not attempt real snapshot support for gated or computed-placement indices -- that
 * would need teaching the whole snapshot/repository pipeline to read shard state through their
 * respective resolvers instead of {@code Metadata}/{@code RoutingTable} directly, a materially
 * larger change. It only replaces a misleading error with an honest one, the same choice this
 * project already made for search-only scaling ({@code ScaleIndexOperationValidator}) and in-place
 * resharding ({@code MetadataInPlaceSplitShardService}).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageGatedIndexSnapshotIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), randomRepoPath().toString())
            .put(ServerlessStoragePlugin.COMPUTED_PLACEMENT_ENABLED_SETTING.getKey(), true)
            .build();
    }

    public void testSnapshottingAGatedIndexByNameFailsWithAnHonestReason() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        client().admin()
            .cluster()
            .preparePutRepository("gated-snapshot-repo")
            .setType(FsRepository.TYPE)
            .setSettings(Settings.builder().put("location", randomRepoPath().resolve("repo")))
            .get();

        Settings gated = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
        client().admin().indices().prepareCreate("gated-snapshot-target").setSettings(gated).get();
        // Prove the index is genuinely alive and serving writes before asserting the snapshot refusal --
        // the whole point is that this is not a dead or nonexistent index the old message would have been
        // an honest description of. assertBusy because a gated creation acknowledges before every node has
        // necessarily learned of it locally -- a write landing on this thread immediately after create()
        // can race that propagation and see a transient IndexNotFoundException. The exception is converted
        // inside the block rather than left to propagate, since assertBusy only retries on AssertionError
        // (HANDOFF.md's own documented trap: a retry that compiles is not a retry that runs).
        assertBusy(() -> {
            try {
                client().prepareIndex("gated-snapshot-target").setId("1").setSource("f", "v").get();
            } catch (Exception e) {
                throw new AssertionError("write not yet servable: " + e.getMessage(), e);
            }
        });

        OpenSearchException failure = expectThrows(
            SnapshotException.class,
            () -> client().admin()
                .cluster()
                .prepareCreateSnapshot("gated-snapshot-repo", "gated-snapshot")
                .setIndices("gated-snapshot-target")
                .setWaitForCompletion(true)
                .get()
        );

        assertTrue(
            "the refusal must name the real reason (metadata off cluster state), not the misleading "
                + "\"don't have primary shards\" message: "
                + failure.getMessage(),
            failure.getMessage().contains("metadata is not stored in cluster state")
        );
        assertFalse(
            "must not resurrect the old, false-sounding message for this case: " + failure.getMessage(),
            failure.getMessage().contains("don't have primary shards")
        );
    }

    public void testSnapshottingAnOrdinaryIndexAlongsideIsUnaffected() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        client().admin()
            .cluster()
            .preparePutRepository("ordinary-snapshot-repo")
            .setType(FsRepository.TYPE)
            .setSettings(Settings.builder().put("location", randomRepoPath().resolve("repo")))
            .get();

        client().admin().indices().prepareCreate("ordinary-snapshot-target").get();
        client().prepareIndex("ordinary-snapshot-target").setId("1").setSource("f", "v").get();

        CreateSnapshotResponse response = client().admin()
            .cluster()
            .prepareCreateSnapshot("ordinary-snapshot-repo", "ordinary-snapshot")
            .setIndices("ordinary-snapshot-target")
            .setWaitForCompletion(true)
            .get();

        assertEquals(
            "an ordinary, non-gated index must be unaffected by the new check",
            org.opensearch.snapshots.SnapshotState.SUCCESS,
            response.getSnapshotInfo().state()
        );
    }
}
