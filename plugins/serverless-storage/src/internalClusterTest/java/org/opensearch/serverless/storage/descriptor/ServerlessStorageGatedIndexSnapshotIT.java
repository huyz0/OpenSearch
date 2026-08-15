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
        client().admin().indices().prepareCreate("serverless_gated-snapshot-target").setSettings(gated).get();
        // Prove the index is genuinely alive and serving writes before asserting the snapshot refusal --
        // the whole point is that this is not a dead or nonexistent index the old message would have been
        // an honest description of. assertBusy because a gated creation acknowledges before every node has
        // necessarily learned of it locally -- a write landing on this thread immediately after create()
        // can race that propagation and see a transient IndexNotFoundException. The exception is converted
        // inside the block rather than left to propagate, since assertBusy only retries on AssertionError
        // (HANDOFF.md's own documented trap: a retry that compiles is not a retry that runs).
        assertBusy(() -> {
            try {
                client().prepareIndex("serverless_gated-snapshot-target").setId("1").setSource("f", "v").get();
            } catch (Exception e) {
                throw new AssertionError("write not yet servable: " + e.getMessage(), e);
            }
        });

        OpenSearchException failure = expectThrows(
            SnapshotException.class,
            () -> client().admin()
                .cluster()
                .prepareCreateSnapshot("gated-snapshot-repo", "gated-snapshot")
                .setIndices("serverless_gated-snapshot-target")
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

    /**
     * The case an operator actually runs, and the one the by-name test above does not cover: a snapshot of
     * everything, taken nightly, with a gated fleet present.
     *
     * <h4>What it does</h4>
     *
     * It succeeds, and the gated indices are not in it. Measured across all four spellings an operator might
     * use -- no indices set, {@code *}, a matching prefix wildcard, and {@code _all} -- every one reported
     * {@code SUCCESS} with only the ordinary index in {@code SnapshotInfo.indices()}. The refusal that the
     * by-name case gets is never reached, because the wildcard expansion snapshot uses resolves against
     * cluster state, where a gated index is not.
     *
     * <h4>Why that is worth a test rather than a shrug</h4>
     *
     * The failure mode is silence at scale. One ordinary index and a hundred million gated ones produce a
     * green backup job covering one index, and nothing in the response says how many were skipped -- the
     * absence is only visible to someone who counts {@code SnapshotInfo.indices()} against what they believe
     * they have. A backup that reports success while covering almost nothing is worse than one that fails,
     * because only the second one gets investigated.
     *
     * <p>Pinned rather than fixed. Capturing a gated index would mean teaching the snapshot and repository
     * pipeline to read shard state through {@code AbsentIndexDescriptorSuppliers} and {@code
     * AbsentIndexRoutingSuppliers} instead of {@code Metadata} and {@code RoutingTable} directly, which is
     * the materially larger change this file's header already declines. What is cheap and not done here,
     * because it is a product decision rather than a test's to make: reporting the count of skipped gated
     * indices on the response, so the silence is at least audible.
     */
    public void testASnapshotOfEverythingSilentlyExcludesTheGatedFleet() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        client().admin()
            .cluster()
            .preparePutRepository("everything-repo")
            .setType(FsRepository.TYPE)
            .setSettings(Settings.builder().put("location", randomRepoPath().resolve("repo")))
            .get();

        Settings gated = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
        client().admin().indices().prepareCreate("serverless_everything-gated").setSettings(gated).get();
        client().admin().indices().prepareCreate("everything-ordinary").get();
        client().prepareIndex("everything-ordinary").setId("1").setSource("f", "v").get();
        // Alive and serving, so "not captured" cannot be read as "there was nothing to capture".
        assertBusy(() -> {
            try {
                client().prepareIndex("serverless_everything-gated").setId("1").setSource("f", "v").get();
            } catch (Exception e) {
                throw new AssertionError("write not yet servable: " + e.getMessage(), e);
            }
        });

        // Every spelling an operator might reach for. All four behave identically, which is worth asserting
        // together: a fix that closed only one of them would leave the others silently excluding.
        String[][] arms = new String[][] { {}, { "*" }, { "_all" }, { "everything-*" } };
        for (int i = 0; i < arms.length; i++) {
            CreateSnapshotResponse response = client().admin()
                .cluster()
                .prepareCreateSnapshot("everything-repo", "snapshot-" + i)
                .setIndices(arms[i])
                .setWaitForCompletion(true)
                .get();
            String label = arms[i].length == 0 ? "(no indices set)" : java.util.Arrays.toString(arms[i]);

            assertEquals(
                "the snapshot reports success for " + label + ", which is the whole problem",
                org.opensearch.snapshots.SnapshotState.SUCCESS,
                response.getSnapshotInfo().state()
            );
            assertTrue(
                "the ordinary index must be captured for " + label,
                response.getSnapshotInfo().indices().contains("everything-ordinary")
            );
            assertFalse(
                "the gated index is not captured, and nothing in this response says so: "
                    + label
                    + " -> "
                    + response.getSnapshotInfo().indices(),
                response.getSnapshotInfo().indices().contains("serverless_everything-gated")
            );
        }
    }
}
