/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.ClusterState;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.gateway.remote.RemoteManifestManager.CLUSTER_REMOTE_STORE_STATE_MANIFEST_SHARD_COUNT_SETTING;

/**
 * Real end-to-end coverage for plan item E5/E6 (plan-100m-index-implementation.md, Area E;
 * rfc-manifest-sharding-design.md): does a real cluster, with sharding turned on, write a manifest whose
 * index list genuinely lives behind {@link UploadedManifestShard} references, read it back correctly on
 * a full restart, and does the cleanup sweep's transitive resolution (the fix C1's codec guard
 * deliberately deferred) survive real garbage collection without losing anything still referenced.
 *
 * <p>Deliberately not a unit test with mocked blob I/O: {@link IndexMetadataManifestSharderTests} and
 * {@link ManifestShardFunctionTests} already cover the pure partitioning logic in isolation, and {@link
 * RemoteClusterStateCleanupManagerTests} covers the keep/stale set computation with mocked manifests.
 * What none of those can prove is that the real write path (blob names generated, blobs actually
 * uploaded), the real read path (blobs actually fetched and reassembled), and the real cleanup sweep
 * (real blob listing and deletion) agree with each other against a real repository -- exactly the risk
 * the RFC's own "Cleanup correctness... deserves the fuzzing treatment" note is about.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ManifestShardingIT extends RemoteStoreBaseIntegTestCase {

    private static final int SHARD_COUNT_FOR_MANIFEST = 3;
    private static final int INDEX_COUNT = 7;

    @Before
    public void setup() {
        // RemoteStoreBaseIntegTestCase randomizes this per run; the mock async-multipart repo it
        // selects when true does not implement deleteBlobsAsyncIgnoringIfNotExists at all (throws
        // UnsupportedOperationException unconditionally), which would make
        // testCleanupSweepDoesNotDeleteLiveDataUnderSharding fail for a reason that has nothing to do
        // with sharding. Same fix RemoteStoreClusterStateRestoreIT already applies for the same reason.
        asyncUploadMockFsRepo = false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(CLUSTER_REMOTE_STORE_STATE_MANIFEST_SHARD_COUNT_SETTING.getKey(), SHARD_COUNT_FOR_MANIFEST)
            .build();
    }

    private List<String> indexNames() {
        return IntStream.range(0, INDEX_COUNT).mapToObj(i -> String.format(Locale.ROOT, "sharded-manifest-index-%d", i))
            .collect(Collectors.toList());
    }

    public void testManifestIsGenuinelySharded() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        for (String indexName : indexNames()) {
            createIndex(indexName, remoteStoreIndexSettings(0, 1));
        }
        ensureGreen(indexNames().toArray(new String[0]));

        RemoteClusterStateService remoteClusterStateService = internalCluster().getInstance(
            RemoteClusterStateService.class,
            internalCluster().getClusterManagerName()
        );
        ClusterState state = getClusterState();
        Optional<ClusterMetadataManifest> manifestOpt = remoteClusterStateService.getLatestClusterMetadataManifest(
            state.getClusterName().value(),
            state.metadata().clusterUUID()
        );
        assertTrue("a manifest must exist after creating indices", manifestOpt.isPresent());
        ClusterMetadataManifest manifest = manifestOpt.get();

        assertEquals(
            "the manifest must declare the shard count this node is configured with",
            SHARD_COUNT_FOR_MANIFEST,
            manifest.getManifestShardCount()
        );
        assertTrue(
            "a sharded manifest's inline indices list must be empty -- the whole point is that the "
                + "index list lives behind shard references instead",
            manifest.getIndices().isEmpty()
        );
        assertFalse("at least one shard reference must exist for " + INDEX_COUNT + " indices", manifest.getIndexMetadataShards().isEmpty());

        List<UploadedIndexMetadata> resolved = remoteClusterStateService.getRemoteManifestManager().resolveIndices(manifest);
        assertEquals(
            "resolving through the shard references must recover every index, same as an unsharded manifest's getIndices()",
            INDEX_COUNT,
            resolved.size()
        );
        assertEquals(
            indexNames().stream().sorted().collect(Collectors.toList()),
            resolved.stream().map(UploadedIndexMetadata::getIndexName).sorted().collect(Collectors.toList())
        );
    }

    public void testShardedManifestSurvivesAFullRestart() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        for (String indexName : indexNames()) {
            createIndex(indexName, remoteStoreIndexSettings(0, 1));
        }
        ensureGreen(indexNames().toArray(new String[0]));

        // A settings update on one index only -- this is what exercises the "only the dirty shard is
        // rewritten, the rest are carried forward by reference" half of the write path, not just the
        // simpler "everything is new" first-write case above.
        client().admin().indices().prepareUpdateSettings(indexNames().get(0)).setSettings(Settings.builder().put("index.refresh_interval", "2s")).get();
        // A deletion -- exercises the "an index leaving its shard also dirties that shard" case.
        String deletedIndex = indexNames().get(indexNames().size() - 1);
        client().admin().indices().prepareDelete(deletedIndex).get();

        internalCluster().fullRestart();
        ensureStableCluster(2);
        List<String> expectedIndices = indexNames().stream().filter(name -> !name.equals(deletedIndex)).collect(Collectors.toList());
        ensureGreen(expectedIndices.toArray(new String[0]));

        assertBusy(() -> {
            ClusterState state = getClusterState();
            assertEquals(expectedIndices.size(), state.metadata().indices().size());
            for (String indexName : expectedIndices) {
                assertTrue("index [" + indexName + "] must survive a full restart of a sharded manifest", state.metadata().hasIndex(indexName));
            }
            assertFalse("the deleted index must not reappear after restart", state.metadata().hasIndex(deletedIndex));
        }, 30, TimeUnit.SECONDS);
    }

    public void testCleanupSweepDoesNotDeleteLiveDataUnderSharding() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        for (String indexName : indexNames()) {
            createIndex(indexName, remoteStoreIndexSettings(0, 1));
        }
        ensureGreen(indexNames().toArray(new String[0]));

        // Rack up enough manifest versions (well past RemoteClusterStateCleanupManager.RETAINED_MANIFESTS)
        // that a real sweep, retaining only the newest few, has real stale manifests -- and therefore
        // real stale-vs-referenced shard/index blob decisions -- to make.
        for (int i = 0; i < 15; i++) {
            client().admin().indices().prepareUpdateSettings(indexNames().get(0)).setSettings(Settings.builder().put("index.refresh_interval", (2 + i) + "s")).get();
        }

        RemoteClusterStateService remoteClusterStateService = internalCluster().getInstance(
            RemoteClusterStateService.class,
            internalCluster().getClusterManagerName()
        );
        RemoteClusterStateCleanupManager cleanupManager = remoteClusterStateService.getCleanupManager();
        ClusterState state = getClusterState();
        String clusterName = state.getClusterName().value();
        String clusterUUID = state.metadata().clusterUUID();

        // Direct, no-round-trip check: pick an index that was never touched by the 15 settings updates
        // above (index 0 was; this is a different one), so its manifest entry -- and the underlying blob
        // it names -- has been carried forward unchanged, by reference, across every one of those 15+
        // manifest versions. That is exactly the shape the transitive-cleanup fix exists for: a retained
        // manifest's un-changed shard must still resolve to this blob's name so the sweep's keep-set
        // protects it. A full restart exercises this too, but indirectly (through node bootstrap/gateway
        // recovery, which may not always re-fetch from remote); checking blob existence directly against
        // the repository removes that ambiguity.
        String untouchedIndexName = indexNames().get(1);
        Optional<ClusterMetadataManifest> preManifestOpt = remoteClusterStateService.getLatestClusterMetadataManifest(
            clusterName,
            clusterUUID
        );
        assertTrue(preManifestOpt.isPresent());
        UploadedIndexMetadata untouchedBefore = remoteClusterStateService.getRemoteManifestManager()
            .resolveIndices(preManifestOpt.get())
            .stream()
            .filter(u -> u.getIndexName().equals(untouchedIndexName))
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected " + untouchedIndexName + " in the pre-cleanup manifest"));
        String untouchedBlobName = RemoteClusterStateUtils.getFormattedIndexFileName(untouchedBefore.getUploadedFilename());
        BlobContainer rootContainer = remoteClusterStateService.getBlobStore().blobContainer(BlobPath.cleanPath());
        assertTrue(
            "sanity check: the untouched index's metadata blob must exist before cleanup runs",
            rootContainer.blobExists(untouchedBlobName)
        );

        // Directly invoke the package-visible sweep (bypassing the "10 state changes since last attempt"
        // scheduling gate -- SKIP_CLEANUP_STATE_CHANGES -- which is a scheduling nicety unrelated to the
        // correctness this test cares about) with a small retain count, so this one call has real,
        // immediate deletions to perform rather than merely being a no-op because everything still fits
        // inside the retention window.
        cleanupManager.deleteStaleClusterMetadata(clusterName, clusterUUID, 2, state.version());
        assertBusy(() -> assertEquals(state.version(), cleanupManager.getLastCleanupAttemptStateVersion()), 30, TimeUnit.SECONDS);

        assertTrue(
            "the untouched index's metadata blob, still referenced by the retained manifest's carried-forward "
                + "shard, must survive the sweep -- its deletion is exactly the over-deletion bug the "
                + "transitive-cleanup fix exists to prevent",
            rootContainer.blobExists(untouchedBlobName)
        );

        // The real assertion: a fresh restart, forcing a real read of whatever the sweep left behind,
        // must still recover every currently-live index. If the transitive resolution this class exists
        // to fix were missing or wrong, the sweep above would have deleted index or shard blobs still
        // referenced by the retained manifest, and this restart would come back with fewer indices than
        // it started with -- the exact failure shape the RFC's "Cleanup correctness" risk describes.
        internalCluster().fullRestart();
        ensureStableCluster(2);
        ensureGreen(indexNames().toArray(new String[0]));
        assertBusy(() -> {
            ClusterState restored = getClusterState();
            assertEquals(
                "every index must still be readable after a real cleanup sweep ran against a sharded manifest",
                INDEX_COUNT,
                restored.metadata().indices().size()
            );
            for (String indexName : indexNames()) {
                assertTrue(restored.metadata().hasIndex(indexName));
            }
        }, 30, TimeUnit.SECONDS);
    }
}
