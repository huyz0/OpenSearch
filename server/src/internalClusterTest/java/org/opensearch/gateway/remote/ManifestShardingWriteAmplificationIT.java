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
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.model.RemoteManifestShard;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.gateway.remote.RemoteManifestManager.CLUSTER_REMOTE_STORE_STATE_MANIFEST_SHARD_COUNT_SETTING;

/**
 * Plan item E8 (plan-100m-index-implementation.md, Area E; rfc-scalable-index-metadata-tasks.md, C3a/C8):
 * "C3a's 256-shard figure came from a simulation, not the implementation. Re-run against the real write
 * path."
 *
 * <p>{@code ManifestShardingWriteAmplificationEstimate} (C3a, in the separate {@code benchmarks} module,
 * not linkable from here) is arithmetic over a hash partition -- exact for the design, but it never calls
 * {@link RemoteManifestManager#uploadManifest}, never writes a real blob, and never runs the real
 * compressor this manifest actually uses at write time. This measures the same question -- bytes written
 * for one cluster-state version with one changed index, sharded against not -- against the genuine write
 * path (E5) and a real FS-backed repository, the same way {@code ManifestShardingIT} proves correctness
 * against a real repository rather than mocks.
 *
 * <p>Deliberately not at the RFC's own 100,000-index, 256-shard scale: a real cluster creating 100,000
 * real indices is not something an integration test can do in reasonable time (index creation is a real
 * cluster-state publication and shard allocation, not the simulation's free arithmetic). This runs at a
 * scale two orders of magnitude smaller and reports the real number honestly at that scale, the same
 * "measured at small scale, not fully closed" discipline {@code PublicationLatencyVsClusterSizeIT} (T14)
 * already established for this plan -- a real ratio at 1,000 indices is a genuine data point, not proof
 * the RFC's 125x figure holds at 100,000.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ManifestShardingWriteAmplificationIT extends RemoteStoreBaseIntegTestCase {

    private static final int INDEX_COUNT = 1000;
    private static final int SHARD_COUNT_FOR_MEASUREMENT = 256;

    @Before
    public void setup() {
        // Same reasoning as ManifestShardingIT: the randomized mock async-multipart repo this base class
        // can select does not implement every operation this test's blob listing relies on being real.
        asyncUploadMockFsRepo = false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true).build();
    }

    private String indexName(int i) {
        return String.format(Locale.ROOT, "write-amp-index-%d", i);
    }

    /**
     * Every blob directly in {@code container}, by name, with its real size. A plain snapshot rather than
     * a diff-as-you-go: blob names in this store are unique per cluster-state version (they carry the
     * term, version and an inverted timestamp), so a before/after set difference correctly isolates
     * exactly what one write added, regardless of how much history already sits in the folder.
     */
    private Map<String, Long> snapshotBlobSizes(BlobContainer container) throws IOException {
        Map<String, Long> sizes = new HashMap<>();
        for (Map.Entry<String, BlobMetadata> entry : container.listBlobs().entrySet()) {
            sizes.put(entry.getKey(), entry.getValue().length());
        }
        return sizes;
    }

    private long bytesOfBlobsAddedSince(Map<String, Long> before, Map<String, Long> after) {
        long total = 0;
        for (Map.Entry<String, Long> entry : after.entrySet()) {
            if (before.containsKey(entry.getKey()) == false) {
                total += entry.getValue();
            }
        }
        return total;
    }

    public void testManifestWriteAmplificationAgainstTheRealWritePath() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        for (int i = 0; i < INDEX_COUNT; i++) {
            createIndex(indexName(i), remoteStoreIndexSettings(0, 1));
        }
        ensureGreen(IntStream.range(0, INDEX_COUNT).mapToObj(this::indexName).collect(Collectors.toList()).toArray(new String[0]));

        RemoteClusterStateService remoteClusterStateService = internalCluster().getInstance(
            RemoteClusterStateService.class,
            internalCluster().getClusterManagerName()
        );
        RemoteManifestManager remoteManifestManager = remoteClusterStateService.getRemoteManifestManager();
        ClusterState state = getClusterState();
        String clusterName = state.getClusterName().value();
        String clusterUUID = state.metadata().clusterUUID();

        BlobPath manifestPath = remoteManifestManager.getManifestFolderPath(clusterName, clusterUUID);
        BlobStoreRepository repository = remoteClusterStateService.getBlobStoreRepository();
        BlobContainer manifestContainer = repository.blobStore().blobContainer(manifestPath);
        BlobContainer shardContainer = repository.blobStore().blobContainer(manifestPath.add(RemoteManifestShard.MANIFEST_SHARDS));

        // Unsharded measurement: sharding is off by default (manifest.shard_count node setting is 0,
        // never set on this cluster yet), so this is today's real, already-shipped write path -- every
        // one of the INDEX_COUNT indices is an inline UploadedIndexMetadata entry in the manifest, and a
        // one-index settings change rewrites the whole list, exactly the write amplification C3a exists
        // to quantify.
        long versionBeforeUnsharded = getClusterState().version();
        Map<String, Long> beforeUnsharded = snapshotBlobSizes(manifestContainer);
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName(0))
            .setSettings(Settings.builder().put("index.refresh_interval", "5s"))
            .get();
        Map<String, Long> afterUnsharded = snapshotBlobSizes(manifestContainer);
        assertEquals(
            "this measurement assumes one settings update is exactly one cluster-state version, the same "
                + "assumption PublicationLatencyMeasurementIT (T14) makes for the same reason",
            versionBeforeUnsharded + 1,
            getClusterState().version()
        );
        long unshardedBytes = bytesOfBlobsAddedSince(beforeUnsharded, afterUnsharded);
        assertTrue("the settings update must have written a new manifest blob", unshardedBytes > 0);

        // Turn sharding on, then let it settle with one write on a *different* index before measuring.
        // previousShardCount (0) != shardCountForThisManifest (256) makes IndexMetadataManifestSharder
        // mark every shard dirty on the very next write (see its own javadoc) -- that first post-toggle
        // write rewrites all SHARD_COUNT_FOR_MEASUREMENT shards, which is not the steady-state "one
        // changed index" cost this test measures, so it happens on indexName(1) and is discarded rather
        // than counted.
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(CLUSTER_REMOTE_STORE_STATE_MANIFEST_SHARD_COUNT_SETTING.getKey(), SHARD_COUNT_FOR_MEASUREMENT)
            )
            .get();
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName(1))
            .setSettings(Settings.builder().put("index.refresh_interval", "6s"))
            .get();

        // Sharded measurement: same shape of change (one index's settings) as the unsharded measurement
        // above, now in steady state under sharding.
        long versionBeforeSharded = getClusterState().version();
        Map<String, Long> beforeSharded = snapshotBlobSizes(manifestContainer);
        Map<String, Long> beforeShardedShards = snapshotBlobSizes(shardContainer);
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName(0))
            .setSettings(Settings.builder().put("index.refresh_interval", "7s"))
            .get();
        Map<String, Long> afterSharded = snapshotBlobSizes(manifestContainer);
        Map<String, Long> afterShardedShards = snapshotBlobSizes(shardContainer);
        assertEquals(versionBeforeSharded + 1, getClusterState().version());
        long shardedBytes = bytesOfBlobsAddedSince(beforeSharded, afterSharded) + bytesOfBlobsAddedSince(
            beforeShardedShards,
            afterShardedShards
        );
        assertTrue("the settings update must have written a new top-level manifest blob and shard blob", shardedBytes > 0);

        double ratio = (double) unshardedBytes / (double) shardedBytes;
        // logger.warn, not info/debug: OpenSearch's gradle test runner only surfaces captured test output
        // on failure, the same reason ManifestShardingIT's own break-the-fix investigation needed a
        // forced failure to see anything -- PublicationLatencyVsClusterSizeIT (T14) uses the identical
        // logger.warn technique to make a real measurement visible on a passing run.
        logger.warn(
            "E8 manifest write amplification, real write path, {} real indices, {} shards, 1 changed "
                + "index: unsharded {} bytes vs sharded {} bytes ({}x)",
            INDEX_COUNT,
            SHARD_COUNT_FOR_MEASUREMENT,
            unshardedBytes,
            shardedBytes,
            String.format(Locale.ROOT, "%.1f", ratio)
        );

        assertTrue(
            String.format(
                Locale.ROOT,
                "sharding a %,d-index manifest at %,d shards must write meaningfully fewer bytes for a "
                    + "single changed index than the unsharded write -- got %,d unsharded vs %,d sharded",
                INDEX_COUNT,
                SHARD_COUNT_FOR_MEASUREMENT,
                unshardedBytes,
                shardedBytes
            ),
            shardedBytes < unshardedBytes
        );
    }
}
