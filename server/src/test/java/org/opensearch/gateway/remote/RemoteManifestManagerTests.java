/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.EncryptedBlobContainer;
import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.model.RemoteManifestShard;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.indices.IndicesModule;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteManifestManagerTests extends OpenSearchTestCase {
    private RemoteManifestManager remoteManifestManager;
    private ClusterSettings clusterSettings;
    private BlobStoreRepository blobStoreRepository;
    private BlobStore blobStore;
    private BlobStoreTransferService blobStoreTransferService;
    private ThreadPool threadPool;

    @Before
    public void setup() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        blobStoreRepository = mock(BlobStoreRepository.class);
        NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(
            Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                ClusterModule.getNamedXWriteables().stream()
            ).flatMap(Function.identity()).collect(toList())
        );
        blobStoreTransferService = mock(BlobStoreTransferService.class);
        blobStore = mock(BlobStore.class);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        threadPool = new TestThreadPool("test");
        Compressor compressor = new NoneCompressor();
        when(blobStoreRepository.getCompressor()).thenReturn(compressor);
        when(blobStoreRepository.getNamedXContentRegistry()).thenReturn(xContentRegistry);
        remoteManifestManager = new RemoteManifestManager(
            clusterSettings,
            "test-cluster-name",
            "test-node-id",
            blobStoreRepository,
            blobStoreTransferService,
            threadPool
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testMetadataManifestUploadWaitTimeSetting() {
        // verify default value
        assertEquals(
            RemoteManifestManager.METADATA_MANIFEST_UPLOAD_TIMEOUT_DEFAULT,
            remoteManifestManager.getMetadataManifestUploadTimeout()
        );

        // verify update metadata manifest upload timeout
        int metadataManifestUploadTimeout = randomIntBetween(1, 10);
        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.state.metadata_manifest.upload_timeout", metadataManifestUploadTimeout + "s")
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(metadataManifestUploadTimeout, remoteManifestManager.getMetadataManifestUploadTimeout().seconds());
    }

    /**
     * Phase G of core-pluggability-refactor-plan.md: resolveCodecVersion's default-behavior-preserving
     * shape. See CLUSTER_REMOTE_STORE_STATE_PIN_CODEC_V5_SETTING's own javadoc for why the default is
     * false (today's unconditional CODEC_V6) rather than true (would have been a default-behavior change).
     */
    public void testResolveCodecVersionDefaultsToCurrentCodecUnconditionally() {
        assertEquals(
            "with nothing configured, every write must land on the current codec, unchanged from before this setting existed",
            ClusterMetadataManifest.MANIFEST_CURRENT_CODEC_VERSION,
            remoteManifestManager.resolveCodecVersion(0)
        );
    }

    public void testResolveCodecVersionPinnedToV5WhenNotSharding() {
        clusterSettings.applySettings(
            Settings.builder().put(RemoteManifestManager.CLUSTER_REMOTE_STORE_STATE_PIN_CODEC_V5_SETTING.getKey(), true).build()
        );

        assertEquals(ClusterMetadataManifest.CODEC_V5, remoteManifestManager.resolveCodecVersion(0));
    }

    public void testResolveCodecVersionShardingOverridesThePin() {
        clusterSettings.applySettings(
            Settings.builder().put(RemoteManifestManager.CLUSTER_REMOTE_STORE_STATE_PIN_CODEC_V5_SETTING.getKey(), true).build()
        );

        assertEquals(
            "an operator who explicitly turned sharding on has already made the CODEC_V6 decision",
            ClusterMetadataManifest.MANIFEST_CURRENT_CODEC_VERSION,
            remoteManifestManager.resolveCodecVersion(4)
        );
    }

    public void testResolveCodecVersionPinSettingIsDynamic() {
        assertEquals(ClusterMetadataManifest.MANIFEST_CURRENT_CODEC_VERSION, remoteManifestManager.resolveCodecVersion(0));

        clusterSettings.applySettings(
            Settings.builder().put(RemoteManifestManager.CLUSTER_REMOTE_STORE_STATE_PIN_CODEC_V5_SETTING.getKey(), true).build()
        );
        assertEquals(ClusterMetadataManifest.CODEC_V5, remoteManifestManager.resolveCodecVersion(0));

        clusterSettings.applySettings(
            Settings.builder().put(RemoteManifestManager.CLUSTER_REMOTE_STORE_STATE_PIN_CODEC_V5_SETTING.getKey(), false).build()
        );
        assertEquals(ClusterMetadataManifest.MANIFEST_CURRENT_CODEC_VERSION, remoteManifestManager.resolveCodecVersion(0));
    }

    public void testReadLatestMetadataManifestFailedIOException() throws IOException {
        final ClusterState clusterState = RemoteClusterStateServiceTests.generateClusterStateWithOneIndex()
            .nodes(RemoteClusterStateServiceTests.nodesWithLocalNodeClusterManager())
            .build();

        BlobContainer blobContainer = mockBlobStoreObjects();
        when(blobContainer.listBlobsByPrefixInSortedOrder("manifest" + DELIMITER, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC))
            .thenThrow(IOException.class);

        Exception e = assertThrows(
            IllegalStateException.class,
            () -> remoteManifestManager.getLatestClusterMetadataManifest(
                clusterState.getClusterName().value(),
                clusterState.metadata().clusterUUID()
            )
        );
        assertEquals(e.getMessage(), "Error while fetching latest manifest file for remote cluster state");
    }

    @SuppressWarnings("unchecked")
    public void testRemoteManifestManagerUsesListBlobsByPrefixInSortedOrder() throws IOException {
        final ClusterState clusterState = RemoteClusterStateServiceTests.generateClusterStateWithOneIndex()
            .nodes(RemoteClusterStateServiceTests.nodesWithLocalNodeClusterManager())
            .build();

        BlobContainer mockBlobContainer = mock(BlobContainer.class);
        CryptoHandler<Object, Object> cryptoHandler = mock(CryptoHandler.class);
        EncryptedBlobContainer<Object, Object> encryptedBlobContainer = new EncryptedBlobContainer<>(mockBlobContainer, cryptoHandler);

        final BlobPath blobPath = mock(BlobPath.class);
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
        when(blobPath.add(anyString())).thenReturn(blobPath);
        when(blobPath.buildAsString()).thenReturn("/blob/path/");
        when(mockBlobContainer.path()).thenReturn(blobPath);
        when(blobStore.blobContainer(any())).thenReturn(encryptedBlobContainer);

        when(mockBlobContainer.listBlobsByPrefixInSortedOrder("manifest" + DELIMITER, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC))
            .thenReturn(java.util.Collections.emptyList());

        remoteManifestManager.getLatestClusterMetadataManifest(
            clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID()
        );

        verify(mockBlobContainer).listBlobsByPrefixInSortedOrder("manifest" + DELIMITER, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
    }

    private static ClusterMetadataManifest shardedManifest(List<UploadedManifestShard> shards) {
        return ClusterMetadataManifest.builder()
            .clusterTerm(1L)
            .stateVersion(1L)
            .clusterUUID("test-cluster-uuid")
            .stateUUID("test-state-uuid")
            .nodeId("test-node-id")
            .opensearchVersion(org.opensearch.Version.CURRENT)
            .previousClusterUUID(ClusterState.UNKNOWN_UUID)
            .codecVersion(ClusterMetadataManifest.MANIFEST_CURRENT_CODEC_VERSION)
            .indices(java.util.Collections.emptyList())
            .routingTableVersion(0L)
            .manifestShardCount(shards.size())
            .indexMetadataShards(shards)
            .build();
    }

    /**
     * The strict half of the sharded-manifest read contract: a shard blob that cannot be read is a failure,
     * not an empty partition. Every caller resolving a manifest it intends to <em>keep</em> depends on this,
     * because the cleanup sweep turns "not referenced" straight into "delete".
     */
    public void testResolveIndicesFailsWhenAShardBlobCannotBeRead() throws IOException {
        mockBlobStoreObjects();
        when(blobStoreTransferService.downloadBlob(any(), anyString())).thenThrow(new IOException("blob is gone"));

        ClusterMetadataManifest manifest = shardedManifest(
            List.of(new UploadedManifestShard(0, "path/manifest/shards/manifest-shard__0__a", 1))
        );

        expectThrows(IllegalStateException.class, () -> remoteManifestManager.resolveIndices(manifest));
    }

    /**
     * The tolerant half, which exists so the cleanup sweep cannot wedge: a stale manifest whose shard blobs
     * have already been deleted must still be resolvable (to nothing) so the sweep gets as far as deleting
     * the manifest itself. Before this, the sweep threw here on every subsequent pass and never reached the
     * manifest delete that would have ended the loop.
     */
    public void testResolveIndicesToleratingMissingShardsSkipsUnreadableShards() throws IOException {
        mockBlobStoreObjects();
        when(blobStoreTransferService.downloadBlob(any(), anyString())).thenThrow(new IOException("blob is gone"));

        ClusterMetadataManifest manifest = shardedManifest(
            List.of(
                new UploadedManifestShard(0, "path/manifest/shards/manifest-shard__0__a", 1),
                new UploadedManifestShard(1, "path/manifest/shards/manifest-shard__1__a", 2)
            )
        );

        assertTrue(remoteManifestManager.resolveIndicesToleratingMissingShards(manifest).isEmpty());
    }

    /**
     * A shard blob is immutable and addressed by a name that is minted fresh on every write, so a second
     * resolution of the same reference must not go back to the blob store. This is what makes sharding pay
     * off on the read side: without it, every publication re-read the entire shard set -- S sequential GETs
     * on the cluster manager's own critical path, twice per publish/commit cycle.
     */
    public void testResolveIndicesReadsEachShardBlobOnlyOnce() throws IOException {
        mockBlobStoreObjects();
        String blobName = "path/manifest/shards/manifest-shard__0__a";
        UploadedIndexMetadata index = new UploadedIndexMetadata("index1", "indexUUID1", "index_metadata1__2");
        BytesReference serialized = RemoteManifestShard.MANIFEST_SHARD_FORMAT.serialize(
            new ManifestShardContent(List.of(index)),
            blobName,
            new NoneCompressor(),
            RemoteClusterStateUtils.FORMAT_PARAMS
        );
        when(blobStoreTransferService.downloadBlob(any(), anyString())).thenAnswer(invocation -> serialized.streamInput());

        ClusterMetadataManifest manifest = shardedManifest(List.of(new UploadedManifestShard(0, blobName, 1)));

        assertEquals(List.of(index), remoteManifestManager.resolveIndices(manifest));
        assertEquals(List.of(index), remoteManifestManager.resolveIndices(manifest));
        assertEquals(List.of(index), remoteManifestManager.resolveIndices(manifest));
        verify(blobStoreTransferService, times(1)).downloadBlob(any(), anyString());

        // And a cold cache genuinely goes back to the store, so the assertion above is about caching rather
        // than about the fixture only ever being asked once.
        remoteManifestManager.clearShardContentCache();
        assertEquals(List.of(index), remoteManifestManager.resolveIndices(manifest));
        verify(blobStoreTransferService, times(2)).downloadBlob(any(), anyString());
    }

    private BlobContainer mockBlobStoreObjects() {
        final BlobPath blobPath = mock(BlobPath.class);
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
        when(blobPath.add(anyString())).thenReturn(blobPath);
        when(blobPath.buildAsString()).thenReturn("/blob/path/");
        final BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobContainer.path()).thenReturn(blobPath);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        return blobContainer;
    }
}
