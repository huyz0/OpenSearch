/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for NodeBundleRegistry, NodeTranslogUploadQueue, and TarTranslogUploadStrategy.
 */
public class TarTranslogUploadStrategyTests extends OpenSearchTestCase {

    public void testRegistrySerializationAndLookup() throws IOException {
        final NodeBundleRegistry registry = new NodeBundleRegistry();

        final List<NodeBundleRegistry.FileReport> files = new ArrayList<>();
        files.add(new NodeBundleRegistry.FileReport(1L, true, 512, 100));
        files.add(new NodeBundleRegistry.FileReport(1L, false, 1124, 50));

        final List<NodeBundleRegistry.ShardReport> shards = new ArrayList<>();
        shards.add(new NodeBundleRegistry.ShardReport("index-uuid", 0, 1L, 1L, files));

        registry.registerBundle("node-1", "txlog_node_bundles/node-1/2026/05/25/bundle_1.tar", 1716645371000L, shards);

        // Verify lookup
        final List<NodeBundleRegistry.FileLocation> locations = registry.getTranslogLocations("index-uuid", 0, 1L);
        assertEquals(2, locations.size());
        assertEquals("txlog_node_bundles/node-1/2026/05/25/bundle_1.tar", locations.get(0).bundlePath);
        assertTrue(locations.get(0).isTlg);
        assertEquals(512, locations.get(0).offset);
        assertEquals(100, locations.get(0).length);

        assertFalse(locations.get(1).isTlg);
        assertEquals(1124, locations.get(1).offset);
        assertEquals(50, locations.get(1).length);

        // Serialize and Deserialize roundtrip
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            registry.writeTo(dos, 3L); // Term 3
        }

        final byte[] data = baos.toByteArray();
        final NodeBundleRegistry deserialized = new NodeBundleRegistry();
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            deserialized.readFrom(dis);
        }

        // Verify deserialized lookup
        final List<NodeBundleRegistry.FileLocation> deserializedLocs = deserialized.getTranslogLocations("index-uuid", 0, 1L);
        assertEquals(2, deserializedLocs.size());
        assertEquals("txlog_node_bundles/node-1/2026/05/25/bundle_1.tar", deserializedLocs.get(0).bundlePath);
        assertTrue(deserializedLocs.get(0).isTlg);
        assertEquals(512, deserializedLocs.get(0).offset);
        assertEquals(100, deserializedLocs.get(0).length);
    }

    public void testRegistryGarbageCollection() {
        final NodeBundleRegistry registry = new NodeBundleRegistry();

        final List<NodeBundleRegistry.FileReport> files1 = new ArrayList<>();
        files1.add(new NodeBundleRegistry.FileReport(1L, true, 512, 100));

        final List<NodeBundleRegistry.FileReport> files2 = new ArrayList<>();
        files2.add(new NodeBundleRegistry.FileReport(2L, true, 512, 100));

        final List<NodeBundleRegistry.ShardReport> shards1 = new ArrayList<>();
        shards1.add(new NodeBundleRegistry.ShardReport("index-uuid", 0, 1L, 1L, files1));

        final List<NodeBundleRegistry.ShardReport> shards2 = new ArrayList<>();
        shards2.add(new NodeBundleRegistry.ShardReport("index-uuid", 0, 1L, 1L, files2));

        registry.registerBundle("node-1", "txlog_node_bundles/node-1/2026/05/25/bundle_1.tar", 1000L, shards1);
        registry.registerBundle("node-1", "txlog_node_bundles/node-1/2026/05/25/bundle_2.tar", 2000L, shards2);

        // Set min remote generation referenced to 2. This makes bundle_1 garbage, but bundle_2 remains active!
        registry.updateShardMinGen("index-uuid", 0, 2L);

        final List<NodeBundleRegistry.FileLocation> locs1 = registry.getTranslogLocations("index-uuid", 0, 1L);
        assertEquals(1, locs1.size());

        final List<NodeBundleRegistry.FileLocation> locs2 = registry.getTranslogLocations("index-uuid", 0, 2L);
        assertEquals(1, locs2.size());
    }

    public void testDownloadTranslogFromMaster() throws Exception {
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterState clusterState = mock(ClusterState.class);
        final Metadata metadata = mock(Metadata.class);
        final DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        final DiscoveryNode masterNode = mock(DiscoveryNode.class);

        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        when(discoveryNodes.getClusterManagerNode()).thenReturn(masterNode);

        // Setup mock repository settings in metadata
        final IndexMetadata indexMetadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.version.created", org.opensearch.Version.CURRENT)
                    .put("index.remote_store.enabled", true)
                    .put("index.remote_store.translog.repository", "mock-repo")
            )
            .build();
        when(metadata.iterator()).thenReturn(Collections.singletonList(indexMetadata).iterator());

        // Mock repository
        final RepositoriesService reposService = mock(RepositoriesService.class);
        final BlobStoreRepository repo = mock(BlobStoreRepository.class);
        final BlobStore blobStore = mock(BlobStore.class);
        final BlobContainer blobContainer = mock(BlobContainer.class);

        when(reposService.repository("mock-repo")).thenReturn(repo);
        when(repo.basePath()).thenReturn(new BlobPath());
        when(repo.blobStore()).thenReturn(blobStore);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);

        // Mock readBlob returning dummy data
        final byte[] dummyData = "dummy-tlg-bytes".getBytes(StandardCharsets.UTF_8);
        when(blobContainer.readBlob(anyString(), anyLong(), anyLong())).thenReturn(new ByteArrayInputStream(dummyData));

        // Mock Client execute for locations request
        final Client client = mock(Client.class);
        doAnswer(invocation -> {
            final ActionListener<GetTranslogLocationResponse> listener = invocation.getArgument(2);
            final List<NodeBundleRegistry.FileLocation> locs = new ArrayList<>();
            locs.add(new NodeBundleRegistry.FileLocation(true, "txlog_node_bundles/node-1/2026/05/26/bundle_1.tar", 100, 15, 1L));
            listener.onResponse(new GetTranslogLocationResponse(locs));
            return null;
        }).when(client).execute(eq(GetTranslogLocationAction.INSTANCE), any(), any());

        final TarTranslogUploadStrategy strategy = new TarTranslogUploadStrategy(null, () -> reposService, clusterService, client);

        final Path tempDir = createTempDir();
        final ShardId shardId = new ShardId(new Index("test-index", "index-uuid"), 0);

        final boolean result = strategy.downloadTranslog(shardId, "1", "1", tempDir);
        assertTrue(result);

        final Path expectedFile = tempDir.resolve("translog-1.tlog");
        assertTrue(Files.exists(expectedFile));
        assertEquals("dummy-tlg-bytes", new String(Files.readAllBytes(expectedFile), StandardCharsets.UTF_8));
    }

    public void testDownloadTranslogSelfHealing() throws Exception {
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterState clusterState = mock(ClusterState.class);
        final Metadata metadata = mock(Metadata.class);
        final DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        final DiscoveryNode localNode = mock(DiscoveryNode.class);

        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        when(discoveryNodes.getClusterManagerNode()).thenReturn(null); // Master is down!

        when(clusterService.localNode()).thenReturn(localNode);
        when(localNode.getId()).thenReturn("node-1");

        // Mock nodes iteration in clusterState
        doAnswer(invocation -> {
            final java.util.function.Consumer<DiscoveryNode> action = invocation.getArgument(0);
            action.accept(localNode);
            return null;
        }).when(discoveryNodes).forEach(any());

        // Setup mock repository settings in metadata
        final IndexMetadata indexMetadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.version.created", org.opensearch.Version.CURRENT)
                    .put("index.remote_store.enabled", true)
                    .put("index.remote_store.translog.repository", "mock-repo")
            )
            .build();
        when(metadata.iterator()).thenReturn(Collections.singletonList(indexMetadata).iterator());

        // Mock repository
        final RepositoriesService reposService = mock(RepositoriesService.class);
        final BlobStoreRepository repo = mock(BlobStoreRepository.class);
        final BlobStore blobStore = mock(BlobStore.class);
        final BlobContainer blobContainer = mock(BlobContainer.class);

        when(reposService.repository("mock-repo")).thenReturn(repo);
        when(repo.basePath()).thenReturn(new BlobPath());
        when(repo.blobStore()).thenReturn(blobStore);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);

        // listBlobs returns a mock bundle name
        final Map<String, org.opensearch.common.blobstore.BlobMetadata> blobsMap = new HashMap<>();
        final org.opensearch.common.blobstore.BlobMetadata blobMetadata = mock(org.opensearch.common.blobstore.BlobMetadata.class);
        blobsMap.put("bundle_1.tar", blobMetadata);
        when(blobContainer.listBlobs()).thenReturn(blobsMap);

        // Setup mock readBlob calls
        final byte[] headerBytes = new byte[1024];
        System.arraycopy("index.bin".getBytes(StandardCharsets.UTF_8), 0, headerBytes, 0, 9);

        final List<NodeBundleRegistry.FileReport> fileReports = new ArrayList<>();
        fileReports.add(new NodeBundleRegistry.FileReport(1L, true, 2000, 15)); // tlg file
        fileReports.add(new NodeBundleRegistry.FileReport(1L, false, 2500, 10)); // ckp file
        final List<NodeBundleRegistry.ShardReport> shardReports = new ArrayList<>();
        shardReports.add(new NodeBundleRegistry.ShardReport("index-uuid", 0, 1L, 1L, fileReports));

        final byte[] indexBinData = serializeIndexBinHelper("node-1", shardReports);
        final String octalSizeStr = String.format(Locale.ROOT, "%011o", indexBinData.length);
        System.arraycopy(octalSizeStr.getBytes(StandardCharsets.UTF_8), 0, headerBytes, 124, 11);
        headerBytes[124 + 11] = 0;

        doAnswer(invocation -> {
            final String blobName = invocation.getArgument(0);
            final long offset = invocation.getArgument(1);
            final long length = invocation.getArgument(2);
            if (offset == 0 && length == 1024) {
                return new ByteArrayInputStream(headerBytes);
            } else if (offset == 512 && length == indexBinData.length) {
                return new ByteArrayInputStream(indexBinData);
            } else if (offset == 2000 && length == 15) {
                return new ByteArrayInputStream("dummy-tlg-bytes".getBytes(StandardCharsets.UTF_8));
            } else if (offset == 2500 && length == 10) {
                return new ByteArrayInputStream("dummy-ckp-bytes".getBytes(StandardCharsets.UTF_8));
            }
            return new ByteArrayInputStream(new byte[0]);
        }).when(blobContainer).readBlob(anyString(), anyLong(), anyLong());

        final TarTranslogUploadStrategy strategy = new TarTranslogUploadStrategy(
            null,
            () -> reposService,
            clusterService,
            mock(Client.class)
        );

        final Path tempDir = createTempDir();
        final ShardId shardId = new ShardId(new Index("test-index", "index-uuid"), 0);

        final boolean result = strategy.downloadTranslog(shardId, "1", "1", tempDir);
        assertTrue(result);

        final Path expectedTlg = tempDir.resolve("translog-1.tlog");
        final Path expectedCkp = tempDir.resolve("translog-1.ckp");
        assertTrue(Files.exists(expectedTlg));
        assertTrue(Files.exists(expectedCkp));
        assertEquals("dummy-tlg-bytes", new String(Files.readAllBytes(expectedTlg), StandardCharsets.UTF_8));
        assertEquals("dummy-ckp-bytes", new String(Files.readAllBytes(expectedCkp), StandardCharsets.UTF_8));
    }

    private byte[] serializeIndexBinHelper(final String nodeId, final List<NodeBundleRegistry.ShardReport> shardReports)
        throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeBytes("TTRI");
            dos.writeByte(1); // Version 1
            final byte[] nodeBytes = nodeId.getBytes(StandardCharsets.UTF_8);
            dos.writeShort(nodeBytes.length);
            dos.write(nodeBytes);

            dos.writeShort(shardReports.size());
            for (final NodeBundleRegistry.ShardReport shard : shardReports) {
                final byte[] uuidBytes = shard.indexUuid.getBytes(StandardCharsets.UTF_8);
                dos.writeShort(uuidBytes.length);
                dos.write(uuidBytes);
                dos.writeInt(shard.shardId);
                dos.writeLong(shard.primaryTerm);
                dos.writeLong(shard.minRemoteGenReferenced);

                dos.writeShort(shard.files.size());
                for (final NodeBundleRegistry.FileReport file : shard.files) {
                    final String name = "translog-" + file.generation + (file.isTlg ? ".tlog" : ".ckp");
                    final byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
                    dos.writeShort(nameBytes.length);
                    dos.write(nameBytes);
                    dos.writeLong(file.generation);
                    dos.writeLong(file.offset);
                    dos.writeLong(file.length);
                }
            }
        }
        return baos.toByteArray();
    }
}
