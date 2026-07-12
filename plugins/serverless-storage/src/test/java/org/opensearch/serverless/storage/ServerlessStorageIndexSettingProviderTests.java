/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.index.IndexModule;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.serverless.storage.allocation.ServerlessStorageExistingShardsAllocator;
import org.opensearch.test.OpenSearchTestCase;

public class ServerlessStorageIndexSettingProviderTests extends OpenSearchTestCase {

    /**
     * Every test unrelated to the mandatory-remote-cluster-state check itself needs this wired on,
     * exactly as {@code ServerlessStoragePlugin#createComponents} always wires it before any real
     * index-creation request is ever handled -- {@code remoteClusterStateEnabled} defaults to
     * {@code false} only in the narrow window before that setter runs (see the field's own javadoc).
     */
    private static ServerlessStorageIndexSettingProvider newProviderWithRemoteClusterStateEnabled() {
        ServerlessStorageIndexSettingProvider provider = new ServerlessStorageIndexSettingProvider();
        provider.setRemoteClusterStateEnabled(true);
        return provider;
    }

    public void testRejectsServerlessStorageWhenRemoteClusterStateIsNotEnabled() {
        Settings requestSettings = Settings.builder()
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ServerlessStorageIndexSettingProvider().getAdditionalIndexSettings("my-index", false, requestSettings)
        );
        assertTrue(e.getMessage().contains(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey()));
        assertTrue(e.getMessage().contains(RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey()));
    }

    public void testAllowsServerlessStorageWhenRemoteClusterStateIsEnabled() {
        Settings requestSettings = Settings.builder()
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
            .build();

        Settings additional = newProviderWithRemoteClusterStateEnabled().getAdditionalIndexSettings("my-index", false, requestSettings);

        assertEquals(
            ServerlessStorageExistingShardsAllocator.NAME,
            additional.get(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey())
        );
    }

    public void testMissingRemoteClusterStateDoesNotAffectAnOrdinaryIndex() {
        Settings additional = new ServerlessStorageIndexSettingProvider().getAdditionalIndexSettings("my-index", false, Settings.EMPTY);

        assertTrue("an index that never opted into serverless storage must never be rejected on this basis", additional.isEmpty());
    }

    public void testInjectsTheCustomAllocatorForAnIndexThatOptsIntoServerlessStorage() {
        Settings requestSettings = Settings.builder()
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
            .build();

        Settings additional = newProviderWithRemoteClusterStateEnabled().getAdditionalIndexSettings("my-index", false, requestSettings);

        assertEquals(
            ServerlessStorageExistingShardsAllocator.NAME,
            additional.get(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey())
        );
    }

    public void testAddsNothingForAnOrdinaryIndex() {
        Settings additional = newProviderWithRemoteClusterStateEnabled().getAdditionalIndexSettings("my-index", false, Settings.EMPTY);

        assertTrue("an index that never opted in must get no additional settings at all", additional.isEmpty());
    }

    public void testRejectsExplicitSegmentReplicationOnAServerlessStorageIndex() {
        Settings requestSettings = Settings.builder()
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT.toString())
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> newProviderWithRemoteClusterStateEnabled().getAdditionalIndexSettings("my-index", false, requestSettings)
        );
        assertTrue(e.getMessage().contains(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey()));
        assertTrue(e.getMessage().contains(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey()));
    }

    public void testAllowsDefaultDocumentReplicationOnAServerlessStorageIndex() {
        // The default (no explicit index.replication.type) must still be accepted -- only an
        // explicit request for SEGMENT replication conflicts with serverless storage's own
        // manifest-publication-as-replication mechanism.
        Settings requestSettings = Settings.builder()
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
            .build();

        Settings additional = newProviderWithRemoteClusterStateEnabled().getAdditionalIndexSettings("my-index", false, requestSettings);

        assertEquals(
            ServerlessStorageExistingShardsAllocator.NAME,
            additional.get(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey())
        );
    }

    public void testAllowsExplicitDocumentReplicationOnAServerlessStorageIndex() {
        Settings requestSettings = Settings.builder()
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.DOCUMENT.toString())
            .build();

        Settings additional = newProviderWithRemoteClusterStateEnabled().getAdditionalIndexSettings("my-index", false, requestSettings);

        assertEquals(
            ServerlessStorageExistingShardsAllocator.NAME,
            additional.get(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey())
        );
    }

    public void testAllowsExplicitSegmentReplicationWithZeroWriterReplicas() {
        // Zero writer replicas means core's peer-to-peer segment-copy protocol never actually
        // engages for this index -- SEGMENT is only requested here to satisfy core's own
        // prerequisite chain for search-only replicas (index.number_of_search_replicas requires
        // index.remote_store.enabled, which requires index.replication.type: SEGMENT). Rejecting
        // this would make reader-shard creation via number_of_search_replicas impossible for every
        // serverless-storage index (rfc-serverless-opensearch.md &sect;18 risk #10).
        Settings requestSettings = Settings.builder()
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT.toString())
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        Settings additional = newProviderWithRemoteClusterStateEnabled().getAdditionalIndexSettings("my-index", false, requestSettings);

        assertEquals(
            ServerlessStorageExistingShardsAllocator.NAME,
            additional.get(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey())
        );
    }

    public void testDoesNotRejectSegmentReplicationForAnOrdinaryIndex() {
        Settings requestSettings = Settings.builder()
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT.toString())
            .build();

        Settings additional = newProviderWithRemoteClusterStateEnabled().getAdditionalIndexSettings("my-index", false, requestSettings);

        assertTrue("an index that never opted into serverless storage must never be rejected on this basis", additional.isEmpty());
    }

    public void testTranslatesLazyDirectoryEnabledIntoIndexStoreType() {
        Settings requestSettings = Settings.builder()
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_LAZY_DIRECTORY_ENABLED_SETTING.getKey(), true)
            .build();

        Settings additional = newProviderWithRemoteClusterStateEnabled().getAdditionalIndexSettings("my-index", false, requestSettings);

        assertEquals(ServerlessStoragePlugin.LAZY_DIRECTORY_STORE_TYPE, additional.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey()));
    }

    public void testDoesNotSetIndexStoreTypeWhenLazyDirectoryIsNotEnabled() {
        Settings requestSettings = Settings.builder()
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
            .build();

        Settings additional = newProviderWithRemoteClusterStateEnabled().getAdditionalIndexSettings("my-index", false, requestSettings);

        assertNull(
            "index.store.type must stay unset (normal FSDirectory) unless the lazy directory feature is explicitly enabled",
            additional.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey())
        );
    }
}
