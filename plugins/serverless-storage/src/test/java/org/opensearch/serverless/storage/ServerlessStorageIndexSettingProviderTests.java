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
import org.opensearch.index.IndexModule;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.serverless.storage.allocation.ServerlessStorageExistingShardsAllocator;
import org.opensearch.test.OpenSearchTestCase;

public class ServerlessStorageIndexSettingProviderTests extends OpenSearchTestCase {

    public void testInjectsTheCustomAllocatorForAnIndexThatOptsIntoServerlessStorage() {
        Settings requestSettings = Settings.builder()
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
            .build();

        Settings additional = new ServerlessStorageIndexSettingProvider().getAdditionalIndexSettings("my-index", false, requestSettings);

        assertEquals(
            ServerlessStorageExistingShardsAllocator.NAME,
            additional.get(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey())
        );
    }

    public void testAddsNothingForAnOrdinaryIndex() {
        Settings additional = new ServerlessStorageIndexSettingProvider().getAdditionalIndexSettings("my-index", false, Settings.EMPTY);

        assertTrue("an index that never opted in must get no additional settings at all", additional.isEmpty());
    }

    public void testRejectsExplicitSegmentReplicationOnAServerlessStorageIndex() {
        Settings requestSettings = Settings.builder()
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT.toString())
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ServerlessStorageIndexSettingProvider().getAdditionalIndexSettings("my-index", false, requestSettings)
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

        Settings additional = new ServerlessStorageIndexSettingProvider().getAdditionalIndexSettings("my-index", false, requestSettings);

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

        Settings additional = new ServerlessStorageIndexSettingProvider().getAdditionalIndexSettings("my-index", false, requestSettings);

        assertEquals(
            ServerlessStorageExistingShardsAllocator.NAME,
            additional.get(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey())
        );
    }

    public void testDoesNotRejectSegmentReplicationForAnOrdinaryIndex() {
        Settings requestSettings = Settings.builder()
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT.toString())
            .build();

        Settings additional = new ServerlessStorageIndexSettingProvider().getAdditionalIndexSettings("my-index", false, requestSettings);

        assertTrue("an index that never opted into serverless storage must never be rejected on this basis", additional.isEmpty());
    }

    public void testTranslatesLazyDirectoryEnabledIntoIndexStoreType() {
        Settings requestSettings = Settings.builder()
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_LAZY_DIRECTORY_ENABLED_SETTING.getKey(), true)
            .build();

        Settings additional = new ServerlessStorageIndexSettingProvider().getAdditionalIndexSettings("my-index", false, requestSettings);

        assertEquals(ServerlessStoragePlugin.LAZY_DIRECTORY_STORE_TYPE, additional.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey()));
    }

    public void testDoesNotSetIndexStoreTypeWhenLazyDirectoryIsNotEnabled() {
        Settings requestSettings = Settings.builder()
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
            .build();

        Settings additional = new ServerlessStorageIndexSettingProvider().getAdditionalIndexSettings("my-index", false, requestSettings);

        assertNull(
            "index.store.type must stay unset (normal FSDirectory) unless the lazy directory feature is explicitly enabled",
            additional.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey())
        );
    }
}
