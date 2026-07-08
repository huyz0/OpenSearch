/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.opensearch.common.settings.Settings;
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
}
