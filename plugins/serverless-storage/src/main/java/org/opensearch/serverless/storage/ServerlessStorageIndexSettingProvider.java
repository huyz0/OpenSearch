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
import org.opensearch.index.shard.IndexSettingProvider;
import org.opensearch.serverless.storage.allocation.ServerlessStorageExistingShardsAllocator;

/**
 * Automatically selects {@link ServerlessStorageExistingShardsAllocator} for every index that opts
 * into serverless storage ({@link ServerlessStoragePlugin#SERVERLESS_STORAGE_ENABLED_SETTING}),
 * rfc-serverless-opensearch.md &sect;7.1.2. This has to happen here, not by asking operators to set
 * {@code index.allocation.existing_shards_allocator} themselves: that setting carries {@link
 * ExistingShardsAllocator#EXISTING_SHARDS_ALLOCATOR_SETTING}'s own {@code PrivateIndex} property,
 * which forbids setting it directly on index creation -- {@link IndexSettingProvider} is precisely
 * the seam for injecting a private, managed default in response to another (public) setting the
 * request already carries, the same shape core itself uses this interface for.
 */
public final class ServerlessStorageIndexSettingProvider implements IndexSettingProvider {

    @Override
    public Settings getAdditionalIndexSettings(String indexName, boolean isDataStreamIndex, Settings templateAndRequestSettings) {
        if (ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.get(templateAndRequestSettings) == false) {
            return Settings.EMPTY;
        }
        return Settings.builder()
            .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), ServerlessStorageExistingShardsAllocator.NAME)
            .build();
    }
}
