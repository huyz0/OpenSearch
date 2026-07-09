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
import org.opensearch.index.shard.IndexSettingProvider;
import org.opensearch.indices.replication.common.ReplicationType;
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
 *
 * <p>Also translates {@link ServerlessStoragePlugin#SERVERLESS_STORAGE_LAZY_DIRECTORY_ENABLED_SETTING}
 * into {@code index.store.type} = {@link ServerlessStoragePlugin#LAZY_DIRECTORY_STORE_TYPE} --
 * same indirection, different underlying mechanism: {@code index.store.type} isn't a private
 * setting, but hiding the raw store-type string behind this plugin's own on/off setting keeps
 * operators from needing to know the store-type value at all, consistent with how every other
 * optional feature in this plugin is a single boolean/config knob, not raw core plumbing.
 *
 * <p>Finally, rejects explicit {@code index.replication.type: SEGMENT} on a serverless-storage
 * index at creation time (rfc-serverless-opensearch.md &sect;18 risk #7, "indices can't mix
 * modes; enforce at index-settings validation"). Serverless storage's own manifest publication
 * (&sect;8) already *is* the shard's segment replication mechanism -- reader shards pick up new
 * segments by polling for a newer published manifest, not via core's peer-to-peer segment-copy
 * protocol -- so also requesting core's {@code SEGMENT} replication type would configure a second,
 * conflicting distribution mechanism for the same shard rather than a real, usable feature
 * combination.
 */
public final class ServerlessStorageIndexSettingProvider implements IndexSettingProvider {

    /** Creates a provider with no configuration state; all decisions are derived from the settings passed to it. */
    public ServerlessStorageIndexSettingProvider() {}

    @Override
    public Settings getAdditionalIndexSettings(String indexName, boolean isDataStreamIndex, Settings templateAndRequestSettings) {
        if (ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.get(templateAndRequestSettings) == false) {
            return Settings.EMPTY;
        }
        if (templateAndRequestSettings.hasValue(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey())
            && IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.get(templateAndRequestSettings) == ReplicationType.SEGMENT) {
            throw new IllegalArgumentException(
                "index ["
                    + indexName
                    + "] cannot combine "
                    + ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey()
                    + "=true with "
                    + IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey()
                    + "="
                    + ReplicationType.SEGMENT
                    + ": serverless storage's own manifest publication already is this shard's segment "
                    + "replication mechanism, so requesting core's segment replication as well would "
                    + "configure two conflicting distribution mechanisms for the same shard"
            );
        }
        Settings.Builder settings = Settings.builder()
            .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), ServerlessStorageExistingShardsAllocator.NAME);
        if (ServerlessStoragePlugin.SERVERLESS_STORAGE_LAZY_DIRECTORY_ENABLED_SETTING.get(templateAndRequestSettings)) {
            settings.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), ServerlessStoragePlugin.LAZY_DIRECTORY_STORE_TYPE);
        }
        return settings.build();
    }
}
