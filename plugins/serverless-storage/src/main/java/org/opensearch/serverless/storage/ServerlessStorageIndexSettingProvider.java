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
 * <p>Finally, rejects explicit {@code index.replication.type: SEGMENT} <em>together with at least
 * one writer replica</em> ({@code index.number_of_replicas > 0}) on a serverless-storage index at
 * creation time (rfc-serverless-opensearch.md &sect;18 risk #7, "indices can't mix modes; enforce
 * at index-settings validation"). Serverless storage's own manifest publication (&sect;8) already
 * *is* the shard's segment replication mechanism for its writer-side replicas -- so also
 * requesting core's peer-to-peer {@code SEGMENT} replication for those same writer replicas would
 * configure a second, conflicting distribution mechanism for the same shard.
 *
 * <p>Deliberately scoped to writer replicas only, not to every explicit {@code SEGMENT} request:
 * a search-only shard copy never goes through core's peer-to-peer segment-copy protocol regardless
 * of this setting (routed via {@code RecoverySource.EmptyStoreRecoverySource} purely off {@link
 * org.opensearch.cluster.routing.ShardRouting#isSearchOnly()}, see {@code ShardRouting}'s own
 * {@code initializeUnassignedShard}), so an index with zero writer replicas requesting {@code
 * SEGMENT} only to satisfy core's own prerequisite chain for search-only replicas ({@code
 * index.number_of_search_replicas} requires {@code index.remote_store.enabled}, which itself
 * requires {@code index.replication.type: SEGMENT}, per {@code
 * IndexMetadata#INDEX_REMOTE_STORE_ENABLED_SETTING}'s validator) has no real conflicting mechanism
 * to reject -- rejecting it anyway would make reader-shard creation via {@code
 * index.number_of_search_replicas} impossible for every serverless-storage index, closing off this
 * plugin's only entry point into that path (rfc-serverless-opensearch.md &sect;18 risk #10).
 */
public final class ServerlessStorageIndexSettingProvider implements IndexSettingProvider {

    /** Creates a provider with no configuration state; all decisions are derived from the settings passed to it. */
    public ServerlessStorageIndexSettingProvider() {}

    // Set once, via #setRemoteClusterStateEnabled, by ServerlessStoragePlugin#createComponents --
    // constructed eagerly (getAdditionalIndexSettingProviders runs before createComponents, the
    // same ordering ServerlessStorageExistingShardsAllocator's own late-setter already works
    // around), so this defaults to false until createComponents runs. Every real index-creation
    // request is handled long after node startup completes, so by the time this provider is ever
    // actually asked to validate a request, the setter has always already run.
    private volatile boolean remoteClusterStateEnabled;

    /**
     * Wires this provider's mandatory-remote-cluster-state enforcement (rfc-serverless-opensearch.md
     * &sect;10, "cluster state: remote cluster state ... becomes mandatory in serverless mode") --
     * late-setter for the same reason {@code ShardReactivationActionFilter#setDependencies} and
     * {@code ServerlessStorageExistingShardsAllocator#setDependencies} already are.
     *
     * @param remoteClusterStateEnabled the node's resolved {@link RemoteClusterStateService#REMOTE_CLUSTER_STATE_ENABLED_SETTING} value.
     */
    public void setRemoteClusterStateEnabled(boolean remoteClusterStateEnabled) {
        this.remoteClusterStateEnabled = remoteClusterStateEnabled;
    }

    @Override
    public Settings getAdditionalIndexSettings(String indexName, boolean isDataStreamIndex, Settings templateAndRequestSettings) {
        if (ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.get(templateAndRequestSettings) == false) {
            return Settings.EMPTY;
        }
        if (remoteClusterStateEnabled == false) {
            throw new IllegalArgumentException(
                "index ["
                    + indexName
                    + "] cannot set "
                    + ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey()
                    + "=true because this node does not have "
                    + RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey()
                    + "=true: serverless storage requires remote cluster state to be enabled "
                    + "cluster-wide (rfc-serverless-opensearch.md section 10)"
            );
        }
        int numberOfWriterReplicas = IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(templateAndRequestSettings);
        if (numberOfWriterReplicas > 0
            && templateAndRequestSettings.hasValue(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey())
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
                    + " and "
                    + IndexMetadata.SETTING_NUMBER_OF_REPLICAS
                    + "="
                    + numberOfWriterReplicas
                    + ": serverless storage's own manifest publication already is this shard's segment "
                    + "replication mechanism for its writer replicas, so requesting core's segment "
                    + "replication for those same replicas would configure two conflicting "
                    + "distribution mechanisms for the same shard"
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
