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
import org.opensearch.serverless.storage.descriptor.DescriptorOnlyCreation;
import org.opensearch.serverless.storage.resharding.DataStreamBackingIndexNames;
import org.opensearch.serverless.storage.resharding.DataStreamShardCountAdvisorCache;
import org.opensearch.serverless.storage.writerengine.ObjectStoreShardRecoveryStrategy;

import java.util.Optional;
import java.util.OptionalInt;

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
 * <p>Selects this plugin's {@link ObjectStoreShardRecoveryStrategy} the same way and for the same
 * reason: {@code index.recovery.strategy} is the seam core resolves a shard's store-population
 * behavior through, and an operator opting an index into serverless storage should not have to know
 * the strategy's name to get the behavior the opt-in implies. It pairs with the allocator above --
 * that decides where a writer shard goes after a node loss, this is what lets it come back there
 * with no peer to copy from.
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
 *
 * <p><b>Also injects a write-load-driven {@code number_of_shards} for a new data-stream backing
 * index, the Elasticsearch-Serverless-style "autosharding" half of this plugin's write-scaling
 * story</b> (rfc-serverless-opensearch.md &sect;16 Phase 4): when {@link DataStreamShardCountAdvisorCache}
 * (populated off the hot path by {@code DataStreamShardCountAdvisorSchedulerTask}) has a
 * recommendation for the data stream this new backing index belongs to (parsed from the index's
 * own about-to-be-created name via {@link DataStreamBackingIndexNames#parseDataStreamName}), that
 * recommendation is injected here. This hook is deliberately the only place that can safely act on
 * the signal: it has no {@code ClusterState} access and cannot itself compute the signal (see the
 * cache class's own javadoc), but it is exactly where core already lets a plugin influence a new
 * index's settings without any new core seam, and it never touches an index that already exists --
 * the same "only ever influence what's not created yet" safety property real Elastic Cloud
 * Serverless autosharding has.
 */
public final class ServerlessStorageIndexSettingProvider implements IndexSettingProvider {

    private volatile DataStreamShardCountAdvisorCache shardCountAdvisorCache;

    /** Creates a provider with no configuration state; all decisions are derived from the settings passed to it. */
    public ServerlessStorageIndexSettingProvider() {}

    /**
     * Supplies the cache this provider consults for a new data-stream backing index's recommended
     * shard count, once available -- this provider is constructed and registered before {@code
     * createComponents} runs, the same "instantiate early, wire in late" shape other
     * eagerly-constructed components in this plugin already use.
     *
     * @param shardCountAdvisorCache the shared cache {@code DataStreamShardCountAdvisorSchedulerTask} publishes into.
     */
    public void setDependencies(DataStreamShardCountAdvisorCache shardCountAdvisorCache) {
        this.shardCountAdvisorCache = shardCountAdvisorCache;
    }

    @Override
    public Settings getAdditionalIndexSettings(String indexName, boolean isDataStreamIndex, Settings templateAndRequestSettings) {
        Settings.Builder dataStreamSettings = Settings.builder();
        if (isDataStreamIndex) {
            DataStreamShardCountAdvisorCache currentCache = this.shardCountAdvisorCache;
            if (currentCache != null && templateAndRequestSettings.hasValue(IndexMetadata.SETTING_NUMBER_OF_SHARDS) == false) {
                Optional<String> dataStreamName = DataStreamBackingIndexNames.parseDataStreamName(indexName);
                if (dataStreamName.isPresent()) {
                    OptionalInt recommendedShardCount = currentCache.recommendedShardCount(dataStreamName.get());
                    if (recommendedShardCount.isPresent()) {
                        dataStreamSettings.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, recommendedShardCount.getAsInt());
                    }
                }
            }
        }
        // The name is what decides serverless-ness since the namespace was introduced, so an index in it
        // carries the setting whether or not the request or a template said so. Derived here rather than
        // asserted at the gate because everything downstream -- storage, computed placement, the engine --
        // already keys off the setting, and rewriting all of that to ask about the name would be a much
        // larger change for the same answer. See DescriptorOnlyCreation#SERVERLESS_NAME_PREFIX.
        final boolean namespaced = DescriptorOnlyCreation.namesAServerlessIndex(indexName);
        if (namespaced) {
            dataStreamSettings.put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true);
        }
        if (namespaced == false && ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.get(templateAndRequestSettings) == false) {
            return dataStreamSettings.build();
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
            .put(dataStreamSettings.build())
            .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), ServerlessStorageExistingShardsAllocator.NAME)
            // Same indirection, one layer further in: an operator opting an index into serverless storage should
            // not also have to know the name of the shard-recovery strategy that implements it. Injected here for
            // the same reason the allocator above is -- it is a managed consequence of the public setting the
            // request already carries, and the two are a pair: the allocator decides where a writer shard goes
            // after a node loss, this strategy is what lets it come back with no peer to copy from.
            .put(IndexModule.INDEX_RECOVERY_STRATEGY_SETTING.getKey(), ObjectStoreShardRecoveryStrategy.NAME);
        if (ServerlessStoragePlugin.SERVERLESS_STORAGE_LAZY_DIRECTORY_ENABLED_SETTING.get(templateAndRequestSettings)) {
            settings.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), ServerlessStoragePlugin.LAZY_DIRECTORY_STORE_TYPE);
        }
        return settings.build();
    }
}
