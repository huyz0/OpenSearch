/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.routing;

import org.apache.lucene.util.CollectionUtil;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.VirtualShardRoutingHelper;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.Strings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.node.ResponseCollectorService;
import org.opensearch.search.slice.SliceBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Routes cluster operations
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class OperationRouting {

    public static final Setting<Boolean> USE_ADAPTIVE_REPLICA_SELECTION_SETTING = Setting.boolSetting(
        "cluster.routing.use_adaptive_replica_selection",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final String IGNORE_AWARENESS_ATTRIBUTES = "cluster.search.ignore_awareness_attributes";
    public static final Setting<Boolean> IGNORE_AWARENESS_ATTRIBUTES_SETTING = Setting.boolSetting(
        IGNORE_AWARENESS_ATTRIBUTES,
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<Double> WEIGHTED_ROUTING_DEFAULT_WEIGHT = Setting.doubleSetting(
        "cluster.routing.weighted.default_weight",
        1.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> WEIGHTED_ROUTING_FAILOPEN_ENABLED = Setting.boolSetting(
        "cluster.routing.weighted.fail_open",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> STRICT_WEIGHTED_SHARD_ROUTING_ENABLED = Setting.boolSetting(
        "cluster.routing.weighted.strict",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> IGNORE_WEIGHTED_SHARD_ROUTING = Setting.boolSetting(
        "cluster.routing.ignore_weighted_routing",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final List<Preference> WEIGHTED_ROUTING_RESTRICTED_PREFERENCES = Arrays.asList(
        Preference.ONLY_NODES,
        Preference.PREFER_NODES
    );

    public static final Setting<Boolean> STRICT_SEARCH_REPLICA_ROUTING_ENABLED = Setting.boolSetting(
        "cluster.routing.search_replica.strict",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile List<String> awarenessAttributes;
    private volatile boolean useAdaptiveReplicaSelection;
    private volatile boolean ignoreAwarenessAttr;
    private volatile double weightedRoutingDefaultWeight;
    private volatile boolean isFailOpenEnabled;
    private volatile boolean isStrictWeightedShardRouting;
    private volatile boolean ignoreWeightedRouting;
    private volatile boolean isStrictSearchOnlyShardRouting;

    public OperationRouting(Settings settings, ClusterSettings clusterSettings) {
        // whether to ignore awareness attributes when routing requests
        this.ignoreAwarenessAttr = clusterSettings.get(IGNORE_AWARENESS_ATTRIBUTES_SETTING);
        this.awarenessAttributes = AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
            this::setAwarenessAttributes
        );
        this.useAdaptiveReplicaSelection = USE_ADAPTIVE_REPLICA_SELECTION_SETTING.get(settings);
        this.weightedRoutingDefaultWeight = WEIGHTED_ROUTING_DEFAULT_WEIGHT.get(settings);
        this.isFailOpenEnabled = WEIGHTED_ROUTING_FAILOPEN_ENABLED.get(settings);
        this.isStrictWeightedShardRouting = STRICT_WEIGHTED_SHARD_ROUTING_ENABLED.get(settings);
        this.ignoreWeightedRouting = IGNORE_WEIGHTED_SHARD_ROUTING.get(settings);
        this.isStrictSearchOnlyShardRouting = STRICT_SEARCH_REPLICA_ROUTING_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(USE_ADAPTIVE_REPLICA_SELECTION_SETTING, this::setUseAdaptiveReplicaSelection);
        clusterSettings.addSettingsUpdateConsumer(IGNORE_AWARENESS_ATTRIBUTES_SETTING, this::setIgnoreAwarenessAttributes);
        clusterSettings.addSettingsUpdateConsumer(WEIGHTED_ROUTING_DEFAULT_WEIGHT, this::setWeightedRoutingDefaultWeight);
        clusterSettings.addSettingsUpdateConsumer(WEIGHTED_ROUTING_FAILOPEN_ENABLED, this::setFailOpenEnabled);
        clusterSettings.addSettingsUpdateConsumer(STRICT_WEIGHTED_SHARD_ROUTING_ENABLED, this::setStrictWeightedShardRouting);
        clusterSettings.addSettingsUpdateConsumer(IGNORE_WEIGHTED_SHARD_ROUTING, this::setIgnoreWeightedRouting);
        clusterSettings.addSettingsUpdateConsumer(STRICT_SEARCH_REPLICA_ROUTING_ENABLED, this::setStrictSearchOnlyShardRouting);
    }

    void setUseAdaptiveReplicaSelection(boolean useAdaptiveReplicaSelection) {
        this.useAdaptiveReplicaSelection = useAdaptiveReplicaSelection;
    }

    void setIgnoreAwarenessAttributes(boolean ignoreAwarenessAttributes) {
        this.ignoreAwarenessAttr = ignoreAwarenessAttributes;
    }

    void setWeightedRoutingDefaultWeight(double weightedRoutingDefaultWeight) {
        this.weightedRoutingDefaultWeight = weightedRoutingDefaultWeight;
    }

    void setFailOpenEnabled(boolean isFailOpenEnabled) {
        this.isFailOpenEnabled = isFailOpenEnabled;
    }

    void setStrictWeightedShardRouting(boolean strictWeightedShardRouting) {
        this.isStrictWeightedShardRouting = strictWeightedShardRouting;
    }

    void setIgnoreWeightedRouting(boolean isWeightedRoundRobinEnabled) {
        this.ignoreWeightedRouting = isWeightedRoundRobinEnabled;
    }

    public boolean isIgnoreAwarenessAttr() {
        return ignoreAwarenessAttr;
    }

    List<String> getAwarenessAttributes() {
        return awarenessAttributes;
    }

    private void setAwarenessAttributes(List<String> awarenessAttributes) {
        this.awarenessAttributes = awarenessAttributes;
    }

    public boolean ignoreAwarenessAttributes() {
        return this.awarenessAttributes.isEmpty() || this.ignoreAwarenessAttr;
    }

    public double getWeightedRoutingDefaultWeight() {
        return this.weightedRoutingDefaultWeight;
    }

    void setStrictSearchOnlyShardRouting(boolean strictSearchOnlyShardRouting) {
        this.isStrictSearchOnlyShardRouting = strictSearchOnlyShardRouting;
    }

    public ShardIterator indexShards(ClusterState clusterState, String index, String id, @Nullable String routing) {
        return shards(clusterState, index, id, routing).shardsIt();
    }

    public ShardIterator getShards(
        ClusterState clusterState,
        String index,
        String id,
        @Nullable String routing,
        @Nullable String preference
    ) {
        return preferenceActiveShardIterator(
            shards(clusterState, index, id, routing),
            clusterState.nodes().getLocalNodeId(),
            clusterState.nodes(),
            preference,
            null,
            null,
            clusterState.getMetadata().weightedRoutingMetadata()
        );
    }

    public ShardIterator getShards(ClusterState clusterState, String index, int shardId, @Nullable String preference) {
        final IndexShardRoutingTable indexShard = clusterState.getRoutingTable().shardRoutingTable(index, shardId);
        return preferenceActiveShardIterator(
            indexShard,
            clusterState.nodes().getLocalNodeId(),
            clusterState.nodes(),
            preference,
            null,
            null,
            clusterState.metadata().weightedRoutingMetadata()
        );
    }

    public GroupShardsIterator<ShardIterator> searchShards(
        ClusterState clusterState,
        String[] concreteIndices,
        @Nullable Map<String, Set<String>> routing,
        @Nullable String preference
    ) {
        return searchShards(clusterState, concreteIndices, routing, preference, null, null, null);
    }

    public GroupShardsIterator<ShardIterator> searchShards(
        ClusterState clusterState,
        String[] concreteIndices,
        @Nullable Map<String, Set<String>> routing,
        @Nullable String preference,
        @Nullable ResponseCollectorService collectorService,
        @Nullable Map<String, Long> nodeCounts,
        @Nullable SliceBuilder slice
    ) {
        final Set<IndexShardRoutingTable> shards = computeTargetedShards(clusterState, concreteIndices, routing);

        Map<Index, List<ShardIterator>> shardIterators = new HashMap<>();
        for (IndexShardRoutingTable shard : shards) {

            IndexMetadata indexMetadataForShard = indexMetadata(clusterState, shard.shardId.getIndex().getName());
            if (indexMetadataForShard.isRemoteSnapshot() && (preference == null || preference.isEmpty())) {
                preference = Preference.PRIMARY.type();
            }

            if (FeatureFlags.isEnabled(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)
                && indexMetadataForShard.getSettings().getAsBoolean(IndexModule.IS_WARM_INDEX_SETTING.getKey(), false)
                && (preference == null || preference.isEmpty())) {
                preference = Preference.PRIMARY_FIRST.type();
            }

            if (preference == null || preference.isEmpty()) {
                if (indexMetadataForShard.getNumberOfSearchOnlyReplicas() > 0 && isStrictSearchOnlyShardRouting) {
                    preference = Preference.SEARCH_REPLICA.type();
                }
            }

            ShardIterator iterator = preferenceActiveShardIterator(
                shard,
                clusterState.nodes().getLocalNodeId(),
                clusterState.nodes(),
                preference,
                collectorService,
                nodeCounts,
                clusterState.metadata().weightedRoutingMetadata()
            );
            if (iterator != null) {
                shardIterators.computeIfAbsent(iterator.shardId().getIndex(), k -> new ArrayList<>()).add(iterator);
            }
        }
        List<ShardIterator> allShardIterators = new ArrayList<>();
        if (slice != null) {
            for (List<ShardIterator> indexIterators : shardIterators.values()) {
                // Filter the returned shards for the given slice
                CollectionUtil.timSort(indexIterators);
                // We use the ordinal of the iterator in the group (after sorting) rather than the shard id, because
                // computeTargetedShards may return a subset of shards for an index, if a routing parameter was
                // specified. In that case, the set of routable shards is considered the full universe of available
                // shards for each index, when mapping shards to slices. If no routing parameter was specified,
                // then ordinals and shard IDs are the same. This mimics the logic in
                // org.opensearch.search.slice.SliceBuilder.toFilter.
                for (int i = 0; i < indexIterators.size(); i++) {
                    if (slice.shardMatches(i, indexIterators.size())) {
                        allShardIterators.add(indexIterators.get(i));
                    }
                }
            }
        } else {
            shardIterators.values().forEach(allShardIterators::addAll);
        }

        return GroupShardsIterator.sortAndCreate(allShardIterators);
    }

    public static ShardIterator getShards(ClusterState clusterState, ShardId shardId) {
        final IndexShardRoutingTable shard = clusterState.routingTable().shardRoutingTable(shardId);
        return shard.activeInitializingShardsRandomIt();
    }

    private static final Map<String, Set<String>> EMPTY_ROUTING = Collections.emptyMap();

    private Set<IndexShardRoutingTable> computeTargetedShards(
        ClusterState clusterState,
        String[] concreteIndices,
        @Nullable Map<String, Set<String>> routing
    ) {
        routing = routing == null ? EMPTY_ROUTING : routing; // just use an empty map
        final Set<IndexShardRoutingTable> set = new HashSet<>();
        // we use set here and not list since we might get duplicates
        for (String index : concreteIndices) {
            // Metadata first, deliberately. An index in neither metadata nor routing is genuinely
            // missing and must still produce IndexNotFoundException; only an index that exists but
            // has no routing entry takes the degraded path below.
            final IndexMetadata indexMetadata = indexMetadata(clusterState, index);
            IndexRoutingTable indexRouting = clusterState.routingTable().index(index);
            if (indexRouting == null) {
                // An installed supplier can compute the entry rather than reporting its absence, which
                // is how a serverless index avoids publishing routing at all. Falls back to Phase A's
                // pessimistic answer when nothing is installed or the supplier declines.
                indexRouting = AbsentIndexRoutingSuppliers.supply(clusterState, indexMetadata);
                if (indexRouting == null) {
                    indexRouting = noShardsAvailable(indexMetadata);
                }
            }
            final Set<String> effectiveRouting = routing.get(index);
            if (effectiveRouting != null) {
                for (String r : effectiveRouting) {
                    final int routingPartitionSize = indexMetadata.getRoutingPartitionSize();
                    for (int partitionOffset = 0; partitionOffset < routingPartitionSize; partitionOffset++) {
                        set.add(RoutingTable.shardRoutingTable(indexRouting, calculateScaledShardId(indexMetadata, r, partitionOffset)));
                    }
                }
            } else {
                for (IndexShardRoutingTable indexShard : indexRouting) {
                    set.add(indexShard);
                }
            }

        }
        return set;
    }

    private ShardIterator preferenceActiveShardIterator(
        IndexShardRoutingTable indexShard,
        String localNodeId,
        DiscoveryNodes nodes,
        @Nullable String preference,
        @Nullable ResponseCollectorService collectorService,
        @Nullable Map<String, Long> nodeCounts,
        @Nullable WeightedRoutingMetadata weightedRoutingMetadata
    ) {
        if (preference == null || preference.isEmpty()) {
            return shardRoutings(indexShard, nodes, collectorService, nodeCounts, weightedRoutingMetadata);
        }

        if (preference.charAt(0) == '_') {
            Preference preferenceType = Preference.parse(preference);
            if (preferenceType == Preference.SHARDS) {
                // starts with _shards, so execute on specific ones
                int index = preference.indexOf('|');

                String shards;
                if (index == -1) {
                    shards = preference.substring(Preference.SHARDS.type().length() + 1);
                } else {
                    shards = preference.substring(Preference.SHARDS.type().length() + 1, index);
                }
                String[] ids = Strings.splitStringByCommaToArray(shards);
                boolean found = false;
                for (String id : ids) {
                    if (Integer.parseInt(id) == indexShard.shardId().id()) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return null;
                }
                // no more preference
                if (index == -1 || index == preference.length() - 1) {
                    return shardRoutings(indexShard, nodes, collectorService, nodeCounts, weightedRoutingMetadata);
                } else {
                    // update the preference and continue
                    preference = preference.substring(index + 1);
                }
            }
            preferenceType = Preference.parse(preference);
            checkPreferenceBasedRoutingAllowed(preferenceType, weightedRoutingMetadata);
            switch (preferenceType) {
                case PREFER_NODES:
                    final Set<String> nodesIds = Arrays.stream(preference.substring(Preference.PREFER_NODES.type().length() + 1).split(","))
                        .collect(Collectors.toSet());
                    return indexShard.preferNodeActiveInitializingShardsIt(nodesIds);
                case LOCAL:
                    return indexShard.preferNodeActiveInitializingShardsIt(Collections.singleton(localNodeId));
                case PRIMARY:
                    return indexShard.primaryActiveInitializingShardIt();
                case REPLICA:
                    return indexShard.replicaActiveInitializingShardIt();
                case PRIMARY_FIRST:
                    return indexShard.primaryFirstActiveInitializingShardsIt();
                case REPLICA_FIRST:
                    return indexShard.replicaFirstActiveInitializingShardsIt();
                case SEARCH_REPLICA:
                    return indexShard.searchReplicaActiveInitializingShardIt();
                case ONLY_LOCAL:
                    return indexShard.onlyNodeActiveInitializingShardsIt(localNodeId);
                case ONLY_NODES:
                    String nodeAttributes = preference.substring(Preference.ONLY_NODES.type().length() + 1);
                    return indexShard.onlyNodeSelectorActiveInitializingShardsIt(nodeAttributes.split(","), nodes);
                default:
                    throw new IllegalArgumentException("unknown preference [" + preferenceType + "]");
            }
        }
        // if not, then use it as the index
        int routingHash = Murmur3HashFunction.hash(preference);
        // The AllocationService lists shards in a fixed order based on nodes
        // so earlier versions of this class would have a tendency to
        // select the same node across different shardIds.
        // Better overall balancing can be achieved if each shardId opts
        // for a different element in the list by also incorporating the
        // shard ID into the hash of the user-supplied preference key.
        routingHash = 31 * routingHash + indexShard.shardId.hashCode();
        if (WeightedRoutingUtils.shouldPerformStrictWeightedRouting(
            isStrictWeightedShardRouting,
            ignoreWeightedRouting,
            weightedRoutingMetadata
        )) {
            return indexShard.activeInitializingShardsWeightedIt(
                weightedRoutingMetadata.getWeightedRouting(),
                nodes,
                getWeightedRoutingDefaultWeight(),
                isFailOpenEnabled,
                routingHash
            );
        } else {
            return indexShard.activeInitializingShardsIt(routingHash);
        }
    }

    private ShardIterator shardRoutings(
        IndexShardRoutingTable indexShard,
        DiscoveryNodes nodes,
        @Nullable ResponseCollectorService collectorService,
        @Nullable Map<String, Long> nodeCounts,
        @Nullable WeightedRoutingMetadata weightedRoutingMetadata
    ) {
        if (WeightedRoutingUtils.shouldPerformWeightedRouting(ignoreWeightedRouting, weightedRoutingMetadata)) {
            return indexShard.activeInitializingShardsWeightedIt(
                weightedRoutingMetadata.getWeightedRouting(),
                nodes,
                getWeightedRoutingDefaultWeight(),
                isFailOpenEnabled,
                null
            );
        } else if (ignoreAwarenessAttributes()) {
            if (useAdaptiveReplicaSelection) {
                return indexShard.activeInitializingShardsRankedIt(collectorService, nodeCounts);
            } else {
                return indexShard.activeInitializingShardsRandomIt();
            }
        } else {
            return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes);
        }
    }

    /**
     * A routing table for an index that is present in metadata but has no {@link IndexRoutingTable}
     * entry: the right number of shards, each with no copies anywhere.
     *
     * <p>This is what makes such an index behave like one whose shards merely happen to be
     * unassigned -- a search returns HTTP 200 with a per-shard {@code NoShardAvailableActionException}
     * for each -- rather than {@code IndexNotFoundException}, which is a 404 and says something that
     * is not true.
     *
     * <p>Synthesising empty shards rather than skipping the index is the point. Skipping would leave
     * the search with nothing to route to and it would return 200 with zero hits, silently, which is
     * a worse answer than a loud failure for an index that exists and holds data. The failure a
     * caller gets here is the same one it already knows how to handle and, for the serverless
     * scale-to-zero case, the same one that reactivation is racing to prevent.
     */
    private static IndexRoutingTable noShardsAvailable(IndexMetadata indexMetadata) {
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(indexMetadata.getIndex());
        for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
            builder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(indexMetadata.getIndex(), shardId)).build());
        }
        return builder.build();
    }

    protected IndexRoutingTable indexRoutingTable(ClusterState clusterState, String index) {
        IndexRoutingTable indexRouting = clusterState.routingTable().index(index);
        if (indexRouting == null) {
            throw new IndexNotFoundException(index);
        }
        return indexRouting;
    }

    /**
     * Site 7, and it is the one every other routing decision here is built on.
     *
     * <p>Routing a document means hashing its id against a shard count, and the shard count is the first
     * thing a gated index cannot supply from cluster state. Repaired here rather than at each of the four
     * callers below, because they all reach the same question through this method and site 8 -- the routing
     * table itself -- already has its supplier fallback and simply never ran, since this threw first.
     */
    protected IndexMetadata indexMetadata(ClusterState clusterState, String index) {
        IndexMetadata indexMetadata = clusterState.metadata().indexOrResolved(index);
        if (indexMetadata == null) {
            throw new IndexNotFoundException(index);
        }
        return indexMetadata;
    }

    protected IndexShardRoutingTable shards(ClusterState clusterState, String index, String id, String routing) {
        final IndexMetadata indexMetadata = indexMetadata(clusterState, index);
        int shardId = generateShardId(indexMetadata, id, routing);

        // The single-document path resolves separately from searchShards, so it needs the same
        // supplier fallback. Without this a computed index answers searches and fails writes and gets,
        // which is worse than not supplying at all: the failure looks like a missing index rather than
        // like an unsupported configuration.
        if (clusterState.routingTable().hasIndex(index) == false) {
            IndexRoutingTable supplied = AbsentIndexRoutingSuppliers.supply(clusterState, indexMetadata);
            if (supplied != null) {
                IndexShardRoutingTable shard = supplied.shard(shardId);
                if (shard != null) {
                    return shard;
                }
            }
        }
        return clusterState.getRoutingTable().shardRoutingTable(index, shardId);
    }

    public ShardId shardId(ClusterState clusterState, String index, String id, @Nullable String routing) {
        IndexMetadata indexMetadata = indexMetadata(clusterState, index);
        return new ShardId(indexMetadata.getIndex(), generateShardId(indexMetadata, id, routing));
    }

    public static int generateShardId(IndexMetadata indexMetadata, @Nullable String id, @Nullable String routing) {
        final String effectiveRouting;
        final int partitionOffset;

        if (routing == null) {
            assert (indexMetadata.isRoutingPartitionedIndex() == false) : "A routing value is required for gets from a partitioned index";
            effectiveRouting = id;
        } else {
            effectiveRouting = routing;
        }

        if (indexMetadata.isRoutingPartitionedIndex()) {
            partitionOffset = Math.floorMod(Murmur3HashFunction.hash(id), indexMetadata.getRoutingPartitionSize());
        } else {
            // we would have still got 0 above but this check just saves us an unnecessary hash calculation
            partitionOffset = 0;
        }

        int numVirtualShards = indexMetadata.getNumberOfVirtualShards();
        if (numVirtualShards != -1) {
            final int hash = Murmur3HashFunction.hash(effectiveRouting) + partitionOffset;
            int vShardId = Math.floorMod(hash, numVirtualShards);
            return VirtualShardRoutingHelper.resolvePhysicalShardId(indexMetadata, vShardId);
        }

        return calculateScaledShardId(indexMetadata, effectiveRouting, partitionOffset);
    }

    private static int calculateScaledShardId(IndexMetadata indexMetadata, String effectiveRouting, int partitionOffset) {
        final int hash = Murmur3HashFunction.hash(effectiveRouting) + partitionOffset;

        // we don't use IMD#getNumberOfShards since the index might have been shrunk such that we need to use the size
        // of original index to hash documents
        int rootShardId = Math.floorMod(hash, indexMetadata.getRoutingNumShards()) / indexMetadata.getRoutingFactor();

        return indexMetadata.getSplitShardsMetadata().getShardIdOfHash(rootShardId, hash);
    }

    private void checkPreferenceBasedRoutingAllowed(Preference preference, @Nullable WeightedRoutingMetadata weightedRoutingMetadata) {
        if (WeightedRoutingUtils.shouldPerformStrictWeightedRouting(
            isStrictWeightedShardRouting,
            ignoreWeightedRouting,
            weightedRoutingMetadata
        ) && WEIGHTED_ROUTING_RESTRICTED_PREFERENCES.contains(preference)) {
            throw new PreferenceBasedSearchNotAllowedException(
                "Preference type based routing not allowed with strict weighted shard routing enabled"
            );
        }
    }
}
