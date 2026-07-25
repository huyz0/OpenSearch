/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.clusterstate;

import org.opensearch.Version;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;

import java.util.Map;

/**
 * The compact per-index record a coordinating node would hold instead of a full
 * {@link IndexMetadata}, with a field set derived from an audit (spike S1) of every
 * coordinating-path read of {@code metadata().index(...)}, {@code getIndicesLookup()}, and
 * {@code routingTable()} in {@code server/src/main/java}.
 *
 * <p>The audit classified each call site as satisfiable from these fields or as requiring the
 * full object. Result: the write path and wildcard/alias resolution are fully satisfiable; the
 * only hot-path gap was {@code SearchPipelineService}'s read of {@code index.search.default_pipeline},
 * covered here by {@link #defaultSearchPipelineId}. Remaining full-object readers are admin APIs
 * (settings, get-index, resize, stats) plus rollover and ingest-pipeline resolution, all of which
 * know their target index name and can fetch on demand.
 *
 * <p>Fields deliberately excluded, and the reason each is safe to exclude:
 * <ul>
 *   <li>the mapping body — only {@code mapping().routingRequired()} is read on the coordinating
 *       path, carried here as a boolean;</li>
 *   <li>the {@link org.opensearch.common.settings.Settings} object — every coordinating-path
 *       consumer reads one of a small number of specific settings, each carried here as a typed
 *       field;</li>
 *   <li>{@code DiscoveryNodeFilters}, {@code rolloverInfos}, {@code customData}, {@code Context},
 *       {@code IngestionStatus} — no coordinating-path reader.</li>
 * </ul>
 *
 * <p>This is a measurement subject, not a proposed core type: it exists so the descriptor's real
 * retained size can be measured against the same baseline as {@link TenantIndexRetentionBenchmark}
 * rather than estimated.
 */
final class RoutingDescriptor {

    // Identity.
    final String name;
    final String uuid;

    // Routing math -- OperationRouting#generateShardId / #calculateScaledShardId.
    final int routingNumShards;
    final int routingFactor;
    final int routingPartitionSize;
    final int numberOfVirtualShards;
    final int numberOfShards;
    final int numberOfReplicas;
    final int numberOfSearchOnlyReplicas;

    // Small typed fields standing in for specific settings reads.
    final IndexMetadata.State state;
    final Version creationVersion;
    final ActiveShardCount waitForActiveShards;
    final String defaultSearchPipelineId;

    // Flags. Kept as separate booleans rather than a packed bitset so the measurement reflects
    // the straightforward implementation; packing would only shrink the result.
    final boolean isSystem;
    final boolean isAppendOnly;
    final boolean isRemoteSnapshot;
    final boolean isWarm;
    final boolean isHidden;
    final boolean bulkAdaptiveShardSelectionEnabled;
    final boolean frozen;
    final boolean routingRequired;

    // Version longs, needed both for diffing and to make the remote detail-blob path deterministic
    // (so a cold hydration is one GET with no LIST).
    final long version;
    final long mappingVersion;
    final long settingsVersion;
    final long aliasesVersion;

    // Alias data. AliasMetadata already stores its filter as CompressedXContent, so carrying it
    // verbatim is cheap and satisfies resolveWriteIndexRouting / resolveSearchRouting.
    final Map<String, AliasMetadata> aliases;

    // Shard placement, flattened. Replaces the IndexRoutingTable -> IndexShardRoutingTable ->
    // ShardRouting graph, whose derived collections, shufflers and attribute caches are all
    // recomputable (see S1 pass 3).
    final ShardPlacement[] shards;

    RoutingDescriptor(
        String name,
        String uuid,
        int routingNumShards,
        int routingFactor,
        int routingPartitionSize,
        int numberOfVirtualShards,
        int numberOfShards,
        int numberOfReplicas,
        int numberOfSearchOnlyReplicas,
        IndexMetadata.State state,
        Version creationVersion,
        ActiveShardCount waitForActiveShards,
        String defaultSearchPipelineId,
        boolean isSystem,
        boolean isAppendOnly,
        boolean isRemoteSnapshot,
        boolean isWarm,
        boolean isHidden,
        boolean bulkAdaptiveShardSelectionEnabled,
        boolean frozen,
        boolean routingRequired,
        long version,
        long mappingVersion,
        long settingsVersion,
        long aliasesVersion,
        Map<String, AliasMetadata> aliases,
        ShardPlacement[] shards
    ) {
        this.name = name;
        this.uuid = uuid;
        this.routingNumShards = routingNumShards;
        this.routingFactor = routingFactor;
        this.routingPartitionSize = routingPartitionSize;
        this.numberOfVirtualShards = numberOfVirtualShards;
        this.numberOfShards = numberOfShards;
        this.numberOfReplicas = numberOfReplicas;
        this.numberOfSearchOnlyReplicas = numberOfSearchOnlyReplicas;
        this.state = state;
        this.creationVersion = creationVersion;
        this.waitForActiveShards = waitForActiveShards;
        this.defaultSearchPipelineId = defaultSearchPipelineId;
        this.isSystem = isSystem;
        this.isAppendOnly = isAppendOnly;
        this.isRemoteSnapshot = isRemoteSnapshot;
        this.isWarm = isWarm;
        this.isHidden = isHidden;
        this.bulkAdaptiveShardSelectionEnabled = bulkAdaptiveShardSelectionEnabled;
        this.frozen = frozen;
        this.routingRequired = routingRequired;
        this.version = version;
        this.mappingVersion = mappingVersion;
        this.settingsVersion = settingsVersion;
        this.aliasesVersion = aliasesVersion;
        this.aliases = aliases;
        this.shards = shards;
    }

    /**
     * One shard copy's placement. Carries exactly what the audit found the coordinating path reads:
     * {@code primary}, {@code state}, {@code currentNodeId}, {@code allocationId}, plus
     * {@code searchOnly} (which {@code Preference.SEARCH_REPLICA} depends on and which the first
     * draft of this field set had missed) and {@code relocatingNodeId}.
     */
    static final class ShardPlacement {
        final int shardId;
        final String currentNodeId;
        final String relocatingNodeId;
        final String allocationId;
        final byte state;
        final boolean primary;
        final boolean searchOnly;

        ShardPlacement(
            int shardId,
            String currentNodeId,
            String relocatingNodeId,
            String allocationId,
            byte state,
            boolean primary,
            boolean searchOnly
        ) {
            this.shardId = shardId;
            this.currentNodeId = currentNodeId;
            this.relocatingNodeId = relocatingNodeId;
            this.allocationId = allocationId;
            this.state = state;
            this.primary = primary;
            this.searchOnly = searchOnly;
        }
    }
}
