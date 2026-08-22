/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.clusterstate;

import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;

import java.util.Map;
import java.util.Set;

/**
 * A benchmarks-module-only class, not a core change: an attempt at the smallest representation
 * that could still plausibly participate in {@code ClusterState}'s diff protocol and answer
 * {@code IndexAbstraction} queries correctly, as opposed to {@link TenantIndexRetentionBenchmark
 * .CompactTenantStub}'s bare-identity floor.
 *
 * <p>Reading {@code IndexMetadata.IndexMetadataDiff} (the object actually sent over the wire and
 * applied on every node) shows the diff protocol keeps several fields whole rather than shrinking
 * them further: the full {@link Settings} object, the full alias map (as {@link AliasMetadata},
 * needed by both {@code IndexAbstraction.Alias} and the diff itself), the four per-field version
 * longs, and {@code state}. Those are carried here unchanged from what a real {@link IndexMetadata}
 * holds -- they are not deferrable without also changing the diff wire format, which is out of
 * scope for a plugin-side or even a load-bearing "compact stub" idea.
 *
 * <p>What genuinely does NOT appear in {@code IndexMetadataDiff} and is safe to omit or defer:
 * the four {@code DiscoveryNodeFilters} objects (derived from settings on demand, only needed
 * during allocation, not diffing or lookup), {@code mappings}/{@code customData}/{@code
 * rolloverInfos} maps when empty (real for a tenant with actual mappings, but this spike's "full"
 * baseline never sets any either, so this class matches it), and {@code context}/{@code
 * ingestionStatus}/{@code splitShardsMetadata} when unused (null in both this class and the
 * baseline). Various derived ints ({@code routingNumShards}, shard-limit settings,
 * {@code waitForActiveShards}) are recomputable from {@code settings} on demand and are also
 * omitted here.
 */
final class RealisticCompactTenantIndex {

    final String name;
    final String uuid;
    final long version;
    final long mappingVersion;
    final long settingsVersion;
    final long aliasesVersion;
    final IndexMetadata.State state;
    final Settings settings;
    final Map<String, AliasMetadata> aliases;
    final int numberOfShards;
    final int numberOfReplicas;
    final Map<Integer, Long> primaryTermsMap;
    final Map<Integer, Set<String>> inSyncAllocationIds;
    final boolean isSystem;

    RealisticCompactTenantIndex(
        String name,
        String uuid,
        Settings settings,
        Map<String, AliasMetadata> aliases,
        int numberOfShards,
        int numberOfReplicas,
        Map<Integer, Long> primaryTermsMap,
        Map<Integer, Set<String>> inSyncAllocationIds
    ) {
        this.name = name;
        this.uuid = uuid;
        this.version = 1L;
        this.mappingVersion = 1L;
        this.settingsVersion = 1L;
        this.aliasesVersion = 1L;
        this.state = IndexMetadata.State.OPEN;
        this.settings = settings;
        this.aliases = aliases;
        this.numberOfShards = numberOfShards;
        this.numberOfReplicas = numberOfReplicas;
        this.primaryTermsMap = primaryTermsMap;
        this.inSyncAllocationIds = inSyncAllocationIds;
        this.isSystem = false;
    }

    /**
     * Compact substitute for the {@code RoutingTable -> IndexRoutingTable -> IndexShardRoutingTable
     * -> ShardRouting} object graph: just enough per-shard placement data to answer "who holds
     * this shard," not the full core routing object chain.
     */
    static final class CompactShardRouting {
        final int shardId;
        final String currentNodeId;
        final boolean primary;

        CompactShardRouting(int shardId, String currentNodeId, boolean primary) {
            this.shardId = shardId;
            this.currentNodeId = currentNodeId;
            this.primary = primary;
        }
    }
}
