/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexCatalogRegistry;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.SupplierBackedIndexCatalog;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.cluster.routing.ComputedShardRouting;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.function.Predicate;

/**
 * C29. Whether the file cache capacity check can see a shard whose placement is computed.
 *
 * <p>{@code RestoreService} sums the sizes of already-restored remote snapshot shards to decide whether
 * another searchable snapshot restore fits in the file cache. It found them through the no-argument
 * {@code allShardsSatisfyingPredicate}, which takes its index list from the routing table's own key set,
 * so an index with no published entry contributed nothing to the total.
 *
 * <p><b>The direction of the error is the reason this was worth changing.</b> Every other instance of
 * this seam produced a visibly empty answer. A capacity check that misses shards produces a total that is
 * too small, which looks entirely normal, and a total that is too small admits a restore that overflows
 * the cache. It fails towards doing the damage rather than towards refusing.
 *
 * <p><b>How it is reachable, since restore itself publishes routing.</b> A freshly restored index is
 * always visible here, because {@code RestoreService} publishes its routing unconditionally. State
 * recovery does not: after a full cluster restart it rebuilds the routing table from metadata and skips
 * whatever the deployment's predicate calls unpublished, so a restored remote snapshot index matching
 * that predicate comes back with no published entry. Reachability is therefore a property of the
 * deployment's configuration rather than something ruled out by construction.
 *
 * <p>These tests exercise the accessors rather than a restore, so they establish that the seam behaves,
 * not that a particular deployment reaches it.
 */
public class ComputedRestoreCapacityTests extends OpenSearchTestCase {

    private static final String INDEX = "computed-snapshot";

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.register(null);
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
        IndexCatalogRegistry.register(null);
    }

    /** What the capacity check used to do: miss the shard entirely, and so undercount. */
    public void testThePublishedTableCannotSeeAComputedShard() {
        registerComputedPlacement();
        ClusterState state = stateWithoutRouting();

        long counted = state.routingTable().allShardsSatisfyingPredicate(primaries()).getShardRoutings().size();

        assertEquals("the published routing table cannot name a computed index, so the capacity total was short", 0L, counted);
    }

    /**
     * What it does now: find the shard, so the total reflects what is actually in the cache.
     *
     * <p>Through {@code ClusterState#allShards()}, the read the capacity check itself uses, so what this
     * proves is the production path end to end: the bridge on the state's routing table, the registry
     * behind it, and the metadata walk that names the unpublished index.
     */
    public void testTheResolverCountsAComputedShard() {
        registerComputedPlacement();
        ClusterState state = stateWithoutRouting();

        long counted = state.allShards().stream().filter(primaries()).count();

        assertEquals("a computed shard must count towards file cache capacity, or the check admits too much", 1L, counted);
    }

    /**
     * The control. With nothing registered in the authority the resolver-aware read must return exactly
     * what the published table returns, since this runs on every searchable snapshot restore in every
     * cluster. The bridge stays attached here deliberately: what production varies is whether the plugin
     * has registered anything, not whether the bridge exists.
     */
    public void testWithoutASupplierTheCountIsUnchanged() {
        ClusterState state = stateWithoutRouting();

        long viaResolver = state.allShards().stream().filter(primaries()).count();
        long viaTable = state.routingTable().allShardsSatisfyingPredicate(primaries()).getShardRoutings().size();

        assertEquals("an unconfigured cluster must count exactly what it counted before", viaTable, viaResolver);
    }

    // ---------------------------------------------------------------- helpers

    private static Predicate<ShardRouting> primaries() {
        return ShardRouting::primary;
    }

    private static void registerComputedPlacement() {
        AbsentIndexRoutingSuppliers.registerUnpublished(metadata -> true);
        AbsentIndexRoutingSuppliers.register((clusterState, metadata) -> computedEntry(metadata));
    }

    private static IndexRoutingTable computedEntry(IndexMetadata indexMetadata) {
        ShardId shard = new ShardId(indexMetadata.getIndex(), 0);
        return IndexRoutingTable.builder(indexMetadata.getIndex())
            .addIndexShard(
                new IndexShardRoutingTable.Builder(shard).addShard(
                    ComputedShardRouting.started(shard, "node-1", RecoverySource.EmptyStoreRecoverySource.INSTANCE)
                ).build()
            )
            .build();
    }

    private static ClusterState stateWithoutRouting() {
        IndexMetadata metadata = IndexMetadata.builder(INDEX)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, INDEX + "-uuid-0000000000")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        // The node-scoped catalog a real node gets from ClusterPlugin#getIndexCatalog(), so
        // ClusterState#allShards() reads through it exactly as it does on a production state. Cleared in
        // this class's @After.
        IndexCatalogRegistry.register(new SupplierBackedIndexCatalog());
        RoutingTable routingTable = RoutingTable.builder().build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(metadata, false).build())
            .routingTable(routingTable)
            .build();
    }
}
