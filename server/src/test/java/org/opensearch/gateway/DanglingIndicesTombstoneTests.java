/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * H4b. Whether a node adopting dangling shard data consults the tombstone.
 *
 * <p>H4a records tombstones and, on its own, prevents nothing. This is the half that does. A node
 * partitioned during a delete rejoins holding shard data for an index cluster state no longer mentions,
 * and without a durable no it imports that data back. {@code IndexGraveyard} is what stops that today,
 * and it is bounded: it purges its oldest tombstones once it exceeds its configured size, so an index
 * deleted long enough ago is forgotten by it. Under Area H there is no per-index cluster state entry at
 * all, so the descriptor is the only record.
 *
 * <p>The case these have to get right in both directions is that <em>absence</em> of a descriptor must
 * not read as deletion. A supplier that is not installed answers null, and treating that as deleted would
 * discard live data on every cluster where the mechanism is off.
 */
public class DanglingIndicesTombstoneTests extends OpenSearchTestCase {

    @After
    public void clearSupplier() {
        AbsentIndexDescriptorSuppliers.register(null);
    }

    /** The load-bearing one: a tombstoned index must not be treated as dangling and re-imported. */
    public void testATombstonedIndexIsNotAdopted() throws Exception {
        Index index = new Index("deleted-idx", "deleted-uuid");
        AbsentIndexDescriptorSuppliers.register(name -> descriptor(index, IndexDescriptor.State.DELETED));

        Map<Index, IndexMetadata> found = danglingState(index).findNewDanglingIndices(Map.of(), emptyMetadata());

        assertTrue("an index whose descriptor is tombstoned must not be adopted: " + found.keySet(), found.isEmpty());
    }

    /**
     * The control that stops this discarding live data. With no supplier installed nothing answers, and
     * an index on disk that cluster state has simply not caught up with must still be adopted.
     */
    public void testWithoutASupplierAnIndexIsStillAdopted() throws Exception {
        Index index = new Index("orphan-idx", "orphan-uuid");

        Map<Index, IndexMetadata> found = danglingState(index).findNewDanglingIndices(Map.of(), emptyMetadata());

        assertEquals("with nothing installed, dangling detection must behave exactly as before", 1, found.size());
    }

    /** A live descriptor is not a tombstone, and must not suppress adoption either. */
    public void testALiveDescriptorDoesNotSuppressAdoption() throws Exception {
        Index index = new Index("live-idx", "live-uuid");
        AbsentIndexDescriptorSuppliers.register(name -> descriptor(index, IndexDescriptor.State.OPEN));

        Map<Index, IndexMetadata> found = danglingState(index).findNewDanglingIndices(Map.of(), emptyMetadata());

        assertEquals("an index whose descriptor says it exists must still be adopted", 1, found.size());
    }

    /**
     * A tombstone for a different incarnation must not suppress this one. Index names are reused, and a
     * descriptor matching by name alone would discard the data of a new index that happens to share the
     * name of a deleted one.
     */
    public void testATombstoneForADifferentUuidDoesNotSuppress() throws Exception {
        Index onDisk = new Index("reused-name", "new-uuid");
        Index deletedEarlier = new Index("reused-name", "old-uuid");
        AbsentIndexDescriptorSuppliers.register(name -> descriptor(deletedEarlier, IndexDescriptor.State.DELETED));

        Map<Index, IndexMetadata> found = danglingState(onDisk).findNewDanglingIndices(Map.of(), emptyMetadata());

        assertEquals("a tombstone for an older incarnation must not discard the current one", 1, found.size());
    }

    // ---------------------------------------------------------------- helpers

    private static IndexDescriptor descriptor(Index index, IndexDescriptor.State state) {
        return new IndexDescriptor(
            index.getName(),
            index.getUUID(),
            1,
            0,
            true,
            state,
            List.of(),
            Version.CURRENT.id,
            false,
            false,
            false,
            false,
            0L
        );
    }

    private static Metadata emptyMetadata() {
        return Metadata.builder().build();
    }

    /** A dangling state whose on-disk scan finds exactly the given index. */
    private static DanglingIndicesState danglingState(Index index) throws Exception {
        MetaStateService metaStateService = mock(MetaStateService.class);
        org.mockito.Mockito.when(metaStateService.loadIndicesStates(org.mockito.ArgumentMatchers.any()))
            .thenReturn(List.of(indexMetadata(index)));
        // NodeEnvironment is final and not reachable from findNewDanglingIndices, so it is null rather
        // than mocked. Passing a mock of a final class fails; passing null documents that this path does
        // not touch it.
        org.opensearch.cluster.service.ClusterService clusterService = mock(org.opensearch.cluster.service.ClusterService.class);
        org.mockito.Mockito.when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        return new DanglingIndicesState(null, metaStateService, null, clusterService);
    }

    private static IndexMetadata indexMetadata(Index index) {
        return IndexMetadata.builder(index.getName())
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
