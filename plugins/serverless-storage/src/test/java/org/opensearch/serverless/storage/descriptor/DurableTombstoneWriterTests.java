/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.DurableTombstones;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.mapper.UnknownFieldRefresh;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

/**
 * The writer {@link DescriptorGate} registers with {@link DurableTombstones} really defers a deletion.
 *
 * <h2>Why this exists</h2>
 *
 * {@code MetadataDeleteIndexService} has called {@code DurableTombstones.whenDurable} since the mechanism
 * landed, and nothing ever registered a {@code Writer}, so the call completed immediately and every gated
 * delete acknowledged with nothing durable behind it. Core's own {@code DurableTombstonesTests} covers the
 * seam's contract and passed throughout, because the seam was working exactly as specified: with no writer
 * installed, it is supposed to complete at once.
 *
 * <p>So the defect was not in the seam and not in its tests. It was that the plugin never filled it, which
 * only counting registrars finds. {@link DescriptorGateReachabilityTests} now asserts the registration
 * exists; this asserts the thing registered actually waits.
 *
 * <h2>Why the ordering is the assertion</h2>
 *
 * A tombstone that is merely submitted is worth nothing to the case it defends. The hazard is a node that
 * was partitioned during the delete rejoining with the shard's data still on disk: it reads durable storage
 * to decide whether the index exists, and a tombstone still in flight reads as "still there". For a gated
 * index there is no cluster state entry and no {@code IndexGraveyard} entry standing behind it, so the
 * tombstone is the only record that says no.
 */
public class DurableTombstoneWriterTests extends OpenSearchTestCase {

    @After
    public void alwaysUninstall() {
        DescriptorGate.uninstall();
    }

    /** A backend that records tombstone writes and completes them only when this test says so. */
    private static final class ControllableBackend implements DescriptorBackend, DescriptorPrefixBackend {

        private final List<IndexDescriptor> tombstones = new ArrayList<>();
        private final List<ActionListener<Void>> pending = new ArrayList<>();

        @Override
        public void putTombstoneAsync(IndexDescriptor tombstone, ActionListener<Void> whenDurable) {
            tombstones.add(tombstone);
            pending.add(whenDurable);
        }

        void completeAll() {
            List<ActionListener<Void>> toComplete = List.copyOf(pending);
            pending.clear();
            toComplete.forEach(l -> l.onResponse(null));
        }

        void failAll(Exception e) {
            List<ActionListener<Void>> toComplete = List.copyOf(pending);
            pending.clear();
            toComplete.forEach(l -> l.onFailure(e));
        }

        @Override
        public void invalidate(String name) {}

        @Override
        public IndexDescriptor get(String name) {
            return null;
        }

        @Override
        public boolean create(IndexDescriptor descriptor) {
            return true;
        }

        @Override
        public java.util.concurrent.CompletableFuture<Boolean> createAsync(IndexDescriptor descriptor) {
            return java.util.concurrent.CompletableFuture.completedFuture(true);
        }

        @Override
        public void put(IndexDescriptor descriptor) {}

        @Override
        public void putAsync(IndexDescriptor descriptor) {}

        @Override
        public void putTombstoneAsync(IndexDescriptor tombstone) {
            putTombstoneAsync(tombstone, ActionListener.wrap(() -> {}));
        }

        @Override
        public boolean available() {
            return true;
        }

        @Override
        public void warmAsync(java.util.Collection<String> names, ActionListener<Void> listener) {
            listener.onResponse(null);
        }

        @Override
        public org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers.PrefixExpansion expandPrefix(String prefix, int limit) {
            return org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers.PrefixExpansion.of(List.of());
        }
    }

    private ControllableBackend install() {
        ControllableBackend backend = new ControllableBackend();
        DescriptorGate.install(
            backend,
            backend,
            mock(MappingGenerationStore.Store.class),
            mock(org.opensearch.action.admin.cluster.stats.GatedMappingStatsAggregator.Aggregator.class),
            mock(UnknownFieldRefresh.Refresher.class),
            true
        );
        return backend;
    }

    private static IndexMetadata metadataFor(String name) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    public void testTheAcknowledgementWaitsForTheTombstoneToLand() {
        ControllableBackend backend = install();
        AtomicBoolean acknowledged = new AtomicBoolean();

        DurableTombstones.whenDurable(List.of(metadataFor("tenant-a")), ActionListener.wrap(ignored -> acknowledged.set(true), e -> {}));

        assertEquals("the writer must have been asked for a tombstone", 1, backend.tombstones.size());
        assertFalse(
            "the tombstone is written but not yet durable, so a delete must not be acknowledged. Before the "
                + "writer was registered this was true immediately, which is the bug.",
            acknowledged.get()
        );

        backend.completeAll();
        assertTrue("once the write lands the delete may be acknowledged", acknowledged.get());
    }

    /** What is written must be a tombstone, or the record says the index still exists. */
    public void testWhatIsWrittenIsATombstoneCarryingTheIdentity() {
        ControllableBackend backend = install();

        DurableTombstones.whenDurable(List.of(metadataFor("tenant-b")), ActionListener.wrap(() -> {}));

        IndexDescriptor written = backend.tombstones.get(0);
        assertEquals("tenant-b", written.name());
        assertFalse("a tombstone must not report the index as existing", written.exists());
        // The uuid and shard count survive deliberately: they identify the dangling data to reclaim, so a
        // tombstone carrying only the name would say an index is gone without saying what to delete.
        assertEquals("tenant-b-uuid", written.uuid());
        assertEquals(1, written.shardCount());
    }

    /**
     * A delete naming several indices is not durable until the last tombstone is, so one straggler has to
     * hold the acknowledgement.
     */
    public void testEveryTombstoneMustLandBeforeTheDeleteIsAcknowledged() {
        ControllableBackend backend = install();
        AtomicBoolean acknowledged = new AtomicBoolean();

        DurableTombstones.whenDurable(
            List.of(metadataFor("tenant-c"), metadataFor("tenant-d"), metadataFor("tenant-e")),
            ActionListener.wrap(ignored -> acknowledged.set(true), e -> {})
        );

        assertEquals(3, backend.tombstones.size());
        assertFalse(acknowledged.get());

        backend.completeAll();
        assertTrue(acknowledged.get());
    }

    /**
     * A failed tombstone fails the delete rather than reporting success, which is the opposite of the
     * scale-down bug this project found where a failed operation returned the unchanged state and read as
     * having worked.
     */
    public void testAFailedTombstoneFailsTheDelete() {
        ControllableBackend backend = install();
        AtomicReference<Exception> failure = new AtomicReference<>();

        DurableTombstones.whenDurable(List.of(metadataFor("tenant-f")), ActionListener.wrap(ignored -> {}, failure::set));

        assertNull(failure.get());
        backend.failAll(new IllegalStateException("the store is unreachable"));

        assertNotNull("a delete whose tombstone could not be written has not achieved what a delete promises", failure.get());
        assertEquals("the store is unreachable", failure.get().getMessage());
    }
}
