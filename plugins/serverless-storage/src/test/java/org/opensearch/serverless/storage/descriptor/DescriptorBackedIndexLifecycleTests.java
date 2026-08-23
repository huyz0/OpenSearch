/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.Version;
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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * {@link DescriptorBackedIndexLifecycle} -- the removal operation core hands a deleted index to -- really
 * defers the deletion, and prunes the stored mapping only after it has.
 *
 * <h2>Why this exists</h2>
 *
 * {@code MetadataDeleteIndexService} called {@code DurableTombstones.whenDurable} from the moment that
 * mechanism landed, and nothing ever registered a {@code Writer}, so the call completed immediately and
 * every gated delete acknowledged with nothing durable behind it. Core's own {@code DurableTombstonesTests}
 * covered the seam's contract and passed throughout, because the seam was working exactly as specified:
 * with no writer installed, it is supposed to complete at once.
 *
 * <p>So the defect was not in the seam and not in its tests. It was that the plugin never filled it, which
 * only counting registrars finds. That seam is gone now -- the removal is one operation supplied through
 * {@code ClusterPlugin#getClaimedIndexLifecycle()} -- but the lesson it taught is the reason this suite
 * asserts against the implementation rather than against the interface's own contract.
 *
 * <h2>Why the ordering is the assertion</h2>
 *
 * A tombstone that is merely submitted is worth nothing to the case it defends. The hazard is a node that
 * was partitioned during the delete rejoining with the shard's data still on disk: it reads durable storage
 * to decide whether the index exists, and a tombstone still in flight reads as "still there". For a gated
 * index there is no cluster state entry and no {@code IndexGraveyard} entry standing behind it, so the
 * tombstone is the only record that says no.
 *
 * <p>The prune's ordering is the second assertion, and it is a latency property rather than a safety one:
 * a request naming five hundred indices must not make the client wait for five hundred blocking round trips
 * whose result is then discarded. It used to be enforced by core writing the two calls in the right order;
 * now it is this class's to keep, so it is this class's to test.
 */
public class DescriptorBackedIndexLifecycleTests extends OpenSearchTestCase {

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

    private MappingGenerationStore.Store mappingStore;

    private ControllableBackend install() {
        ControllableBackend backend = new ControllableBackend();
        mappingStore = mock(MappingGenerationStore.Store.class);
        DescriptorGate.install(
            backend,
            backend,
            mappingStore,
            mock(org.opensearch.action.admin.cluster.stats.GatedMappingStatsAggregator.Aggregator.class),
            mock(UnknownFieldRefresh.Refresher.class),
            true
        );
        return backend;
    }

    /** What core does with the stage: acknowledge on completion, fail the request on failure. */
    private static void acknowledgeOn(CompletionStage<Void> removal, AtomicBoolean acknowledged, AtomicReference<Throwable> failure) {
        removal.whenComplete((ignored, thrown) -> {
            if (thrown != null) {
                failure.set(thrown);
            } else {
                acknowledged.set(true);
            }
        });
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
        AtomicReference<Throwable> failure = new AtomicReference<>();

        acknowledgeOn(
            new DescriptorBackedIndexLifecycle().removeIndices(List.of(metadataFor("serverless_tenant-a"))),
            acknowledged,
            failure
        );

        assertEquals("the removal must have been asked for a tombstone", 1, backend.tombstones.size());
        assertFalse(
            "the tombstone is written but not yet durable, so a delete must not be acknowledged. Before the "
                + "writer was registered this was true immediately, which is the bug.",
            acknowledged.get()
        );

        backend.completeAll();
        assertTrue("once the write lands the delete may be acknowledged", acknowledged.get());
        assertNull(failure.get());
    }

    /** What is written must be a tombstone, or the record says the index still exists. */
    public void testWhatIsWrittenIsATombstoneCarryingTheIdentity() {
        ControllableBackend backend = install();

        new DescriptorBackedIndexLifecycle().removeIndices(List.of(metadataFor("serverless_tenant-b")));

        IndexDescriptor written = backend.tombstones.get(0);
        assertEquals("serverless_tenant-b", written.name());
        assertFalse("a tombstone must not report the index as existing", written.exists());
        // The uuid and shard count survive deliberately: they identify the dangling data to reclaim, so a
        // tombstone carrying only the name would say an index is gone without saying what to delete.
        assertEquals("serverless_tenant-b-uuid", written.uuid());
        assertEquals(1, written.shardCount());
    }

    /**
     * A delete naming several indices is not durable until the last tombstone is, so one straggler has to
     * hold the acknowledgement.
     */
    public void testEveryTombstoneMustLandBeforeTheDeleteIsAcknowledged() {
        ControllableBackend backend = install();
        AtomicBoolean acknowledged = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        acknowledgeOn(
            new DescriptorBackedIndexLifecycle().removeIndices(
                List.of(metadataFor("serverless_tenant-c"), metadataFor("serverless_tenant-d"), metadataFor("serverless_tenant-e"))
            ),
            acknowledged,
            failure
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
        AtomicBoolean acknowledged = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        acknowledgeOn(
            new DescriptorBackedIndexLifecycle().removeIndices(List.of(metadataFor("serverless_tenant-f"))),
            acknowledged,
            failure
        );

        assertNull(failure.get());
        backend.failAll(new IllegalStateException("the store is unreachable"));

        assertFalse("a delete whose tombstone could not be written must not be acknowledged", acknowledged.get());
        assertNotNull("a delete whose tombstone could not be written has not achieved what a delete promises", failure.get());
        assertEquals("the store is unreachable", failure.get().getMessage());
    }

    /**
     * A failed removal must not prune the mapping either. The tombstone is what makes the deletion real, so
     * removing the mapping without it would leave an index that still exists and whose declared fields are
     * gone -- a silent loss, and the one ordering the prune's javadoc calls the safety argument.
     */
    public void testAFailedTombstoneDoesNotPruneTheMapping() {
        ControllableBackend backend = install();
        AtomicBoolean acknowledged = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        acknowledgeOn(
            new DescriptorBackedIndexLifecycle().removeIndices(List.of(metadataFor("serverless_tenant-g"))),
            acknowledged,
            failure
        );
        backend.failAll(new IllegalStateException("the store is unreachable"));

        verify(mappingStore, never()).delete("serverless_tenant-g-uuid");
    }

    /**
     * The prune happens, and happens after the acknowledgement rather than in front of it.
     *
     * <p>Both halves matter and each one was a defect on its own. Nothing pruned at all until a delete-time
     * prune was added, so the mapping store grew with every index that had ever existed. And pruning first
     * would make a client naming five hundred indices wait out five hundred blocking round trips -- for
     * every index, including those that never declared a mapping, since removing an absent document still
     * costs one -- to hear about a deletion that had already happened.
     */
    public void testTheMappingIsPrunedAfterTheAcknowledgementAndNotBefore() {
        ControllableBackend backend = install();
        AtomicBoolean acknowledged = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean prunedBeforeAcknowledgement = new AtomicBoolean();
        org.mockito.Mockito.doAnswer(invocation -> {
            prunedBeforeAcknowledgement.set(acknowledged.get() == false);
            return null;
        }).when(mappingStore).delete("serverless_tenant-h-uuid");

        acknowledgeOn(
            new DescriptorBackedIndexLifecycle().removeIndices(List.of(metadataFor("serverless_tenant-h"))),
            acknowledged,
            failure
        );
        backend.completeAll();

        assertTrue("the delete must be acknowledged", acknowledged.get());
        verify(mappingStore).delete("serverless_tenant-h-uuid");
        assertFalse(
            "the prune is a blocking round trip and must not stand between the durable tombstone and the "
                + "client hearing about a deletion that has already happened",
            prunedBeforeAcknowledgement.get()
        );
    }

    /**
     * A prune that fails does not fail the delete. The index is gone either way, and reporting the delete as
     * failed would invite a retry of something that already happened; a stranded document is the lesser
     * outcome, and it is logged rather than swallowed.
     */
    public void testAFailedPruneDoesNotFailTheDelete() {
        ControllableBackend backend = install();
        AtomicBoolean acknowledged = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        org.mockito.Mockito.doThrow(new IllegalStateException("the mapping index is unavailable"))
            .when(mappingStore)
            .delete("serverless_tenant-i-uuid");

        acknowledgeOn(
            new DescriptorBackedIndexLifecycle().removeIndices(List.of(metadataFor("serverless_tenant-i"))),
            acknowledged,
            failure
        );
        backend.completeAll();

        assertTrue("a stranded mapping document must not turn a completed deletion into a failed one", acknowledged.get());
        assertNull(failure.get());
    }

    /**
     * With the gate uninstalled -- a stock node, or one with descriptor gating switched off -- the operation
     * completes at once and writes nothing, which is exactly what an unregistered {@code
     * DurableTombstones.Writer} used to answer. The plugin returns this object unconditionally, so this is
     * the case an ordinary cluster is actually in.
     */
    public void testWithNothingInstalledTheRemovalCompletesImmediately() {
        DescriptorGate.uninstall();
        AtomicBoolean acknowledged = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        acknowledgeOn(new DescriptorBackedIndexLifecycle().removeIndices(List.of(metadataFor("ordinary"))), acknowledged, failure);

        assertTrue("an uninstalled gate must not defer the acknowledgement", acknowledged.get());
        assertNull(failure.get());
    }
}
