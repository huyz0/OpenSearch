/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.delete;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.ClaimedIndexLifecycle;
import org.opensearch.cluster.metadata.IndexCatalog;
import org.opensearch.cluster.metadata.IndexCatalogRegistry;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataDeleteIndexService;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The acknowledgement contract {@link MetadataDeleteIndexService} owes a {@link ClaimedIndexLifecycle}:
 * nothing is acknowledged until the removal is durable, and a removal that fails fails the request.
 *
 * <h2>Why this is asserted through the service rather than through the seam</h2>
 *
 * The property that matters is not "the interface can express a deferral" -- it obviously can, it returns a
 * {@link CompletionStage}. It is that the one core caller actually waits on it and actually propagates its
 * failure. The predecessor seam, {@code DurableTombstones}, had a thorough suite asserting its own contract
 * and passed for the whole time nothing was registered with it, because the seam was behaving exactly as
 * specified while the delete it guarded acknowledged with nothing durable behind it. Testing the caller is
 * what that history says to do.
 *
 * <h2>Why the all-gated road</h2>
 *
 * A request naming only claimed indices takes {@code deleteGatedIndices}, which reaches the plane without
 * submitting a cluster state update task at all -- so this can drive the real service against a mocked
 * {@link ClusterService} and still exercise the real deferral. The mixed and ordinary roads reach the plane
 * through the same private helper, one hop further on, behind an acked cluster state task; {@code
 * ClusterStateChanges} covers those end to end with {@link ClaimedIndexLifecycle#NOOP} installed, which is
 * the stock-node case.
 *
 * <p>In this package rather than beside {@code MetadataDeleteIndexServiceTests} because {@link
 * DeleteIndexClusterStateUpdateRequest}'s constructor is package-private, and a request is what the entry
 * point under test takes.
 */
public class MetadataDeleteIndexServiceLifecycleTests extends OpenSearchTestCase {

    private static final String NAME = "claimed-index";
    private static final String UUID = "claimed-index-uuid";

    @After
    public void clearCatalog() {
        IndexCatalogRegistry.register(null);
    }

    /** A plane that hands its stage back to the test, so the test decides when a removal becomes durable. */
    private static final class ControllablePlane implements ClaimedIndexLifecycle {
        private final CompletableFuture<Void> durable = new CompletableFuture<>();
        private final AtomicReference<Collection<IndexMetadata>> seen = new AtomicReference<>();

        @Override
        public CompletionStage<Void> removeIndices(Collection<IndexMetadata> indices) {
            seen.set(indices);
            return durable;
        }
    }

    private static IndexMetadata metadata() {
        return IndexMetadata.builder(NAME)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, UUID)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    /**
     * A catalog that resolves the one claimed index, which is what makes the service treat it as claimed:
     * absent from the published map, present when asked by name. Exactly the distinguishing signal {@code
     * Metadata#indexOrResolved} exists to keep separate from a plain existence check.
     */
    private static void registerCatalogForTheClaimedIndex() {
        IndexCatalogRegistry.register(new IndexCatalog() {
            @Override
            public IndexMetadata resolveMetadata(Metadata metadata, String indexName) {
                return NAME.equals(indexName) ? metadata() : null;
            }

            @Override
            public boolean isActive() {
                return true;
            }
        });
    }

    private static MetadataDeleteIndexService serviceWith(ClaimedIndexLifecycle plane) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.builder(ClusterName.DEFAULT).build());
        return new MetadataDeleteIndexService(Settings.EMPTY, clusterService, mock(AllocationService.class), plane);
    }

    private static DeleteIndexClusterStateUpdateRequest requestForTheClaimedIndex() {
        return new DeleteIndexClusterStateUpdateRequest().indices(new Index[] { new Index(NAME, UUID) });
    }

    /**
     * The ordering the whole seam exists for. A delete acknowledged before its record is durable can be
     * resurrected: a node partitioned during the delete rejoins holding the shard's data, and with no
     * durable no behind the deletion it imports that data back.
     */
    public void testTheAcknowledgementWaitsForTheRemovalToBeDurable() {
        registerCatalogForTheClaimedIndex();
        ControllablePlane plane = new ControllablePlane();
        AtomicBoolean acknowledged = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();

        serviceWith(plane).deleteIndices(
            requestForTheClaimedIndex(),
            ActionListener.wrap(response -> acknowledged.set(response.isAcknowledged()), failure::set)
        );

        assertNotNull("the plane must have been asked to remove the index", plane.seen.get());
        assertFalse(
            "the delete must not be acknowledged while the removal is still in flight, or a crash between "
                + "the two loses the only record that the index was deleted",
            acknowledged.get()
        );

        plane.durable.complete(null);

        assertTrue("and must be acknowledged once the removal is durable", acknowledged.get());
        assertNull(failure.get());
    }

    /**
     * A removal that fails fails the delete rather than being swallowed. Reporting success would leave the
     * caller believing a durable no exists when it does not, which is the precise condition that lets a
     * partitioned node resurrect the index later.
     */
    public void testAFailedRemovalFailsTheRequest() {
        registerCatalogForTheClaimedIndex();
        ControllablePlane plane = new ControllablePlane();
        AtomicBoolean acknowledged = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();

        serviceWith(plane).deleteIndices(
            requestForTheClaimedIndex(),
            ActionListener.wrap(response -> acknowledged.set(true), failure::set)
        );
        plane.durable.completeExceptionally(new IllegalStateException("the descriptor store is unreachable"));

        assertFalse("a delete whose record could not be written must not report success", acknowledged.get());
        assertNotNull("it must report the failure instead", failure.get());
        assertEquals(
            "the client must see the cause rather than the plumbing wrapped around it",
            "the descriptor store is unreachable",
            failure.get().getMessage()
        );
    }

    /** A plane that throws on the way in has removed nothing, so that is a failed delete too. */
    public void testAThrowingPlaneFailsTheRequest() {
        registerCatalogForTheClaimedIndex();
        AtomicReference<Exception> failure = new AtomicReference<>();

        serviceWith(new ClaimedIndexLifecycle() {
            @Override
            public CompletionStage<Void> removeIndices(Collection<IndexMetadata> indices) {
                throw new IllegalStateException("the plane exploded");
            }
        }).deleteIndices(requestForTheClaimedIndex(), ActionListener.wrap(response -> {}, failure::set));

        assertNotNull("a throwing plane must fail the delete rather than acknowledge it", failure.get());
        assertEquals("the plane exploded", failure.get().getMessage());
    }

    /**
     * With no plugin supplying a plane -- every ordinary node, which {@code ClusterModule} gives {@link
     * ClaimedIndexLifecycle#NOOP} -- the acknowledgement is not deferred at all, exactly as it was not
     * before either of the seams this replaced existed.
     */
    public void testWithNoPlaneRegisteredTheAcknowledgementIsNotDeferred() {
        registerCatalogForTheClaimedIndex();
        AtomicBoolean acknowledged = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();

        serviceWith(ClaimedIndexLifecycle.NOOP).deleteIndices(
            requestForTheClaimedIndex(),
            ActionListener.wrap(response -> acknowledged.set(response.isAcknowledged()), failure::set)
        );

        assertTrue("an unregistered plane must not defer the acknowledgement", acknowledged.get());
        assertNull(failure.get());
    }

    /** The metadata of what was deleted reaches the plane, since a removal record is derived from it. */
    public void testThePlaneReceivesTheMetadataOfWhatWasDeleted() {
        registerCatalogForTheClaimedIndex();
        ControllablePlane plane = new ControllablePlane();

        serviceWith(plane).deleteIndices(
            requestForTheClaimedIndex(),
            ActionListener.wrap(response -> {}, e -> { throw new AssertionError(e); })
        );

        List<IndexMetadata> seen = List.copyOf(plane.seen.get());
        assertEquals(1, seen.size());
        assertEquals(NAME, seen.get(0).getIndex().getName());
        assertEquals("a removal record identifies the dangling data to reclaim, which needs the uuid", UUID, seen.get(0).getIndexUUID());
    }
}
