/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;

/**
 * H4c. What a mapping update does to an index that has no cluster state entry.
 *
 * <p>This started as an optimisation and the probe turned it into a prerequisite. H3 and H5 let an index
 * exist with no metadata entry, and every mapping update path resolves its target through
 * {@code Metadata#getIndexSafe}, which throws when the index is not in the map. So such an index cannot
 * accept a document carrying a new field: dynamic mapping fails at the first unknown field rather than
 * degrading.
 *
 * <p>That inverts the priority. Moving mappings to a compare-and-swap on an object-store generation was
 * filed as a way to remove a global serialisation point from the write path, which is true and secondary.
 * The reason to do it is that without it H3 and H5 are not usable for any index whose mapping is not
 * fully known at creation, which is most of them.
 *
 * <p><b>What this class asserts, after the protocol left core.</b> The compare-and-swap, the field
 * extraction and the tombstone refusal are now {@link ClaimedIndexLifecycle#putMapping}'s implementer's, and
 * they are asserted where they live ({@code DescriptorBackedIndexLifecycleTests}). What is left here is the
 * decision core still makes and the two ways it can go wrong: a request no cluster state entry can serve is
 * handed over <em>without</em> a cluster state task, and one nothing holds fails with the {@link
 * IndexNotFoundException} the ordinary path always gave.
 *
 * <p>The store-level assertions deliberately did not stay behind as mocks. An earlier version of this class
 * called the store directly and passed with the wiring in {@code MetadataMappingService} disabled entirely,
 * which is the "correct and unreachable" failure this project has shipped twice; asserting the handoff is
 * the version of that assertion which cannot pass with the handoff removed.
 */
public class GatedIndexMappingUpdateTests extends OpenSearchTestCase {

    private final TestIndexCreationStrategy strategy = new TestIndexCreationStrategy();

    private ThreadPool threadPool;

    @Before
    public void startThreadPool() {
        // A real pool rather than a mock, because the point of T19 is which thread the work runs on: the
        // handoff dispatches to GENERIC, and a mock would let that dispatch be removed silently.
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void stopThreadPool() {
        ThreadPool.terminate(threadPool, 10, java.util.concurrent.TimeUnit.SECONDS);
    }

    @Before
    public void registerStrategy() {
        // Phase D3 of core-pluggability-refactor-plan.md: a test-local IndexCreationStrategy, not
        // DescriptorOnlyCreation (relocated into plugins/serverless-storage) -- see TestIndexCreationStrategy's
        // own javadoc.
        IndexCreationStrategyRegistry.register(strategy);
    }

    @After
    public void clearRegistrations() {
        strategy.deactivate();
        IndexDescriptorPublisher.register(null);
        IndexDescriptorPublisher.registerCreator(null);
        IndexCreationStrategyRegistry.register(null);
    }

    /** Records what core handed over, which is the whole of what core is responsible for here. */
    private static final class RecordingLifecycle implements ClaimedIndexLifecycle {
        private final AtomicReference<List<Index>> indices = new AtomicReference<>();
        private final AtomicReference<String> source = new AtomicReference<>();
        private final AtomicReference<Thread> thread = new AtomicReference<>();
        private volatile Exception failWith;

        @Override
        public CompletionStage<Void> removeIndices(Collection<IndexMetadata> removed) {
            throw new AssertionError("nothing on this path removes an index");
        }

        @Override
        public CompletionStage<Void> putMapping(Collection<Index> targets, String mappingSource) {
            indices.set(new ArrayList<>(targets));
            source.set(mappingSource);
            thread.set(Thread.currentThread());
            return failWith == null ? CompletableFuture.completedFuture(null) : CompletableFuture.failedFuture(failWith);
        }
    }

    private MetadataMappingService serviceOver(ClusterState state, ClaimedIndexLifecycle lifecycle) {
        ClusterService clusterService = org.mockito.Mockito.mock(ClusterService.class);
        org.mockito.Mockito.when(clusterService.state()).thenReturn(state);
        return new MetadataMappingService(
            clusterService,
            org.mockito.Mockito.mock(org.opensearch.indices.IndicesService.class),
            threadPool,
            lifecycle
        );
    }

    private static PutMappingClusterStateUpdateRequest request(String source) {
        PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(source);
        request.indices(new Index[] { new Index("gated-idx", "gated-uuid") });
        return request;
    }

    /**
     * The gap, stated as a passing test. A gated index is absent from metadata, and resolving it for a
     * mapping update throws rather than finding it.
     */
    public void testAGatedIndexCannotBeResolvedForAMappingUpdate() {
        IndexDescriptorPublisher.register(descriptor -> {});
        // T18 split recording from creating: a gated index is created by the creator, and its
        // future is what the acknowledgement waits on. Registering only a publisher would leave
        // createGated returning null, which creation now treats as "no record anywhere".
        IndexDescriptorPublisher.registerCreator(descriptor -> CompletableFuture.completedFuture(Boolean.TRUE));
        strategy.activate(indexMetadata -> true);

        ClusterState state = MetadataCreateIndexService.clusterStateCreateIndex(
            ClusterState.builder(ClusterName.DEFAULT).build(),
            Set.of(),
            index("gated-idx"),
            (current, reason) -> current,
            null,
            write -> {}
        );

        assertFalse("the premise: a gated index has no metadata entry", state.metadata().hasIndex("gated-idx"));

        // Every mapping update path resolves its target this way before merging anything.
        expectThrows(IndexNotFoundException.class, () -> state.getMetadata().getIndexSafe(new Index("gated-idx", "gated-idx-uuid")));
    }

    /**
     * The control. An ordinary index resolves, so the failure above is about the gate rather than about
     * the resolution mechanism being broken generally.
     */
    public void testAnOrdinaryIndexResolvesForAMappingUpdate() {
        ClusterState state = MetadataCreateIndexService.clusterStateCreateIndex(
            ClusterState.builder(ClusterName.DEFAULT).build(),
            Set.of(),
            index("ordinary-idx"),
            (current, reason) -> current,
            null,
            write -> {}
        );

        IndexMetadata resolved = state.getMetadata().getIndexSafe(state.metadata().index("ordinary-idx").getIndex());

        assertEquals("an ordinary index must still resolve for a mapping update", "ordinary-idx", resolved.getIndex().getName());
    }

    /**
     * H7a/T19. The wiring, exercised through the entry point a real mapping update goes through.
     *
     * <p>The assertion is not only that the request reached the plane. It is that no cluster state task was
     * ever submitted -- which is what "off the cluster manager thread" means, stated as something a test can
     * observe rather than as a claim in a comment. The previous claim was in a comment, and it was wrong:
     * this ran inside the put-mapping executor, two blocking round trips deep, on the cluster manager's own
     * update thread.
     */
    public void testAMappingChangeIsHandedOverWithoutTouchingTheClusterManager() {
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        ClusterService clusterService = org.mockito.Mockito.mock(ClusterService.class);
        // A cluster state with no entry for the index, which is what gated means.
        org.mockito.Mockito.when(clusterService.state()).thenReturn(ClusterState.builder(ClusterName.DEFAULT).build());
        MetadataMappingService service = new MetadataMappingService(
            clusterService,
            org.mockito.Mockito.mock(org.opensearch.indices.IndicesService.class),
            threadPool,
            lifecycle
        );

        PlainActionFuture<ClusterStateUpdateResponse> future = PlainActionFuture.newFuture();
        service.putMapping(request("{\"properties\":{\"age\":{\"type\":\"long\"}}}"), future);
        assertTrue("a handed-over put-mapping must be acknowledged once the plane says it landed", future.actionGet().isAcknowledged());

        assertEquals("every index the request named must be handed over", 1, lifecycle.indices.get().size());
        assertEquals("gated-uuid", lifecycle.indices.get().get(0).getUUID());
        assertEquals(
            "the source goes over verbatim, because what a plane can represent is the plane's question",
            "{\"properties\":{\"age\":{\"type\":\"long\"}}}",
            lifecycle.source.get()
        );
        assertThat(
            "and it runs on GENERIC, not on the calling transport thread and not on the cluster manager's",
            lifecycle.thread.get().getName(),
            containsString("[generic]")
        );
        org.mockito.Mockito.verify(clusterService, org.mockito.Mockito.never())
            .submitStateUpdateTask(
                org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any()
            );
    }

    /**
     * A plane that could not apply the change must fail the request rather than acknowledge it.
     *
     * <p>There is no cluster state entry standing behind this write that would still carry the change, so an
     * acknowledgement here is the silent success this area has produced repeatedly -- and the failure has to
     * arrive unwrapped, or a client sees {@code CompletionException} instead of what actually went wrong.
     */
    public void testAFailedHandoverFailsTheRequestWithItsOwnCause() {
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        lifecycle.failWith = new IllegalArgumentException("this mapping cannot be carried");
        MetadataMappingService service = serviceOver(ClusterState.builder(ClusterName.DEFAULT).build(), lifecycle);

        PlainActionFuture<ClusterStateUpdateResponse> future = PlainActionFuture.newFuture();
        service.putMapping(request("{\"properties\":{\"profile\":\"keyword\"}}"), future);

        IllegalArgumentException failure = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(failure.getMessage(), containsString("cannot be carried"));
    }

    /**
     * With no plugin at all, a put-mapping for an index cluster state does not have is not found.
     *
     * <p>This is what replaces {@code CoreIsInertWithoutAPluginTests}' assertion that the mapping store was
     * unregistered, and it is the stronger statement: not "the seam is off" but "a stock node answers
     * exactly as it always did". The old code reached that answer by submitting a cluster state task and
     * letting {@code getIndexSafe} throw inside it; this reaches it without the task.
     */
    public void testWithNoPluginAPutMappingForAnAbsentIndexIsNotFound() {
        MetadataMappingService service = serviceOver(ClusterState.builder(ClusterName.DEFAULT).build(), ClaimedIndexLifecycle.NOOP);

        PlainActionFuture<ClusterStateUpdateResponse> future = PlainActionFuture.newFuture();
        service.putMapping(request("{\"properties\":{\"age\":{\"type\":\"long\"}}}"), future);

        IndexNotFoundException failure = expectThrows(IndexNotFoundException.class, future::actionGet);
        assertThat(failure.getMessage(), containsString("gated-idx"));
    }

    private static IndexMetadata index(String name) {
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
}
