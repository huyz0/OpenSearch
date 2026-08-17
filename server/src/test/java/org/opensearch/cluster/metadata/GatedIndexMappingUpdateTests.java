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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * H4c. What a mapping update does to an index that has no cluster state entry.
 *
 * <p>This started as an optimisation and the probe turned it into a prerequisite. H3 and H5 let an index
 * exist with no metadata entry, and every mapping update path resolves its target through
 * {@code Metadata#getIndexSafe}, which throws when the index is not in the map. So a gated index cannot
 * accept a document carrying a new field: dynamic mapping fails at the first unknown field rather than
 * degrading.
 *
 * <p>That inverts the priority. Moving mappings to a compare-and-swap on an object-store generation was
 * filed as a way to remove a global serialisation point from the write path, which is true and secondary.
 * The reason to do it is that without it H3 and H5 are not usable for any index whose mapping is not
 * fully known at creation, which is most of them.
 *
 * <p>H7a closed it. A gated index's mapping update now records its fields through
 * {@link MappingGenerationStore} and makes no cluster state change, so the resolution below still throws
 * and the write path no longer depends on it.
 */
public class GatedIndexMappingUpdateTests extends OpenSearchTestCase {

    private final TestIndexCreationStrategy strategy = new TestIndexCreationStrategy();

    private ThreadPool threadPool;

    @Before
    public void startThreadPool() {
        // A real pool rather than a mock, because the point of T19 is which thread the work runs on: the
        // gated path dispatches to GENERIC, and a mock would let that dispatch be removed silently.
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void stopThreadPool() {
        ThreadPool.terminate(threadPool, 10, java.util.concurrent.TimeUnit.SECONDS);
    }

    /**
     * Drives a put-mapping the store cannot carry, and asserts it failed with nothing recorded.
     *
     * <p>Shared by the two refusal cases so they differ only in the mapping, which is the one thing that
     * should differ: an object field and a field parameter fail the same guard for different reasons, and a
     * reader comparing them should see the mappings side by side rather than two copies of the wiring.
     */
    private void expectRefusal(String mappingSource, Map<String, Map<String, Object>> stored) {
        ClusterService clusterService = org.mockito.Mockito.mock(ClusterService.class);
        org.mockito.Mockito.when(clusterService.state()).thenReturn(ClusterState.builder(ClusterName.DEFAULT).build());
        MetadataMappingService service = new MetadataMappingService(
            clusterService,
            org.mockito.Mockito.mock(org.opensearch.indices.IndicesService.class),
            threadPool
        );

        PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(mappingSource);
        request.indices(new Index[] { new Index("gated-idx", "gated-uuid") });

        PlainActionFuture<ClusterStateUpdateResponse> future = PlainActionFuture.newFuture();
        service.putMapping(request, future);
        expectThrows(IllegalArgumentException.class, future::actionGet);

        assertTrue(
            "nothing may be recorded, because a partial record is the loss this refuses: " + stored,
            stored.isEmpty() || stored.get("gated-uuid") == null
        );
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
        MappingGenerationStore.register(null);
        IndexCreationStrategyRegistry.register(null);
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
        IndexDescriptorPublisher.registerCreator(descriptor -> java.util.concurrent.CompletableFuture.completedFuture(Boolean.TRUE));
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
     * H7a. The wiring, exercised through the entry point a real mapping update goes through.
     *
     * <p>An earlier version of this test called {@code MappingGenerationStore} directly and passed with the
     * wiring in {@code MetadataMappingService} disabled entirely, which is the "correct and unreachable"
     * failure this project has shipped twice. It went to the executor for that reason, and to
     * {@link MetadataMappingService#putMapping} for T19's: the executor runs on the cluster manager's update
     * thread, and this work is precisely what must not happen there.
     *
     * <p>So the assertion is not only that the field reached the store. It is that no cluster state task was
     * ever submitted -- which is what "off the cluster manager thread" means, stated as something a test can
     * observe rather than as a claim in a comment. The previous claim was in a comment, and it was wrong.
     */
    public void testAGatedMappingIsRecordedWithoutTouchingTheClusterManager() throws Exception {
        Map<String, Map<String, Object>> stored = new HashMap<>();
        registerStore(stored);

        ClusterService clusterService = org.mockito.Mockito.mock(ClusterService.class);
        // A cluster state with no entry for the index, which is what gated means.
        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        org.mockito.Mockito.when(clusterService.state()).thenReturn(empty);

        MetadataMappingService service = new MetadataMappingService(
            clusterService,
            org.mockito.Mockito.mock(org.opensearch.indices.IndicesService.class),
            threadPool
        );

        PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            "{\"properties\":{\"age\":{\"type\":\"long\"}}}"
        );
        request.indices(new Index[] { new Index("gated-idx", "gated-uuid") });

        PlainActionFuture<ClusterStateUpdateResponse> future = PlainActionFuture.newFuture();
        service.putMapping(request, future);
        assertTrue("a gated put-mapping must be acknowledged", future.actionGet().isAcknowledged());

        assertEquals("the field must have reached the store", "long", MappingGenerationStore.typeOf(stored.get("gated-uuid").get("age")));
        org.mockito.Mockito.verify(clusterService, org.mockito.Mockito.never())
            .submitStateUpdateTask(
                org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any()
            );
    }

    /** A second field from another shard must join the first rather than replace it. */
    public void testASecondFieldJoinsTheFirst() {
        Map<String, Map<String, Object>> stored = new HashMap<>();
        registerStore(stored);

        MappingGenerationStore.updateMapping("gated-uuid", Map.of("age", "long"));
        MappingGenerationStore.updateMapping("gated-uuid", Map.of("city", "keyword"));

        assertEquals("both fields must survive", 2, stored.get("gated-uuid").size());
    }

    /** With no store installed, nothing changes: the ordinary path is untouched. */
    public void testWithoutAStoreTheOrdinaryPathIsUnchanged() {
        assertFalse("no store means no gated handling at all", MappingGenerationStore.isRegistered());
    }

    /**
     * A put-mapping the store cannot carry must fail the request rather than record part of it.
     *
     * <p>The extraction here used to skip any property it could not read, record the rest and report
     * success. So a declaration added to a gated index was acknowledged and silently absent, while the same
     * declaration at creation was refused and kept the index in cluster state -- the "gated at creation and
     * refused on update, or the reverse" that the shared extractor exists to make impossible.
     *
     * <p>The case is a shorthand definition rather than the object field it was when T13 wrote it, because
     * T15 widened the store to hold whole definitions and object fields now carry. What remains
     * unrepresentable is a definition that is not an object at all.
     *
     * <p>Asserted through {@code putMapping} rather than against the extractor, for the reason the test
     * above records: an extractor test passes just as well with the wiring removed.
     */
    public void testAPutMappingTheStoreCannotCarryIsRefused() throws Exception {
        Map<String, Map<String, Object>> stored = new HashMap<>();
        registerStore(stored);

        expectRefusal("{\"properties\":{\"profile\":\"keyword\"}}", stored);
    }

    /**
     * A field parameter is carried rather than refused, which is what T15 changed.
     *
     * <p>This asserted a refusal when T13 wrote it, and the refusal was correct at the time: the store held
     * a flat name-to-type map, so {@code ignore_above} sitting beside a perfectly good {@code keyword} had
     * nowhere to go. The guard before that asked whether a type was declared -- true here -- rather than
     * whether the definition was nothing but the type, which is why the parameter was dropped invisibly.
     *
     * <p>Now the stored value is the definition, so the assertion is that the parameter arrived, not that
     * the request was rejected. Asserting only that the field is present would pass against the old
     * behaviour, which stored the type and dropped the rest.
     */
    public void testAFieldParameterIsCarriedIntoTheStore() throws Exception {
        Map<String, Map<String, Object>> stored = new HashMap<>();
        registerStore(stored);

        ClusterService clusterService = org.mockito.Mockito.mock(ClusterService.class);
        org.mockito.Mockito.when(clusterService.state()).thenReturn(ClusterState.builder(ClusterName.DEFAULT).build());
        MetadataMappingService service = new MetadataMappingService(
            clusterService,
            org.mockito.Mockito.mock(org.opensearch.indices.IndicesService.class),
            threadPool
        );

        PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            "{\"properties\":{\"code\":{\"type\":\"keyword\",\"ignore_above\":256}}}"
        );
        request.indices(new Index[] { new Index("gated-idx", "gated-uuid") });

        PlainActionFuture<ClusterStateUpdateResponse> future = PlainActionFuture.newFuture();
        service.putMapping(request, future);
        assertTrue("a definition the store can hold must be acknowledged", future.actionGet().isAcknowledged());

        Map<String, Object> definition = MappingGenerationStore.definition(stored.get("gated-uuid").get("code"));
        assertNotNull("the field must have reached the store", definition);
        assertEquals("keyword", definition.get("type"));
        assertEquals("and its parameter with it, or the store reduced it to a type again", 256, definition.get("ignore_above"));
    }

    /**
     * An object field is carried too, with its sub-properties.
     *
     * <p>The limitation that forced every refusal in this area was the store's flat name-to-type map, and a
     * property with its own properties was the case that could not be squeezed into it at all. Kept as a
     * separate case from the parameter above because they were unrepresentable for different reasons, and a
     * widening that handled one and not the other would leave the feature half-usable.
     */
    public void testAnObjectFieldIsCarriedIntoTheStore() throws Exception {
        Map<String, Map<String, Object>> stored = new HashMap<>();
        registerStore(stored);

        ClusterService clusterService = org.mockito.Mockito.mock(ClusterService.class);
        org.mockito.Mockito.when(clusterService.state()).thenReturn(ClusterState.builder(ClusterName.DEFAULT).build());
        MetadataMappingService service = new MetadataMappingService(
            clusterService,
            org.mockito.Mockito.mock(org.opensearch.indices.IndicesService.class),
            threadPool
        );

        PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            "{\"properties\":{\"profile\":{\"properties\":{\"city\":{\"type\":\"keyword\"}}}}}"
        );
        request.indices(new Index[] { new Index("gated-idx", "gated-uuid") });

        PlainActionFuture<ClusterStateUpdateResponse> future = PlainActionFuture.newFuture();
        service.putMapping(request, future);
        assertTrue("an object field must be acknowledged now that the store can hold it", future.actionGet().isAcknowledged());

        Map<String, Object> definition = MappingGenerationStore.definition(stored.get("gated-uuid").get("profile"));
        assertNotNull("the object field must have reached the store", definition);
        assertTrue("with its sub-properties, or carrying it bought nothing: " + definition, definition.containsKey("properties"));
    }

    private static void registerStore(Map<String, Map<String, Object>> stored) {
        MappingGenerationStore.register(new MappingGenerationStore.Store() {
            @Override
            public void delete(String indexUuid) {
                throw new AssertionError("nothing on this path deletes a mapping");
            }

            private final Map<String, MappingGenerationStore.MappingGeneration> byUuid = new HashMap<>();

            @Override
            public MappingGenerationStore.MappingGeneration read(String indexUuid) {
                return byUuid.get(indexUuid);
            }

            @Override
            public boolean compareAndSwap(String uuid, long expected, MappingGenerationStore.MappingGeneration updated) {
                MappingGenerationStore.MappingGeneration current = byUuid.get(uuid);
                if ((current == null ? 0L : current.generation()) != expected) {
                    return false;
                }
                byUuid.put(uuid, updated);
                stored.put(uuid, new HashMap<>(updated.fields()));
                return true;
            }
        });
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
