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
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

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

    @After
    public void clearRegistrations() {
        DescriptorOnlyCreation.register(null);
        IndexDescriptorPublisher.register(null);
        IndexDescriptorPublisher.registerCreator(null);
        MappingGenerationStore.register(null);
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
        DescriptorOnlyCreation.register(indexMetadata -> true);

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
     * H7a. The wiring, exercised through the executor a real mapping update goes through.
     *
     * <p>An earlier version of this test called {@code MappingGenerationStore} directly and passed with
     * the wiring in {@code MetadataMappingService} disabled entirely, which is the "correct and
     * unreachable" failure this project has shipped twice. It is written against the executor now, so
     * removing the wiring fails it.
     */
    public void testTheExecutorRecordsAGatedIndexMapping() throws Exception {
        Map<String, Map<String, String>> stored = new HashMap<>();
        registerStore(stored);

        MetadataMappingService service = new MetadataMappingService(
            org.mockito.Mockito.mock(org.opensearch.cluster.service.ClusterService.class),
            org.mockito.Mockito.mock(org.opensearch.indices.IndicesService.class)
        );

        Index gated = new Index("gated-idx", "gated-uuid");
        PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            "{\"properties\":{\"age\":{\"type\":\"long\"}}}"
        );
        request.indices(new Index[] { gated });

        // A cluster state with no entry for the index, which is what gated means.
        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();

        ClusterStateTaskExecutor.ClusterTasksResult<PutMappingClusterStateUpdateRequest> result = service.new PutMappingExecutor().execute(
            empty,
            java.util.List.of(request)
        );

        assertSame("a gated mapping update must not change cluster state at all", empty, result.resultingState);
        assertEquals("the field must have reached the store", "long", stored.get("gated-uuid").get("age"));
    }

    /** A second field from another shard must join the first rather than replace it. */
    public void testASecondFieldJoinsTheFirst() {
        Map<String, Map<String, String>> stored = new HashMap<>();
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
     * success. So an object field added to a gated index was acknowledged and silently absent, while the
     * same field at creation was refused and kept the index in cluster state -- the "gated at creation and
     * refused on update, or the reverse" that the shared extractor exists to make impossible.
     *
     * <p>Asserted through the executor rather than against the extractor, for the reason the test beside it
     * records: an extractor test passes just as well with the wiring removed.
     */
    public void testTheExecutorRefusesAMappingTheStoreCannotCarry() throws Exception {
        Map<String, Map<String, String>> stored = new HashMap<>();
        registerStore(stored);

        MetadataMappingService service = new MetadataMappingService(
            org.mockito.Mockito.mock(org.opensearch.cluster.service.ClusterService.class),
            org.mockito.Mockito.mock(org.opensearch.indices.IndicesService.class)
        );

        Index gated = new Index("gated-idx", "gated-uuid");
        // An object field: the store holds a flat name-to-type map and has nowhere to put it.
        PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            "{\"properties\":{\"profile\":{\"properties\":{\"city\":{\"type\":\"keyword\"}}}}}"
        );
        request.indices(new Index[] { gated });

        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        ClusterStateTaskExecutor.ClusterTasksResult<PutMappingClusterStateUpdateRequest> result = service.new PutMappingExecutor().execute(
            empty,
            java.util.List.of(request)
        );

        assertNotNull(
            "a mapping the store cannot carry must fail the request, not be acknowledged with the field " + "missing",
            result.executionResults.get(request).getFailure()
        );
        assertTrue(
            "and nothing may be recorded, because a partial record is the loss this refuses: " + stored,
            stored.isEmpty() || stored.get("gated-uuid") == null
        );
    }

    /**
     * A field parameter must not pass the guard and then be dropped.
     *
     * <p>Separate from the object-field case because they fail differently. An object field has no
     * top-level type and was visibly unrepresentable; {@code ignore_above} sits beside a perfectly good
     * {@code keyword} and was invisible -- the guard asked whether a type was declared, which is true here,
     * rather than whether the definition was nothing but the type.
     */
    public void testTheExecutorRefusesAFieldParameterItWouldDrop() throws Exception {
        Map<String, Map<String, String>> stored = new HashMap<>();
        registerStore(stored);

        MetadataMappingService service = new MetadataMappingService(
            org.mockito.Mockito.mock(org.opensearch.cluster.service.ClusterService.class),
            org.mockito.Mockito.mock(org.opensearch.indices.IndicesService.class)
        );

        Index gated = new Index("gated-idx", "gated-uuid");
        PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            "{\"properties\":{\"code\":{\"type\":\"keyword\",\"ignore_above\":256}}}"
        );
        request.indices(new Index[] { gated });

        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        ClusterStateTaskExecutor.ClusterTasksResult<PutMappingClusterStateUpdateRequest> result = service.new PutMappingExecutor().execute(
            empty,
            java.util.List.of(request)
        );

        assertNotNull(
            "a field carrying a parameter the store would drop must fail rather than be reduced to its type",
            result.executionResults.get(request).getFailure()
        );
        assertTrue("and nothing may be recorded: " + stored, stored.isEmpty() || stored.get("gated-uuid") == null);
    }

    private static void registerStore(Map<String, Map<String, String>> stored) {
        MappingGenerationStore.register(new MappingGenerationStore.Store() {
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
