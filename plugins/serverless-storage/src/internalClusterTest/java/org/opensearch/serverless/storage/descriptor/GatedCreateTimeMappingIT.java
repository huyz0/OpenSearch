/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A mapping given at creation must not be accepted and then thrown away.
 *
 * <h2>What was wrong</h2>
 *
 * A descriptor carries a {@code mappingGeneration} but not the mapping: mappings live in
 * {@code MappingGenerationStore}, and {@code IndexDescriptor.from} does not copy them.
 * {@code DescriptorRepresentable} refuses to gate an index whose declared state a descriptor cannot carry --
 * a hidden index, an index with any alias -- and had no such check for mappings.
 *
 * <p>So a gated index created with an explicit mapping had that mapping parsed, validated, built into
 * {@code IndexMetadata}, and then dropped on the floor when the descriptor was written. The creation was
 * acknowledged. Nothing failed. The fields simply were not there afterwards, and the first document to
 * arrive would infer them again from its own values -- which is the same wrong answer that looks like a
 * right one this area keeps producing, except that here it silently changes the field types the user asked
 * for.
 *
 * <h2>What it does now, in two steps</h2>
 *
 * <b>First it refused</b>, in the same place and for the same reason as the other rules: an index the
 * descriptor could not represent kept its cluster state entry. That stopped the loss and left a hole big
 * enough to make the feature useless, since a deployment giving every tenant an explicit mapping got no
 * gated indices at all -- which is the case this design exists for. A refusal is a way of not losing data,
 * not a way of supporting something.
 *
 * <p><b>Then it carried them.</b> The declared fields are written to {@code MappingGenerationStore} during
 * gated creation, which is where a gated index's mapping lives anyway once documents start arriving, and
 * the refusal narrowed to what genuinely cannot round-trip.
 *
 * <p>Two orderings carry the correctness. The mapping is written before the descriptor, because the
 * descriptor is the acknowledgement: if the mapping write fails the creation fails and no index exists,
 * where the reverse order would leave a name that resolves to an index whose declared fields are missing.
 * And the extraction returns nothing at all rather than a partial map when some property will not fit,
 * because carrying the fields that do fit and dropping the rest is the original silent loss one level in.
 *
 * <p>Two more rounds followed, and the shape of them is the point. T13 found the carrying was leakier than
 * it read: the guard asked whether a type was declared, not whether the definition was nothing but the type,
 * so a date format or a length limit passed it and was dropped. Tightening the guard fixed the loss and
 * refused most real mappings. T15 then widened the store to hold a field's whole definition, which is what
 * the limitation always was, and both refusals shrank to a property whose definition is not an object at all.
 */
public class GatedCreateTimeMappingIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private volatile java.nio.file.Path sharedBasePath;

    private java.nio.file.Path basePath() {
        if (sharedBasePath == null) {
            synchronized (this) {
                if (sharedBasePath == null) {
                    sharedBasePath = randomRepoPath();
                }
            }
        }
        return sharedBasePath;
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(org.opensearch.serverless.storage.ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(
                org.opensearch.serverless.storage.ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(),
                basePath().toString()
            )
            .build();
    }

    @After
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    /**
     * A gated creation writes its declared mapping without reading one first.
     *
     * <p>T41, and it lives here rather than only in the measurement class because a criterion that only
     * holds behind {@code -Dtests.mappingcost} is a criterion CI never checks. Reverting
     * {@code MetadataCreateIndexService} to {@code updateMapping} leaves every other test in this file
     * green: the stored fields are identical either way, and the only difference is one round trip that
     * nothing else looks at.
     *
     * <p>The count is asserted rather than a duration, for the usual reason. One creation is enough --
     * this is about which code path ran, and a population would only add flakiness and the cleanup problem
     * that put the measurement class behind a flag in the first place.
     */
    public void testAGatedCreationDoesNotReadAMappingThatCannotExist() throws Exception {
        var plane = installBlobBackedDescriptorPlane();
        // The descriptor store alone rather than the composition production registers: this counts round
        // trips, and the write-behind stats projection is neither one of them nor something a count taken on
        // the calling thread could see.
        ReadCountingStore counting = new ReadCountingStore(new DescriptorBackedMappingStore(plane::points, null));
        MappingGenerationStore.register(counting);

        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("serverless_gated-unread").settings(gated())
                    .mapping(Map.of("properties", Map.of("tenant", Map.of("type", "keyword"))))
            )
            .actionGet();

        assertEquals("a creation's mapping read can only answer absent, so it must not be issued", 0L, counting.reads());
        // This asserted one swap until T58. The write is not gone -- it moved into the descriptor the
        // creation was already writing, so the declared fields land in the same operation that makes the
        // index exist, and the round trip T41 removed from the creation path became no round trip at all.
        // The criterion still has teeth in the direction that matters: routing a creation's mapping back
        // through this store, by either verb, fails here.
        assertEquals("a creation's mapping rides its descriptor, so the store must not be touched", 0L, counting.swaps());
        // And they landed, so this cannot pass by the mapping having been dropped on the way. Read after the
        // counts, since this read is one.
        var generation = MappingGenerationStore.currentMapping(descriptorUuid("serverless_gated-unread"));
        assertNotNull("the declared fields must still be readable through the registered store", generation);
        assertEquals("keyword", MappingGenerationStore.typeOf(generation.fields().get("tenant")));
    }

    /**
     * A deleted gated index takes its mapping with it.
     *
     * <p>T47. Nothing ever removed a document from the mapping index, so it grew with every index that had
     * ever existed rather than with the live population. Asserted through the store rather than by counting
     * documents, because the store is what every reader goes through and a document that is unreachable
     * through it is not the thing being fixed.
     */
    public void testDeletingAGatedIndexRemovesItsStoredMapping() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("serverless_gated-deleted").settings(gated())
                    .mapping(Map.of("properties", Map.of("tenant", Map.of("type", "keyword"))))
            )
            .actionGet();
        String uuid = descriptorUuid("serverless_gated-deleted");
        assertNotNull("the mapping must be there before the delete, or this asserts nothing", MappingGenerationStore.currentMapping(uuid));

        client().admin().indices().prepareDelete("serverless_gated-deleted").get();

        assertBusy(
            () -> assertNull(
                "a deleted index's mapping must not outlive it in a shared index nothing else prunes",
                MappingGenerationStore.currentMapping(uuid)
            )
        );
    }

    /**
     * A prune that fails does not fail the deletion.
     *
     * <p>The index is gone by the time this runs, so reporting the delete as failed would invite a retry of
     * something that already happened. Without this the swallow in {@code removeStoredMappings} is untested:
     * every other double's delete succeeds, so removing the catch left the whole suite green.
     */
    public void testADeleteThatCannotPruneTheMappingStillSucceeds() throws Exception {
        installBlobBackedDescriptorPlane();
        MappingGenerationStore.register(new ThrowingDeleteStore(new IndexBackedMappingStore(client())));

        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("serverless_gated-unprunable").settings(gated())
                    .mapping(Map.of("properties", Map.of("tenant", Map.of("type", "keyword"))))
            )
            .actionGet();

        assertTrue(
            "a mapping that could not be removed must not turn its index's deletion into a failure",
            client().admin().indices().prepareDelete("serverless_gated-unprunable").get().isAcknowledged()
        );
    }

    /**
     * A request naming a gated index and an ordinary one prunes the gated one's mapping too.
     *
     * <p>That request takes a different path -- the cluster state task, since there is state to change --
     * and the prune was wired only into the all-gated one. Any wildcard over a mixed cluster is this
     * request, so the leak it left was the common case rather than an edge.
     */
    public void testAMixedDeleteAlsoPrunesTheGatedMapping() throws Exception {
        installBlobBackedDescriptorPlane();
        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("serverless_mixed-gated").settings(gated())
                    .mapping(Map.of("properties", Map.of("tenant", Map.of("type", "keyword"))))
            )
            .actionGet();
        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("mixed-ordinary").settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                )
            )
            .actionGet();
        String uuid = descriptorUuid("serverless_mixed-gated");

        client().admin().indices().prepareDelete("serverless_mixed-gated", "mixed-ordinary").get();

        assertBusy(
            () -> assertNull(
                "a mixed delete must prune the gated index's mapping, or every wildcard delete leaks one",
                MappingGenerationStore.currentMapping(uuid)
            )
        );
    }

    /** A store whose delete always fails, for the case where pruning cannot happen. */
    private static final class ThrowingDeleteStore implements MappingGenerationStore.Store {

        private final MappingGenerationStore.Store delegate;

        ThrowingDeleteStore(MappingGenerationStore.Store delegate) {
            this.delegate = delegate;
        }

        @Override
        public MappingGenerationStore.MappingGeneration read(String indexUuid) {
            return delegate.read(indexUuid);
        }

        @Override
        public boolean compareAndSwap(String indexUuid, long expectedGeneration, MappingGenerationStore.MappingGeneration updated) {
            return delegate.compareAndSwap(indexUuid, expectedGeneration, updated);
        }

        @Override
        public void delete(String indexUuid) {
            throw new IllegalStateException("the mapping index cannot be written");
        }
    }

    /** An index that never declared a mapping is deleted without the missing document failing anything. */
    public void testDeletingAGatedIndexWithNoMappingSucceeds() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin().indices().create(new CreateIndexRequest("serverless_gated-plain").settings(gated())).actionGet();

        assertTrue(
            "a mapping that was never written must not turn its index's deletion into a failure",
            client().admin().indices().prepareDelete("serverless_gated-plain").get().isAcknowledged()
        );
    }

    /** Counts what the creation path asks of the store, delegating everything else to the real one. */
    private static final class ReadCountingStore implements MappingGenerationStore.Store {

        private final MappingGenerationStore.Store delegate;
        private final java.util.concurrent.atomic.AtomicLong reads = new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.atomic.AtomicLong swaps = new java.util.concurrent.atomic.AtomicLong();

        ReadCountingStore(MappingGenerationStore.Store delegate) {
            this.delegate = delegate;
        }

        @Override
        public MappingGenerationStore.MappingGeneration read(String indexUuid) {
            reads.incrementAndGet();
            return delegate.read(indexUuid);
        }

        @Override
        public void delete(String indexUuid) {
            delegate.delete(indexUuid);
        }

        @Override
        public boolean compareAndSwap(String indexUuid, long expectedGeneration, MappingGenerationStore.MappingGeneration updated) {
            swaps.incrementAndGet();
            return delegate.compareAndSwap(indexUuid, expectedGeneration, updated);
        }

        long reads() {
            return reads.get();
        }

        long swaps() {
            return swaps.get();
        }
    }

    /**
     * A mapping of plain typed fields is carried into the mapping store, and the index is still gated.
     *
     * <p>The first version of this asserted the opposite: that such an index kept its cluster state entry,
     * because refusing was the only thing standing between an accepted mapping and a discarded one. That
     * stopped the loss and left a hole big enough to make the feature useless -- a deployment giving every
     * tenant an explicit mapping got no gated indices at all, which is the case this design exists for.
     *
     * <p>So the fields now go to {@code MappingGenerationStore} at creation, which is where a gated index's
     * mapping lives, and the refusal narrows to what genuinely cannot round-trip.
     */
    public void testAMappingOfPlainFieldsIsCarriedAndTheIndexStaysGated() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("serverless_gated-mapped").settings(gated())
                    .mapping(Map.of("properties", Map.of("tenant", Map.of("type", "keyword"), "amount", Map.of("type", "double"))))
            )
            .actionGet();

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertNull(
            "an index whose mapping can be carried field by field must still be gated, or carrying it " + "bought nothing",
            state.metadata().index("serverless_gated-mapped")
        );

        // The fields themselves, from the store that now holds them. Asserting the index is gated proves
        // only that it took the fast path; asserting the types proves the mapping was not lost on the way,
        // which is the whole question.
        String uuid = descriptorUuid("serverless_gated-mapped");
        var generation = org.opensearch.serverless.storage.descriptor.MappingGenerationStore.currentMapping(uuid);
        assertNotNull("the declared fields must be in the mapping store after a gated creation", generation);
        assertEquals(
            "keyword",
            org.opensearch.serverless.storage.descriptor.MappingGenerationStore.typeOf(generation.fields().get("tenant"))
        );
        assertEquals(
            "double",
            org.opensearch.serverless.storage.descriptor.MappingGenerationStore.typeOf(generation.fields().get("amount"))
        );
    }

    /**
     * An object field is carried too, which is what T15 changed.
     *
     * <p>This asserted the opposite until then, and the reason it did is worth keeping. The store held a
     * flat map of field name to type, so a property with its own properties had no representation in it and
     * refusing was the only answer that did not lose the field. T13 found the refusal was leakier than it
     * read -- a definition naming a type and carrying anything else passed the guard and was reduced to the
     * type -- and tightened it, which was honest and refused most real mappings.
     *
     * <p>T15 widened the store to hold a field's whole definition rather than its type, so the limitation
     * that forced both refusals is gone. An object field is a definition like any other now.
     */
    public void testAnObjectFieldIsCarried() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("serverless_gated-object").settings(gated())
                    .mapping(Map.of("properties", Map.of("nested", Map.of("properties", Map.of("inner", Map.of("type", "keyword"))))))
            )
            .actionGet();

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertNull(
            "an index with an object field must now be gated rather than kept resident",
            state.metadata().index("serverless_gated-object")
        );

        var generation = org.opensearch.serverless.storage.descriptor.MappingGenerationStore.currentMapping(
            descriptorUuid("serverless_gated-object")
        );
        assertNotNull("its declared fields must be in the mapping store", generation);
        Map<String, Object> nested = generation.definitionOf("nested");
        assertNotNull("the object field must be in the store: " + generation.fields(), nested);
        assertTrue(
            "and its sub-properties must have come with it, or carrying it bought nothing: " + nested,
            nested.containsKey("properties")
        );
    }

    /** The descriptor's uuid for a gated name, which is the key the mapping store is written under. */
    private String descriptorUuid(String name) throws Exception {
        var descriptor = org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers.supply(name);
        assertNotNull("the gated index must resolve through the descriptor seam", descriptor);
        return descriptor.uuid();
    }

    /**
     * The control: with no mapping there is nothing a descriptor cannot carry, so gating still happens.
     *
     * <p>Without this, a refusal that rejected every gated creation would satisfy the test above while
     * turning the feature off entirely, which is the failure mode of every check written as a refusal.
     */
    public void testAnIndexWithNoMappingIsStillGated() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin().indices().create(new CreateIndexRequest("serverless_gated-plain").settings(gated())).actionGet();

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertNull(
            "an index with nothing a descriptor cannot represent must still be gated, or the refusal above "
                + "has switched the whole feature off",
            state.metadata().index("serverless_gated-plain")
        );
    }

    private static Settings gated() throws Exception {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
    }
}
