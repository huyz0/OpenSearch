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
import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * What a gated mapping keeps, and what it quietly does not.
 *
 * <h2>The hole this opens</h2>
 *
 * T11 stopped a create-time mapping being discarded by writing its fields to
 * {@code MappingGenerationStore}, and guarded the write with
 * {@code DescriptorRepresentable#fieldDefinitionsOrNull}, which returns null rather than a partial map when
 * something will not round-trip. Its own javadoc names the reason: "carrying the fields that do fit and
 * dropping the rest is the original silent loss one level in."
 *
 * <p>It then does exactly that one level further in. The extraction requires a declared {@code type} and no
 * sub-{@code properties}, and takes the type. It never checks the definition contains <em>nothing else</em>.
 * So every field parameter -- {@code analyzer}, {@code format}, {@code ignore_above}, {@code null_value},
 * {@code index}, {@code doc_values}, multi-{@code fields} -- passes the guard and is dropped, while the
 * creation is acknowledged and the index is gated.
 *
 * <p>The damage is not uniform, and the worst of it is not "a setting was lost". A
 * {@code date} declared with {@code format: dd/MM/yyyy} is stored as a bare {@code date}, so documents that
 * used to parse now fail or parse to a different day. A {@code keyword} with a {@code normalizer} silently
 * becomes case-sensitive, so a lookup that used to match stops matching. Both are wrong answers rather than
 * errors, from an index whose creation reported success.
 *
 * <h2>And the two extractors do not mirror each other</h2>
 *
 * {@code fieldDefinitionsOrNull} is documented as mirroring the extraction
 * {@code MetadataMappingService#recordGatedMapping} does for a put-mapping on a gated index, "deliberately:
 * two extractors that disagreed would mean a field gated at creation and refused on update, or the reverse."
 *
 * <p>They disagree. {@code recordGatedMapping} skips any property it cannot read and records the rest, then
 * reports success. An object field added by put-mapping to a gated index is therefore accepted and dropped,
 * where the same field at creation is refused and keeps the index resident. That is the reverse case the
 * comment says must not exist, and it is the partial map the other extractor exists to refuse.
 *
 * <p>These tests are written to fail against the code as committed. That is the point: the defect was
 * introduced by the change that was supposed to prevent it, and a test written after the fix would not
 * have shown that.
 */
public class GatedMappingFidelityIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

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
     * A date format must not be dropped by gating.
     *
     * <p>The sharpest case available, because losing it changes what documents mean rather than merely how
     * they are stored. {@code dd/MM/yyyy} is not a prefix or a variant of the default ISO format: a document
     * reading {@code 03/04/2026} parses as the third of April under the declared format and fails outright
     * under the default. So an index gated with the format dropped rejects the tenant's documents, or
     * accepts a different date, having acknowledged the mapping that would have handled them.
     */
    public void testADateFormatIsNotLostWhenTheIndexIsGated() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("serverless_fidelity-date").settings(gated())
                    .mapping(Map.of("properties", Map.of("when", Map.of("type", "date", "format", "dd/MM/yyyy"))))
            )
            .actionGet();

        assertMappingIsNotSilentlyReduced("serverless_fidelity-date", "when", "format");
    }

    /**
     * A length limit must not be dropped either.
     *
     * <p>Second case, because a single failing example invites a fix aimed at that example. {@code date} and
     * {@code keyword} take different parameters and lose them through the same missing check, so a fix that
     * satisfies one and not the other has addressed the symptom rather than the cause.
     *
     * <p>{@code ignore_above} rather than an analyzer, deliberately. An analyzer name has to resolve against
     * the index's analysis configuration, so a test using one can fail for that reason instead of this one
     * and would be measuring the wrong thing. {@code ignore_above} is a plain value on a built-in type,
     * which leaves the parameter's survival as the only thing the test can be reacting to.
     */
    public void testALengthLimitIsNotLostWhenTheIndexIsGated() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("serverless_fidelity-limit").settings(gated())
                    .mapping(Map.of("properties", Map.of("code", Map.of("type", "keyword", "ignore_above", 256))))
            )
            .actionGet();

        assertMappingIsNotSilentlyReduced("serverless_fidelity-limit", "code", "ignore_above");
    }

    /**
     * The control, and it is what stops the fix being "refuse everything".
     *
     * <p>A field declaring a type and nothing else round-trips exactly, so it must still be gated and its
     * type must still be in the store. Without this, tightening the guard until every mapping is refused
     * would satisfy both cases above while switching the feature back off -- which is the failure mode of
     * every check written as a refusal, and the one T11's own control was written to catch.
     */
    public void testAPlainTypedFieldIsStillCarriedAndStillGated() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("serverless_fidelity-plain").settings(gated())
                    .mapping(Map.of("properties", Map.of("tenant", Map.of("type", "keyword"))))
            )
            .actionGet();

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertNull("a mapping that round-trips completely must still be gated", state.metadata().index("serverless_fidelity-plain"));

        String uuid = descriptorUuid("serverless_fidelity-plain");
        MappingGenerationStore.MappingGeneration generation = MappingGenerationStore.currentMapping(uuid);
        assertNotNull("and its fields must be in the store", generation);
        assertEquals("keyword", MappingGenerationStore.typeOf(generation.fields().get("tenant")));
    }

    /**
     * Either the parameter survives, or the index was not gated. What must not happen is both.
     *
     * <p>Deliberately not asserting which. Carrying the parameter and refusing to gate are both defensible
     * answers and the choice is a design decision; an index that is gated <em>and</em> has lost the
     * parameter is the one outcome that is a defect under either. Writing the assertion this way means the
     * test states the property rather than the implementation, and survives a change of mind about which
     * of the two answers to give.
     */
    private void assertMappingIsNotSilentlyReduced(String index, String field, String parameter) throws Exception {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        if (state.metadata().index(index) != null) {
            // Refused, and the mapping is intact in cluster state where an ordinary index keeps it. This was
            // the answer between T13 and T15: honest, and it cost residency for most real mappings.
            assertNotNull("a refused index must keep the mapping it was created with", state.metadata().index(index).mapping());
            assertTrue(
                "and the parameter must be in it: " + state.metadata().index(index).mapping().source(),
                state.metadata().index(index).mapping().source().toString().contains(parameter)
            );
            return;
        }

        // Gated, so the store must hold the parameter as well as the type. Asserting the index is gated
        // proves only that it took the fast path; asserting the parameter is what says nothing was dropped
        // on the way, which is the whole question this class exists for.
        MappingGenerationStore.MappingGeneration generation = MappingGenerationStore.currentMapping(descriptorUuid(index));
        assertNotNull("a gated index's declared fields must be in the mapping store", generation);
        Map<String, Object> definition = generation.definitionOf(field);
        assertNotNull("field [" + field + "] must be in the store, not only its neighbours: " + generation.fields(), definition);
        assertTrue(
            "field ["
                + field
                + "] was stored as ["
                + definition
                + "], so ["
                + parameter
                + "] was accepted and then dropped. A tenant that declared a date format or a length limit "
                + "got neither, and was told its mapping was accepted",
            definition.containsKey(parameter)
        );
    }

    /**
     * A put-mapping the store cannot carry must fail, not succeed with the field missing.
     *
     * <p>The other half of the defect T13 fixed, and the worse half. The creation path refused what it
     * could not hold, so the index stayed resident and worked. The put-mapping path skipped whatever it
     * could not read, recorded the rest and reported success -- so the same declaration arriving by update
     * was acknowledged and silently reduced.
     *
     * <p>The case here is a shorthand definition ({@code "profile": "keyword"}) rather than the object field
     * it used to be, because T15 made object fields carry. What is left with no representation is a
     * property whose definition is not an object at all, and there is still no third option for it: the
     * index is not in cluster state, so the mapping cannot be applied the ordinary way, and the store
     * cannot hold it. Failing is the only answer that does not lose the field.
     */
    public void testAPutMappingTheStoreCannotCarryFailsRatherThanSucceeding() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin().indices().create(new CreateIndexRequest("serverless_fidelity-put").settings(gated())).actionGet();
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertNull(
            "the premise: the index is gated, so a put-mapping takes the store path",
            state.metadata().index("serverless_fidelity-put")
        );

        Exception failure = expectThrows(
            Exception.class,
            () -> client().admin()
                .indices()
                .preparePutMapping("serverless_fidelity-put")
                .setSource(Map.of("properties", Map.of("profile", "keyword")))
                .get()
        );

        assertTrue(
            "the failure must say the store cannot carry the field, rather than being an unrelated error: " + failure,
            failure.toString().contains("cannot carry")
                || failure.toString().contains("held outside cluster state")
                || failure.toString().contains("no representation")
        );

        // And nothing was recorded, because a partial record is the loss this refuses.
        MappingGenerationStore.MappingGeneration generation = MappingGenerationStore.currentMapping(
            descriptorUuid("serverless_fidelity-put")
        );
        assertTrue(
            "a refused put-mapping must leave the store untouched, not half-applied: "
                + (generation == null ? "nothing" : generation.fields()),
            generation == null || generation.fields().containsKey("profile") == false
        );
    }

    /**
     * The control: a put-mapping the store can carry still succeeds, end to end against a real store.
     *
     * <p>Without it, making the refusal throw on everything would satisfy the case above while breaking
     * every dynamic field inference on every gated index, which is the path this store was built for.
     *
     * <p>This case could not be written when the refusal landed, and what stopped it was not fidelity. It
     * failed with "Expected current thread [clusterManagerService#updateTask] to not be the cluster-manager
     * service thread. Reason: [Blocking operation]" -- the gated branch lived inside {@code
     * PutMappingExecutor}, which runs on the cluster manager's update thread, and the store's read and write
     * both block. T19 moved the work to {@code putMapping}, before any task is submitted, so this now
     * exercises the real path against the real backing index rather than an in-memory double.
     */
    public void testAPutMappingOfPlainFieldsStillSucceeds() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin().indices().create(new CreateIndexRequest("serverless_fidelity-put-ok").settings(gated())).actionGet();
        client().admin()
            .indices()
            .preparePutMapping("serverless_fidelity-put-ok")
            .setSource(Map.of("properties", Map.of("level", Map.of("type", "keyword"))))
            .get();

        MappingGenerationStore.MappingGeneration generation = MappingGenerationStore.currentMapping(
            descriptorUuid("serverless_fidelity-put-ok")
        );
        assertNotNull("a put-mapping that round-trips must reach the store", generation);
        assertEquals("keyword", MappingGenerationStore.typeOf(generation.fields().get("level")));
    }

    /** The descriptor's uuid for a gated name, which is the key the mapping store is written under. */
    private String descriptorUuid(String name) throws Exception {
        var descriptor = org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers.supply(name);
        assertNotNull("the gated index must resolve through the descriptor seam", descriptor);
        return descriptor.uuid();
    }

    private static Settings gated() {
        return Settings.builder()
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
    }
}
