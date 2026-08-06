/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexCreationValidator;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.plugins.Plugin;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A gated creation with nothing to validate must not build an index service to validate it.
 *
 * <h2>The cost this is about</h2>
 *
 * Every creation used to route through {@code IndicesService.withTempIndexService}, which builds a complete
 * {@code IndexService} -- settings, analysis, mappers, caches, engine factory, every plugin's
 * {@code onIndexModule} hook -- and throws it away. That is expensive, and it happens inside
 * {@code IndicesService.createIndexService}, which is {@code synchronized}, so concurrent creations queue on
 * one monitor on the cluster manager.
 *
 * <p>Profiling 215,000 gated creations at concurrency 16 measured both halves: 41.5% of on-CPU samples
 * inside that call tree, and 122,222 monitor-blocking events on {@code IndicesService} totalling 1,453
 * seconds of blocked thread time in a 120 second run, which is roughly 60% of all generic-thread time. For a
 * gated index the object is built to produce a mapping that {@code IndexDescriptor#from} then discards.
 *
 * <h2>Why the assertion is shaped this way</h2>
 *
 * "It got faster" is not a property a test can hold, and a timing assertion on a shared machine is a flake
 * generator. What can be asserted is <em>which path ran</em>, and {@link IndexCreationValidator} turns out
 * to state that precisely: the bypass passes a null {@code MapperService}, because a validator reaching it
 * has declared it does not read one, and the ordinary path passes a real one built from the index service.
 * So the argument the validator receives <b>is</b> the observation, with no new seam invented to take it.
 *
 * <p>That matters more than it might look. This branch's recurring defect is a mechanism that is correct and
 * unreachable -- built, tested through its own entry point, and called by nothing in production. A test that
 * only checked the fast path produces the right answer would pass just as happily if the fast path were
 * never entered.
 */
public class GatedCreationWithoutTemporaryIndexServiceIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    /**
     * What the last validation saw: null means the bypass ran, an instance means an index service was built.
     *
     * <p>Static because the plugin is constructed by the node, not by the test, so there is no instance to
     * reach through. Cleared in {@link #forgetWhatWasValidated} so one case cannot read another's result --
     * which would be the difference between asserting the bypass ran and asserting it ran at some point.
     */
    private static final java.util.Map<String, Boolean> BUILT_AN_INDEX_SERVICE = new java.util.concurrent.ConcurrentHashMap<>();

    private static final AtomicInteger VALIDATIONS = new AtomicInteger();

    /**
     * Whether an index service was built to validate {@code index}.
     *
     * <p>Keyed by index name rather than kept as "the last one", and that is a correction rather than a
     * tidy-up. A creation that carries a mapping causes a second creation behind it -- the mapping store's
     * own index, which is ordinary and does build one -- so a last-wins record reports that second creation
     * and attributes it to the first. The fast path looked broken for a day because of it.
     */
    private static boolean builtAnIndexServiceFor(String index) {
        Boolean built = BUILT_AN_INDEX_SERVICE.get(index);
        assertNotNull("no validation was recorded for [" + index + "], so this asserts nothing", built);
        return built;
    }

    /**
     * A validator that records rather than rejects, declaring it needs no mappings.
     *
     * <p>The declaration is not incidental. {@code MetadataCreateIndexService} checks every registered
     * validator, so one answering true keeps every creation on the ordinary path -- including this test's,
     * which would then observe a non-null mapper service and read as a bypass that does not work.
     */
    public static final class RecordingValidator implements IndexCreationValidator {
        @Override
        public boolean requiresMappings() {
            return false;
        }

        @Override
        public void validate(MapperService mapperService, IndexSettings indexSettings) {
            BUILT_AN_INDEX_SERVICE.put(indexSettings.getIndex().getName(), mapperService != null);
            VALIDATIONS.incrementAndGet();
        }
    }

    /** Carries {@link RecordingValidator} into the node, since validators are collected from plugins. */
    public static final class RecordingValidatorPlugin extends Plugin {
        @Override
        public Collection<IndexCreationValidator> getIndexCreationValidators() {
            return List.of(new RecordingValidator());
        }
    }

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
        return List.of(org.opensearch.serverless.storage.ServerlessStoragePlugin.class, RecordingValidatorPlugin.class);
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

    @Before
    public void forgetWhatWasValidated() throws Exception {
        BUILT_AN_INDEX_SERVICE.clear();
        VALIDATIONS.set(0);
    }

    @After
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    /**
     * Runs with the harness's own wildcard template in place, which is not incidental.
     *
     * <p>{@code OpenSearchIntegTestCase.randomIndexTemplate} installs {@code random_index_template} matching
     * {@code *} on every internal cluster test, carrying randomized settings and nothing else. The first
     * version of the bypass declined whenever any template applied, so this test failed -- correctly -- and
     * that failure is what showed the condition was about the wrong thing. A settings-only template needs no
     * validation, and being unable to cope with one would have left the fast path unreachable here and in
     * any real deployment that configures its tenants with a template.
     *
     * <p>So the template is deliberately left in place rather than suppressed. Suppressing it would have made
     * the test pass against the narrower predicate and hidden the limitation completely.
     */
    public void testAGatedCreationWithNothingToValidateBuildsNoIndexService() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin().indices().create(new CreateIndexRequest("gated-plain").settings(gated())).actionGet();

        assertEquals("the validator must still have run; skipping it is not what this change does", 1, VALIDATIONS.get());
        assertFalse(
            "a gated creation with no mappings, aliases, templates or sort must be validated without an "
                + "index service, and one built here means it was built anyway",
            builtAnIndexServiceFor("gated-plain")
        );
    }

    /**
     * The control, and the reason the assertion above means anything.
     *
     * <p>An ordinary index is not gated, so it must keep taking the path it always took. Without this, a
     * bypass that fired for everything would pass the test above while quietly removing mapping validation
     * from the whole cluster, which is the R1 violation this branch exists to avoid.
     */
    public void testAnOrdinaryCreationStillBuildsOne() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin().indices().create(new CreateIndexRequest("ordinary").settings(ordinary())).actionGet();

        assertTrue(
            "an index that was never gated must still be validated through a real index service",
            builtAnIndexServiceFor("ordinary")
        );
    }

    /**
     * A mapping of plainly typed fields takes the fast path, and its fields still reach the store.
     *
     * <p>This asserted the opposite until T21, and the reason it did was sound: the bypass is a statement
     * about requests with nothing to validate, and losing that distinction would mean a gated index created
     * with a mapping nobody ever parsed. What changed is not the principle but what "validate" costs. For a
     * field whose definition is nothing but {@code {"type": X}}, validation establishes exactly one thing --
     * that X is a field type this node can build -- and {@code IndicesService.hasFieldTypeParser} answers it
     * from an immutable registry with no lock. Nothing is skipped; the same question is asked cheaply.
     *
     * <p>T18 is why it was worth doing: mapped gated creations ran at 275 per second against 6,011, and
     * T11 through T15 had just made a mapped index the ordinary gated index rather than a refused one.
     *
     * <p><b>The second assertion is the one that matters more.</b> The fast path builds no document mapper,
     * so it passes {@code () -> null} to {@code buildIndexMetadata} and the declared mapping has to be put
     * on the metadata by hand. Miss that, and this creates a gated index whose mapping was parsed,
     * validated, and then present nowhere -- acknowledged, fields silently absent, which is the loss T11,
     * T13 and T15 were spent removing. Asserting only that the fast path ran would pass in exactly that
     * case.
     */
    public void testAMappingOfPlainlyTypedFieldsTakesTheFastPathAndStillReachesTheStore() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("gated-mapped").settings(gated())
                    .mapping(Map.of("properties", Map.of("tenant", Map.of("type", "keyword"))))
            )
            .actionGet();

        assertFalse(
            "a mapping of plainly typed fields can be validated against the type registry, so no index "
                + "service should have been built to validate it",
            builtAnIndexServiceFor("gated-mapped")
        );

        var descriptor = org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers.supply("gated-mapped");
        assertNotNull("the index must still be gated", descriptor);
        var generation = org.opensearch.cluster.metadata.MappingGenerationStore.currentMapping(descriptor.uuid());
        assertNotNull(
            "the declared fields must be in the mapping store. The fast path builds no document mapper, so "
                + "a mapping it fails to put on the metadata is accepted and then present nowhere",
            generation
        );
        assertEquals("keyword", org.opensearch.cluster.metadata.MappingGenerationStore.typeOf(generation.fields().get("tenant")));
    }

    /**
     * A field carrying a parameter still builds one, because there is more than a type name to check.
     *
     * <p>The boundary of what T21 admits, and the reason it is drawn here. {@code ignore_above} sits beside
     * a perfectly good {@code keyword}, so the registry check would pass and say nothing about the
     * parameter. An analyzer has to resolve against the index's analysis configuration and a date format has
     * to parse; neither is a registry lookup. Admitting these on the strength of the type name would be a
     * mapping accepted and not really checked.
     */
    public void testAFieldCarryingAParameterStillBuildsOne() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("gated-parameterised").settings(gated())
                    .mapping(Map.of("properties", Map.of("code", Map.of("type", "keyword", "ignore_above", 256))))
            )
            .actionGet();

        assertTrue(
            "a definition with more than a type in it has more to validate than the registry can answer, "
                + "so it must still be validated through a real index service",
            builtAnIndexServiceFor("gated-parameterised")
        );
    }

    /**
     * And a bad mapping on a gated index is still refused.
     *
     * <p>This is the failure the previous test guards against, stated as an outcome rather than as a path.
     * Both are here because they can break separately: the path check would still pass if the index service
     * were built and its result ignored.
     */
    public void testAGatedCreationWithAnUnparseableMappingIsStillRejected() throws Exception {
        installBlobBackedDescriptorPlane();

        expectThrows(
            Exception.class,
            () -> client().admin()
                .indices()
                .create(
                    new CreateIndexRequest("gated-bad-mapping").settings(gated())
                        .mapping(Map.of("properties", Map.of("tenant", Map.of("type", "no_such_field_type"))))
                )
                .actionGet()
        );
    }

    /**
     * A gated index with an alias keeps the index service too, because alias filters are validated against a
     * query shard context that only one can produce.
     */
    public void testAGatedCreationCarryingAnAliasStillBuildsOne() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("gated-aliased").settings(gated())
                    .alias(new org.opensearch.action.admin.indices.alias.Alias("everything"))
            )
            .actionGet();

        assertTrue(
            "an alias needs a query shard context, so the index service must still be built",
            builtAnIndexServiceFor("gated-aliased")
        );
    }

    private static Settings ordinary() throws Exception {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
    }

    private static Settings gated() throws Exception {
        return Settings.builder().put(ordinary()).put("index.serverless_storage.enabled", true).build();
    }
}
