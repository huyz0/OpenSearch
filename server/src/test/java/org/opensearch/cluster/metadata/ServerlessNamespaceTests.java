/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.SystemIndices;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The two halves of "a name belongs to exactly one plane", asserted from core.
 *
 * <p>An index is gated because of its name. The plugin's gate refuses a name outside the {@code serverless_}
 * namespace, so no name out there can be held by a descriptor alone; {@code clusterStateCreateIndex} refuses a
 * name inside it a cluster state entry, so no name in here can be held by cluster state. This class owns the second half and the
 * routing decision that follows from the first.
 *
 * <h2>What this replaced</h2>
 *
 * {@code AdmissionTemplateResolutionTests} and {@code DescriptorOnlyCreationAdmissionTests}, which tested a
 * settings-based admission road that no longer exists. That road resolved templates on the request path to
 * guess whether a creation would turn out gated, could only reach <em>probably</em>, and needed a fallback
 * onto a cluster state update task for the times it guessed wrong -- which is both why creation could only be
 * distributed for the certainly-gated cases and how an ordinary index came to be able to claim a name a
 * descriptor already held. Their subject was retired rather than their assertions weakened; what they
 * protected against (a creation admitted down a road it could not finish) cannot happen when the name is the
 * decision, and the tests here are what says so.
 *
 * <h2>A real, pre-existing gap this class's own D2-final-slice migration found</h2>
 *
 * {@code MetadataCreateIndexService#certainlyGated} was migrated to {@code IndexCreationStrategyRegistry
 * .claims(...)} back in D2's second slice, but this test class was never updated to also register the
 * {@code SupplierBackedIndexCreationStrategy} bridge into that registry -- it only ever registered directly
 * on {@link DescriptorOnlyCreation}, the registry {@code certainlyGated} no longer reads. That silently
 * broke {@code testCertainlyGatedIsTheNameAndNothingElse} before this class's own final D2 slice (the
 * {@code clusterStateCreateIndex} migration this class also exercises) ever touched it -- confirmed by
 * running this suite against the pre-final-slice commit, which already failed the same way. Registering the
 * bridge here, unconditionally, in {@code @Before} (mirroring how {@code SupplierBackedIndexCreationStrategy}
 * is documented safe to register unconditionally in production, since it answers false until {@link
 * DescriptorOnlyCreation} itself has something registered) fixes both that pre-existing gap and this class's
 * own new coverage of {@code clusterStateCreateIndex}'s two sites in one change.
 */
public class ServerlessNamespaceTests extends OpenSearchTestCase {

    @Before
    public void registerCreationStrategyBridge() {
        // Mirrors production: ServerlessStoragePlugin#getIndexCreationStrategy() always returns this same
        // adapter, unconditionally, and it answers false until DescriptorOnlyCreation itself has a gate
        // registered -- so registering it here doesn't change what any test below asserts, only makes
        // IndexCreationStrategyRegistry (which MetadataCreateIndexService now actually consults) reachable
        // at all, the same way Node.java makes it reachable on a real cluster.
        IndexCreationStrategyRegistry.register(new SupplierBackedIndexCreationStrategy());
    }

    @After
    public void clearRegistrations() {
        DescriptorOnlyCreation.register(null);
        IndexDescriptorPublisher.register(null);
        IndexDescriptorPublisher.registerCreator(null);
        IndexCreationStrategyRegistry.register(null);
    }

    public void testTheNamespaceIsAPrefixOnTheNameAndNothingElse() {
        assertTrue(DescriptorOnlyCreation.namesAServerlessIndex("serverless_tenant-42"));
        assertTrue(
            "the prefix alone is in the namespace, however useless a name it is",
            DescriptorOnlyCreation.namesAServerlessIndex("serverless_")
        );
        assertFalse(DescriptorOnlyCreation.namesAServerlessIndex("tenant-42"));
        assertFalse("a prefix in the middle is not a prefix", DescriptorOnlyCreation.namesAServerlessIndex("my-serverless_index"));
        assertFalse("the hyphenated spelling is a different name", DescriptorOnlyCreation.namesAServerlessIndex("serverless-tenant"));
        assertFalse("total on the name means null is an answer, not a throw", DescriptorOnlyCreation.namesAServerlessIndex(null));
    }

    /**
     * The half this class owns: an index in the namespace that the gate declines must be refused, not
     * quietly given the cluster state entry it would once have fallen back to.
     *
     * <p>That fallback is exactly how the collision happened. A creation was admitted as gated, the gate
     * declined it -- for a filtered alias, say -- and it took the ordinary road, which validates its name
     * against cluster state and against nothing else, so it took a name a live descriptor already held.
     */
    public void testANamespacedIndexTheGateDeclinesIsRefusedRatherThanMadeOrdinary() {
        DescriptorOnlyCreation.register(indexMetadata -> false);

        IllegalArgumentException refused = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataCreateIndexService.clusterStateCreateIndex(
                ClusterState.builder(ClusterName.DEFAULT).build(),
                Set.of(),
                index("serverless_declined"),
                (current, reason) -> current,
                null,
                write -> {}
            )
        );
        assertTrue(
            "the caller has to be told which plane refused it, or this is the silent decline the area is about: " + refused.getMessage(),
            refused.getMessage().contains("serverless_declined") && refused.getMessage().contains("serverless namespace")
        );
    }

    /**
     * The control, and the half of this that R1 protects. A gate that declines everything is the ordinary
     * cluster's gate, and an ordinary name must be untouched by any of it.
     */
    public void testAnOrdinaryNameTheGateDeclinesStillGetsItsEntry() {
        DescriptorOnlyCreation.register(indexMetadata -> false);

        ClusterState state = MetadataCreateIndexService.clusterStateCreateIndex(
            ClusterState.builder(ClusterName.DEFAULT).build(),
            Set.of(),
            index("ordinary"),
            (current, reason) -> current,
            null,
            write -> {}
        );
        assertTrue("an index outside the namespace is created the way it always was", state.metadata().hasIndex("ordinary"));
    }

    /**
     * And with nothing registered, the namespace is just a string. A cluster with no serverless plugin has
     * no second plane for a name to belong to, so there is nothing to keep it out of cluster state.
     */
    public void testWithNoGateRegisteredANamespacedNameIsAnOrdinaryName() {
        ClusterState state = MetadataCreateIndexService.clusterStateCreateIndex(
            ClusterState.builder(ClusterName.DEFAULT).build(),
            Set.of(),
            index("serverless_no-plugin-here"),
            (current, reason) -> current,
            null,
            write -> {}
        );
        assertTrue(
            "refusing this would break a cluster that never asked for any of it",
            state.metadata().hasIndex("serverless_no-plugin-here")
        );
    }

    /**
     * Where the creation runs, which is the same decision read from the same name.
     *
     * <p>The alias-carrying template is the case worth keeping from the retired tests, and its expected
     * answer has inverted. It used to force the creation onto the cluster manager, because the gate would
     * decline the finished index and the road it declined onto could only run there. There is no such road
     * now: a namespaced index a template would make unrepresentable is refused, identically on every node,
     * so there is nothing to route away and no reason to resolve the template to find out.
     */
    public void testCertainlyGatedIsTheNameAndNothingElse() {
        final MetadataCreateIndexService service = serviceWithAliasCarryingTemplate("serverless_*");

        assertFalse(
            "with no gate registered nothing may be routed off the cluster manager",
            service.certainlyGated(request("serverless_tenant-1"), lastState)
        );

        DescriptorOnlyCreation.register(indexMetadata -> true);
        assertTrue(
            "a name in the namespace can only be gated or refused, and neither needs the cluster manager -- "
                + "not even with a template whose alias will refuse it",
            service.certainlyGated(request("serverless_tenant-1"), lastState)
        );
        assertFalse(
            "an ordinary index needs a cluster state update, and creating one anywhere else loses it",
            service.certainlyGated(request("tenant-1"), lastState)
        );
    }

    private ClusterState lastState;

    private static CreateIndexClusterStateUpdateRequest request(String name) {
        return new CreateIndexClusterStateUpdateRequest("cause", name, name).settings(Settings.EMPTY);
    }

    private MetadataCreateIndexService serviceWithAliasCarryingTemplate(String pattern) {
        final Metadata.Builder metadataBuilder = Metadata.builder()
            .put(
                IndexTemplateMetadata.builder("with-an-alias")
                    .patterns(singletonList(pattern))
                    .putAlias(AliasMetadata.builder("an-alias"))
                    .build()
            );
        lastState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metadata(metadataBuilder).build();
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(lastState);
        return new MetadataCreateIndexService(
            Settings.EMPTY,
            clusterService,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new SystemIndices(Collections.emptyMap()),
            true,
            null,
            null,
            null
        );
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
