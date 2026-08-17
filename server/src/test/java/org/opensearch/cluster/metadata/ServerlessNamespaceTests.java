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
 * The half of "a name belongs to exactly one plane" that lives in core: {@code clusterStateCreateIndex}
 * refuses a namespaced name a cluster state entry when the registered {@link IndexCreationStrategy} declines
 * it, rather than quietly making it an ordinary index.
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
 * <h2>Phase D3: no longer {@code DescriptorOnlyCreation}</h2>
 *
 * This class used to register directly on {@code DescriptorOnlyCreation}'s static registry. D3 of {@code
 * core-pluggability-refactor-plan.md} relocated that class into {@code plugins/serverless-storage} -- it was
 * never core's vocabulary to own, and {@code server/src/test} was the only reason it still had to compile
 * here. What this class actually tests -- that {@code MetadataCreateIndexService} correctly consults
 * whatever {@link IndexCreationStrategy} is registered -- does not need {@code DescriptorOnlyCreation}'s own
 * specific "serverless_" naming convention, only a stand-in with the same shape; see {@link
 * TestIndexCreationStrategy}. The pure unit test of that specific naming convention moved with the class
 * itself, into the plugin's own test suite.
 */
public class ServerlessNamespaceTests extends OpenSearchTestCase {

    private final TestIndexCreationStrategy strategy = new TestIndexCreationStrategy();

    @Before
    public void registerStrategy() {
        IndexCreationStrategyRegistry.register(strategy);
    }

    @After
    public void clearRegistrations() {
        strategy.deactivate();
        IndexDescriptorPublisher.register(null);
        IndexDescriptorPublisher.registerCreator(null);
        IndexCreationStrategyRegistry.register(null);
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
        strategy.activate(indexMetadata -> false);

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
            refused.getMessage().contains("serverless_declined") && refused.getMessage().contains("test namespace")
        );
    }

    /**
     * The control, and the half of this that R1 protects. A gate that declines everything is the ordinary
     * cluster's gate, and an ordinary name must be untouched by any of it.
     */
    public void testAnOrdinaryNameTheGateDeclinesStillGetsItsEntry() {
        strategy.activate(indexMetadata -> false);

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

        strategy.activate(indexMetadata -> true);
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
